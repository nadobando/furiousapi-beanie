import logging
import re
from typing import List, Union, Optional, Dict, Tuple
from typing import Type, Any

import flatten_dict
from beanie import Document
from furiousapi.pydantic import field_info_type
from beanie.odm.operators.find.comparison import BaseFindComparisonOperator
from beanie.odm.operators.find.logical import And, Or, Not
from beanie.odm.queries.find import FindMany
from beanie.operators import In
from furiousapi.core.fields import SortingDirection
from furiousapi.pydantic import PYDANTIC_V2
from furiousapi.pydantic import get_model_field, get_model_fields
from furiousapi.rql.transform import BaseRQLModelTransform
from lark import Token, Tree
from pydantic import BaseModel

from furiousapi.beanie import utils
from furiousapi.beanie.utils import get_field, create_subset_model

LOGGER = logging.getLogger(__name__)


class MongoRQLTransform(BaseRQLModelTransform):
    model: Type[Document]

    def __init__(self, *args, validate_fields: bool = True, fetch_links: bool = True, **kwargs):
        super().__init__(*args, **kwargs)
        self.fetch_links = fetch_links
        self.validate_fields = validate_fields
        self.__alias_mapping__ = utils.alias_to_field(self.model)
        self.__is_distinct__: bool = False

    def listing(self, expression: Tuple[str, str, Any]) -> Union[Dict, In]:
        op, field, values = super().listing(expression)
        _, field, _ = get_field(field, self.model, validate_fields=self.validate_fields)
        if op == "__contains__":
            return In(field, values)

        return {field: {f"${expression[0]}": values}}

    def not_(self, expression: List) -> Not:
        return Not(expression[0])

    def and_(self, ops: List) -> And:
        return And(*ops)

    def or_(self, ops: List) -> Or:
        return Or(*ops)

    def comp(self, c: List) -> BaseFindComparisonOperator:
        op, query_field, value = super().comp(c)
        model, field, path = get_field(query_field, self.model)
        op = getattr(field, op)
        aliased_field = utils.alias_to_field(model).get(field, field)
        aliased_path = utils.alias_to_field(model).get(path, path)
        if PYDANTIC_V2:

            model_ = model.__pydantic_validator__.validate_assignment(
                obj=model.model_construct(), field_name=aliased_path, field_value=value
            )
            value = model_.__getattribute__(aliased_path)
        else:
            field = get_model_field(model, aliased_field)
            value = field.validate(value, {}, loc=aliased_field)[0]

        return op(value)

    def searching(self, s: List) -> Dict[str, Any]:
        op, field, value = super().searching(s)

        if op == "like":
            return {field: re.compile(value)}
        if op == "ilike":
            return {field: re.compile(value, flags=re.RegexFlag.IGNORECASE)}
        raise NotImplementedError(op)

    @staticmethod
    def _process_select(select_tree: list[list], model: Type[BaseModel]) -> dict[str, Any]:  # noqa: C901,PLR0915
        projection = {}
        collapsed_parents = set()
        explicit_projections = set()

        def resolve_fields(m: Type[BaseModel]) -> List[str]:
            return list(get_model_fields(m).keys())

        def get_submodel(model: Type[Union[BaseModel, Document]], field: str) -> Optional[Type[BaseModel]]:
            if model is None:
                raise AssertionError(field)
            field_info = get_model_field(model, field)
            if not field_info:
                return None

            if issubclass(model, Document):
                link_field = model.get_link_fields().get(field)
                if link_field:
                    return link_field.document_class

            return field_info_type(field_info)

        def flatten(children: list) -> list:
            flat = []
            for c in children:
                if isinstance(c, list):
                    flat.extend(flatten(c))
                else:
                    flat.append(c)
            return flat

        def collect_explicit_paths(branch: list[str], base: str = "") -> None:
            if not branch:
                return
            head, *rest = branch
            path = f"{base}.{head}" if base else head
            if not rest:
                explicit_projections.add(path)
            else:
                collect_explicit_paths(rest, path)

        def walk(path: list[str], subtree: list, model: Type[BaseModel]) -> None:  # noqa: C901
            head = subtree[0]
            children = subtree[1:]
            new_path = [*path, head]

            if head == "*":
                for field in resolve_fields(model):
                    projection_key = ".".join([*path, field])
                    projection[projection_key] = 1
                return

            field_model = get_submodel(model, head)

            if not children:
                projection[".".join(new_path)] = 1
                return

            flat_children = flatten(children)
            if flat_children == ["*"]:
                collapsed_parents.add(".".join(new_path))
                return

            child_has_wildcard = any(c == "*" for c in flat_children)

            if child_has_wildcard and field_model:
                # Only include subfields that are NOT explicitly walked into
                for subfield in resolve_fields(field_model):
                    full_key = ".".join([*new_path, subfield])
                    nested_prefix = full_key + "."
                    if not any(other.startswith(nested_prefix) for other in explicit_projections):
                        projection[full_key] = 1
                # Also walk into explicitly requested deeper paths
                for child in children:
                    if child != "*" and isinstance(child, list):
                        walk(new_path, child, field_model)
                return

            for child in children:
                if isinstance(child, list):
                    collect_explicit_paths(child, ".".join(new_path))
                    walk(new_path, child, field_model)

        # Track all explicit projections first
        for branch in select_tree:
            collect_explicit_paths(branch)

        # Walk all branches
        for branch in select_tree:
            walk([], branch, model)

        # Prune parent fields if deeper children exist
        pruned = {}
        keys = sorted(projection.keys())
        for key in keys:
            if not any(other != key and other.startswith(f"{key}.") for other in keys):
                pruned[key] = projection[key]

        # Collapse full wildcards into parent key
        for prefix in collapsed_parents:
            pruned = {k: v for k, v in pruned.items() if not k.startswith(f"{prefix}.")}
            pruned[prefix] = 1

        top_level_fields = set(resolve_fields(model))
        projected_top_levels = {k for k in pruned if "." not in k and k in top_level_fields}
        if top_level_fields == projected_top_levels:
            return {}  # Signal: no projection needed
        return pruned

    def select(self, selected: List[Union[str, List[str]]]) -> List:
        selected = super().select(selected)
        selected_fields = self._process_select(selected, self.model)
        self.__selected_fields__ = selected_fields
        if self.validate_fields:
            for field in selected_fields:
                get_field(field, self.model)
        return selected

    def sign_prop(self, s: List[Union[Token, str]]) -> List[Union[str, SortingDirection]]:
        field, direction = super().sign_prop(s)
        _, expr_field, _ = get_field(field, self.model)
        return direction(expr_field)

    def start(self, _: Tree) -> FindMany:
        query = self.model.find_many(*self.__filter_fields__, fetch_links=self.fetch_links)

        if self.__selected_fields__:
            unflatten = flatten_dict.unflatten(self.__selected_fields__, "dot")
            returned_model = (self.__selected_fields__ and create_subset_model(self.model, unflatten)) or self.model

            select = {}
            for k, v in self.__selected_fields__.items():
                key = self.__alias_mapping__.get(k, k)
                if "." in key:
                    root, _, rest = key.partition(".")
                    attr = getattr(getattr(self.model, root), rest)
                else:
                    if k == "*":
                        continue
                    attr = getattr(self.model, key)
                select[attr] = v

            query = query.project(returned_model)

        if self.__sorting_fields__:
            query = query.sort(*self.__sorting_fields__)

        return query
