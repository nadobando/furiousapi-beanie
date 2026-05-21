import logging
import re
from typing import Any, Dict, List, Optional, Tuple, Type, Union, cast

from beanie import Document
from beanie import SortDirection
from beanie.odm.operators.find.comparison import BaseFindComparisonOperator
from beanie.odm.operators.find.logical import And, Or, Not
from beanie.odm.queries.find import FindMany
from beanie.operators import In
from furiousapi.pydantic import PYDANTIC_V2
from furiousapi.pydantic import field_info_type
from furiousapi.pydantic import get_model_field, get_model_fields
from furiousapi.rql.transform import BaseRQLModelTransform, SelectedField
from lark import Token, Tree
from pydantic import BaseModel

from furiousapi.beanie import utils
from furiousapi.beanie.utils import get_field, create_subset_model

LOGGER = logging.getLogger(__name__)
RQLMongoProjection = Dict[str, Union[None, "RQLMongoProjection"]]


class MongoRQLTransform(BaseRQLModelTransform):
    model: Type[Document]

    def __init__(self, *args, validate_fields: bool = True, fetch_links: bool = True, **kwargs):
        super().__init__(*args, **kwargs)
        self.fetch_links = fetch_links
        self.validate_fields = validate_fields
        # beanie.Document sets __hash__ = None, which trips mypy's Hashable check
        # for lru_cache args even though `type[Document]` is hashable at runtime.
        self.__alias_mapping__ = utils.alias_to_field(self.model)  # type: ignore[arg-type]
        self.__is_distinct__: bool = False

    def listing(self, expression: Tuple[str, str, Any]) -> Union[Dict, In]:
        op, field, values = super().listing(expression)
        _, field, _ = get_field(field, self.model, validate_fields=self.validate_fields)  # type: ignore[arg-type]
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
        model, field, path = get_field(query_field, self.model)  # type: ignore[arg-type]
        op = getattr(field, op)
        to_field_map = utils.alias_to_field(model)  # type: ignore[arg-type]
        # `field` is ExpressionField (subclass of str) at runtime; str() it so the
        # dict lookup against str keys type-checks cleanly.
        field_key = str(field) if field is not None else ""
        aliased_field = to_field_map.get(field_key, field_key)
        aliased_path = to_field_map.get(path, path)
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
    def _process_select(select_tree: RQLMongoProjection, model: Type[BaseModel]) -> RQLMongoProjection:  # noqa: C901

        def resolve_fields(m: Type[BaseModel]) -> List[str]:
            return list(get_model_fields(m).keys())

        def get_submodel(model: Type[Union[BaseModel, Document]], field: str) -> Optional[Type[BaseModel]]:
            if model is None:
                raise AssertionError(field)
            field_info = get_model_field(model, field)
            if not field_info:
                return None

            if issubclass(model, Document):
                link_fields = model.get_link_fields() or {}
                link_field = link_fields.get(field)
                if link_field:
                    return link_field.document_class

            return field_info_type(field_info)

        def walk(path: List[str], subtree: RQLMongoProjection, model: Type[BaseModel]) -> Dict:
            projection: Dict[str, Any] = {}
            for k, v in subtree.items():
                head = k
                children = v
                new_path = [*path, head]
                if children:
                    if children.keys() == {"*"}:
                        projection[head] = 1
                        continue
                    field_model = get_submodel(model, head)
                    if field_model:
                        projection[head] = walk(new_path, children, field_model)

                elif head == "*":
                    for field in resolve_fields(model):
                        projection[field] = 1
                else:
                    projection[head] = 1

            return projection

        return walk([], select_tree, model)

    def selection(self, s: List[Union[str, Dict]]) -> SelectedField:
        selected: SelectedField = super().selection(s)
        return self._process_select(cast("RQLMongoProjection", selected), self.model)

    def sign_prop(self, s: List[Union[Token, str]]) -> List[Union[str, SortDirection]]:
        field, direction = super().sign_prop(s)
        _, expr_field, _ = get_field(field, self.model)  # type: ignore[arg-type]
        return direction(expr_field)

    def start(self, _: Tree) -> FindMany:
        query = self.model.find_many(*self.__filter_fields__, fetch_links=self.fetch_links)

        if self.__selected_fields__:
            returned_model = (
                self.__selected_fields__ and create_subset_model(self.model, self.__selected_fields__)
            ) or self.model

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

            # create_subset_model returns BaseModel; beanie's project() narrows
            # its signature to Document. The dynamic subset model functions as a
            # projection class regardless of its declared base.
            query = query.project(cast("Type[Document]", returned_model))

        if self.__sorting_fields__:
            query = query.sort(*self.__sorting_fields__)

        return query
