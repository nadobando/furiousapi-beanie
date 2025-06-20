from __future__ import annotations

import collections.abc
import copy
import sys
import typing
from functools import lru_cache
from inspect import getmembers, isclass
from typing import Any, Set, get_args, get_origin, Union
from typing import Optional
from typing import Sequence
from typing import TYPE_CHECKING, Dict
from typing import Tuple, Type, List

import furiousapi.pydantic
from beanie import Document
from beanie.odm.interfaces.detector import ModelType
from beanie.odm.operators.find.logical import And
from beanie.odm.utils.pydantic import get_config_value
from furiousapi.pydantic import PYDANTIC_V2, field_info_type
from furiousapi.pydantic import get_model_fields
from pydantic import BaseModel
from pydantic import create_model
from typing_extensions import Annotated

if TYPE_CHECKING:
    from beanie.odm.fields import ExpressionField
    from pydantic.fields import FieldInfo
    from beanie.odm.utils.projection import ProjectionModelType
    from pymongo import IndexModel
    from types import GenericAlias
    from beanie.odm.documents import DocType
    from beanie.odm.operators.find import BaseFindOperator

if PYDANTIC_V2:
    from pydantic import TypeAdapter


def _get_bulk_query_by_unique_index(
    model: Type[DocType], bulk: list[DocType], unique_index: IndexModel
) -> list[BaseFindOperator]:
    unique_keys = unique_index.document.get("key").keys()
    return [And(*[getattr(model, key) == getattr(item, key) for key in unique_keys for item in bulk])]


def gather_documents(*modules) -> Sequence[Type[DocType]]:
    """Returns a list of all MongoDB document models defined in `models` module."""

    result = []
    for module in modules:
        result += [
            doc
            for _, doc in getmembers(sys.modules[module], isclass)
            if issubclass(doc, Document) and doc.__name__ != "Document"
        ]
    return result


@lru_cache
def get_model_field_hint(model: Type[DocType], attribute: Union[ExpressionField, str]) -> Union[type, GenericAlias]:
    return typing.get_type_hints(model)[attribute]


def _unwrap_type(type_hint: Any) -> Set[Type[Any]]:
    origin = get_origin(type_hint)
    args = get_args(type_hint)

    if origin is Annotated:
        return _unwrap_type(args[0])

    if origin is Union:
        return {t for arg in args if arg is not type(None) for t in _unwrap_type(arg)}

    if origin in (list, set, tuple, collections.abc.Sequence, collections.abc.Iterable):
        return _unwrap_type(args[0]) if args else {type_hint}

    if origin is None:
        return {type_hint}

    if args:
        return {t for arg in args for t in _unwrap_type(arg)}

    return {type_hint}


@lru_cache
def get_concrete_types(model: Type[BaseModel], field_name: str) -> Set[Type[Any]]:
    field = get_model_fields(model).get(field_name)
    if not field:
        raise AttributeError(model, field_name)
    annotation = field.annotation
    return _unwrap_type(annotation)


def expand_wildcard_projection(model: Type[BaseModel], tree: List[List]) -> List[List]:
    def resolve_fields(m: Type[BaseModel]) -> List[str]:
        return list(get_model_fields(m).keys())

    def get_submodel(model: Type[BaseModel], field: str) -> Optional[Type]:
        info = get_model_fields(model).get(field)
        if not info:
            return None
        return info.annotation if PYDANTIC_V2 else info.type_

    expanded = []

    for branch in tree:
        head = branch[0]
        children = branch[1:]

        # Root-level wildcard: ['*']
        if head == "*" and not children:
            for field in resolve_fields(model):
                expanded.append([field])
            continue

        # If no children, just pass through
        if not children:
            expanded.append([head])
            continue

        # Expand children of `head`
        submodel = get_submodel(model, head)
        if not submodel or not isinstance(submodel, type) or not issubclass(submodel, BaseModel):
            continue

        child_keys = []
        has_wildcard = False
        for child in children:
            if child == "*":
                has_wildcard = True
            else:
                child_keys.append(child)

        new_subtree = []

        # Expand '*' into all fields of the submodel
        if has_wildcard:
            for field in resolve_fields(submodel):
                new_subtree.append([field])

        # Recurse into the other children
        for child in child_keys:
            if isinstance(child, list) and child:
                sub = expand_wildcard_projection(submodel, [child])
                new_subtree.extend(sub)

        expanded.append([head, *new_subtree])

    return expanded


@lru_cache
def alias_to_field(model: Type[BaseModel]) -> Dict[str, Union[str, dict]]:
    result: Dict[str, Union[str, dict]] = {}
    model_fields = get_model_fields(model)

    for name, field in model_fields.items():
        alias = field.alias
        if alias and alias != name:
            result[alias] = name

        cls = furiousapi.pydantic.field_info_type(field)

        if cls and issubclass(cls, BaseModel):
            nested = alias_to_field(cls)
            if nested:
                if alias and alias != name:
                    result[alias] = nested
                else:
                    result[name] = nested

    return result


@lru_cache
def get_field(
    path: str,
    model: Type[DocType],
    before_field: Optional[ExpressionField] = None,
    *,
    validate_fields: bool = True,
) -> Tuple[Type[DocType], Optional[ExpressionField], str]:
    if path == "*":
        return model, None, path
    if path.endswith(".*"):
        path = path[:-2]

    root, _, next_ = path.partition(".")
    root = alias_to_field(model).get(root, root)
    if not validate_fields:
        return model, getattr(getattr(model, root), next_), path
    field = None
    if issubclass(model, Document):
        try:
            field = getattr(model, root)
        except AttributeError as e:
            raise AttributeError(f"{model.__name__} has no {root} field") from e
    elif model.model_fields.get(root):
        if not next_:
            return model, getattr(before_field, path), path

        model = field_info_type(model.model_fields[root])

        if model:
            return get_field(next_, model, root)

        raise ValueError(f"{before_field}.{root} not a document")
    if not next_:
        if not field:
            raise AttributeError(f"{path} does not exists in {model.__name__}")
        if before_field is not None:
            return model, getattr(before_field, field), path
        return model, field, path
    link_fields = model.get_link_fields()
    if field in link_fields:
        model_ = link_fields[field].document_class
    else:
        field_info = model.model_fields[field]
        model_ = field_info_type(field_info)

    if before_field is None:
        before_field = field
    else:
        before_field = getattr(before_field, field)
    return get_field(next_, model_, before_field)


Projection = Dict[str, Union[int, "Projection"]]


def create_subset_model(model: Type[Union[BaseModel, Document]], projection: Projection) -> Type[BaseModel]:
    """
    Recursively create a subset of the given Pydantic model using the provided projection.

    :param model: The original Pydantic model class.
    :param projection: A dict defining which fields to include, with nested dicts for submodels.
    :return: A new Pydantic model class with only the selected fields.
    """
    fields: Dict[str, Tuple[Any, Any]] = {}
    model_fields = get_model_fields(model)
    aliases = alias_to_field(model)
    for field_name_, proj in projection.items():
        field_name = aliases.get(field_name_, field_name_)
        if field_name not in model_fields:
            continue  # Optional: ignore unknown fields

        field_info: FieldInfo = model_fields[field_name]
        field_type: Any = None

        if issubclass(model, Document) and model.get_link_fields().get(field_name):
            field_type = model.get_link_fields()[field_name].document_class
        elif PYDANTIC_V2:
            schema = TypeAdapter(field_info.annotation).core_schema
            field_type = schema.get("schema", {}).get("cls")
            if not field_type:
                concretes = get_concrete_types(model, field_name)
                field_type = next(iter(concretes), None)
        else:
            concretes = get_concrete_types(model, field_name)
            field_type = next(iter(concretes), None)

        # if field_type couldn't be determined, fallback to annotation
        if not field_type:
            field_type = field_info.annotation

        if isinstance(proj, dict) and issubclass(field_type, BaseModel):
            sub_model = create_subset_model(field_type, proj)

            fields[field_name] = (sub_model, copy.deepcopy(field_info))

        else:
            fields[field_name] = (field_type, copy.deepcopy(field_info))

    name = f"SubsetOf{model.__name__}"
    return create_model(name, __base__=BaseModel, **fields)


def get_projection(model: Type[ProjectionModelType]) -> Optional[Dict[str, Union[int, Dict[str, Any]]]]:
    if hasattr(model, "get_model_type") and (
        model.get_model_type() == ModelType.UnionDoc
        or (model.get_model_type() == ModelType.Document and getattr(model, "_inheritance_inited", False))
    ):
        return None

    if hasattr(model, "Settings"):
        settings = model.Settings
        if hasattr(settings, "projection"):
            return settings.projection

    if get_config_value(model, "extra") == "allow":
        return None

    document_projection: Dict[str, Any] = {}

    for name, field in get_model_fields(model).items():
        alias = field.alias or name
        outer_type = getattr(field, "annotation", None)

        # Detect nested model (BaseModel subclass)
        if isinstance(outer_type, type) and issubclass(outer_type, BaseModel):
            nested_proj = get_projection(outer_type)
            if nested_proj is not None:
                document_projection[alias] = nested_proj
                continue

        document_projection[alias] = 1

    return document_projection
