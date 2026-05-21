from __future__ import annotations

import collections.abc
import copy
import sys
import typing
from functools import lru_cache
from inspect import getmembers, isclass
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
    Type,
    Union,
    _SpecialForm,
    cast,
    get_args,
    get_origin,
)

import furiousapi.pydantic
from beanie import Document
from beanie.odm.interfaces.detector import ModelType
from beanie.odm.operators.find.logical import And
from beanie.odm.utils.pydantic import get_config_value
from furiousapi.pydantic import PYDANTIC_V2, field_info_type
from furiousapi.pydantic import get_model_fields
from pydantic import BaseModel
from pydantic import create_model

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
    model: Type[Document], bulk: list[Document], unique_index: IndexModel
) -> list[BaseFindOperator]:
    key_doc = unique_index.document.get("key")
    if key_doc is None:
        return []
    unique_keys = key_doc.keys()
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


MAPPING_ARG_COUNT = 2


def _handle_annotated(args: Tuple, visit: Callable) -> Any:
    return _walk_type_hint(args[0], visit)


def _handle_union(args: Tuple, visit: Callable) -> Any:
    for arg in args:
        if arg is not type(None):
            result = _walk_type_hint(arg, visit)
            if result is not None:
                return result
    return None


def _handle_iterable(args: Tuple, visit: Callable) -> Any:
    if args:
        return _walk_type_hint(args[0], visit)
    return None


def _handle_mapping(args: Tuple, visit: Callable) -> Any:
    if len(args) == MAPPING_ARG_COUNT:
        return _walk_type_hint(args[1], visit)
    return None


def _handle_generic_args(args: Tuple, visit: Callable) -> Any:
    for arg in args:
        result = _walk_type_hint(arg, visit)
        if result is not None:
            return result
    return None


_HANDLER_MAP: Dict[Union[Type, _SpecialForm], Callable] = {
    Annotated: _handle_annotated,
    Union: _handle_union,
    list: _handle_iterable,
    set: _handle_iterable,
    tuple: _handle_iterable,
    collections.abc.Sequence: _handle_iterable,
    collections.abc.Iterable: _handle_iterable,
    dict: _handle_mapping,
    collections.abc.Mapping: _handle_mapping,
}


def _walk_type_hint(type_hint: Any, visit: Callable[[Any], Any]) -> Any:
    origin = get_origin(type_hint)
    args = get_args(type_hint)

    result = visit(type_hint)
    if result is not None:
        return result

    handler = _HANDLER_MAP.get(origin)
    if handler:
        result = handler(args, visit)
    elif args:
        result = _handle_generic_args(args, visit)
    else:
        result = None

    return result


# @lru_cache
# def contains_type(type_hint: Any, target_type: type) -> bool:
#     def match_visitor(t: Any):
#         if typing.get_origin(t) == target_type:
#             return True
#         return False
#
#     return bool(_walk_type_hint(type_hint, match_visitor))


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
        return info.annotation if PYDANTIC_V2 else getattr(info, "type_", None)

    expanded: List[List[Any]] = []

    for branch in tree:
        head = branch[0]
        children = branch[1:]

        # Root-level wildcard: ['*']
        if head == "*" and not children:
            # expanded.extend([[field] for field in resolve_fields(model)])
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

    root_part, _, next_ = path.partition(".")
    # See note on alias_to_field's lru_cache use; type[Document] is hashable.
    to_field = alias_to_field(model)  # type: ignore[arg-type]
    root_lookup = to_field.get(root_part, root_part)
    # to_field may map an alias to a nested dict; only top-level string aliases
    # resolve here, so fall back to the raw path segment for nested cases.
    root = root_lookup if isinstance(root_lookup, str) else root_part

    if not validate_fields:
        return model, getattr(getattr(model, root), next_), path

    field = _get_field_from_model(model, root)

    if not next_:
        if not field:
            raise AttributeError(f"{path} does not exists in {model.__name__}")
        if before_field is not None:
            return model, getattr(before_field, field), path
        return model, cast("ExpressionField", field), path

    expr_field: Optional[ExpressionField] = cast("Optional[ExpressionField]", field)
    model_, before_field = _get_next_model_and_expr(model, expr_field, before_field)
    return get_field(next_, model_, before_field)  # type: ignore[arg-type]


def _get_field_from_model(model: Type[DocType], root: str) -> Optional[Union[ExpressionField, str]]:
    if issubclass(model, Document):
        try:
            return getattr(model, root)
        except AttributeError as e:
            raise AttributeError(f"{model.__name__} has no {root} field") from e

    if root in model.model_fields:
        return root  # placeholder to be handled later
    return None


def _get_next_model_and_expr(
    model: Type[DocType],
    field: Optional[ExpressionField],
    before_field: Optional[ExpressionField],
) -> Tuple[Type[DocType], ExpressionField]:
    if field is None:
        raise ValueError("Cannot resolve next model from a None field")
    field_key = str(field)
    link_fields = model.get_link_fields() or {}
    model_: Any
    if field_key in link_fields:
        model_ = link_fields[field_key].document_class
    else:
        model_ = field_info_type(model.model_fields[field_key])

    expr = field if before_field is None else getattr(before_field, field_key)
    return cast("Type[DocType]", model_), cast("ExpressionField", expr)


Projection = Dict[str, Union[int, "Projection"]]

SUBSET_PREFIX = "__SubSetOf"


def _resolve_subset_field_type(model: Type[Union[Document, BaseModel]], field_info: FieldInfo, field_name: str) -> Any:
    """Resolve the concrete python type for a projection-subset field.

    Checks beanie link fields first, then unwraps the annotation via pydantic v2's
    TypeAdapter (or get_concrete_types in v1), and finally falls back to the raw
    field annotation.
    """
    if issubclass(model, Document):
        link_fields = model.get_link_fields() or {}
        if link_fields.get(field_name):
            return link_fields[field_name].document_class
    if PYDANTIC_V2:
        schema = TypeAdapter(field_info.annotation).core_schema
        field_type = schema.get("schema", {}).get("cls")
        if field_type:
            return field_type
    concretes = get_concrete_types(model, field_name)  # type: ignore[arg-type]
    return next(iter(concretes), None) or field_info.annotation


def create_subset_model(model: Type[Union[Document, BaseModel]], projection: Projection) -> Type[BaseModel]:
    """
    Recursively create a subset of the given Pydantic model using the provided projection.

    :param model: The original Pydantic model class.
    :param projection: A dict defining which fields to include, with nested dicts for submodels.
    :return: A new Pydantic model class with only the selected fields.
    """
    fields: Dict[str, Tuple[Any, Any]] = {}
    model_fields = get_model_fields(model)
    aliases = alias_to_field(model)  # type: ignore[arg-type]
    for field_name_, proj in projection.items():
        alias_lookup = aliases.get(field_name_, field_name_)
        # aliases may map to nested dicts; only top-level string aliases resolve here.
        field_name = alias_lookup if isinstance(alias_lookup, str) else field_name_
        if field_name not in model_fields:
            continue

        field_info: FieldInfo = cast("FieldInfo", model_fields[field_name])
        field_type: Any = _resolve_subset_field_type(model, field_info, field_name)

        field_info_copy = copy.deepcopy(field_info)

        if isinstance(proj, dict) and issubclass(field_type, BaseModel):
            if issubclass(field_type, Document):
                sub_model = create_subset_model(field_type, proj)
                annotation = sub_model
            elif issubclass(field_type, BaseModel):
                annotation = create_subset_model(field_type, proj)
            else:
                raise TypeError

            fields[field_name] = (annotation, field_info_copy)

        else:
            fields[field_name] = (field_info.annotation, field_info_copy)

    name = f"{SUBSET_PREFIX}{model.__name__}"
    # create_model's overloads don't accept arbitrary kwargs dict at the type
    # level; the (annotation, field_info) tuple-form is supported at runtime.
    return create_model(name, __base__=BaseModel, **cast("Dict[str, Any]", fields))


def get_projection(model: Type[ProjectionModelType]) -> Optional[Dict[str, Union[int, Dict[str, Any]]]]:
    get_model_type = getattr(model, "get_model_type", None)
    if get_model_type is not None and (
        get_model_type() == ModelType.UnionDoc
        or (get_model_type() == ModelType.Document and getattr(model, "_inheritance_inited", False))
    ):
        return None

    settings = getattr(model, "Settings", None)
    if settings is not None and hasattr(settings, "projection"):
        return settings.projection

    if get_config_value(model, "extra") == "allow":
        return None

    document_projection: Dict[str, Any] = {}

    for name, field in get_model_fields(model).items():
        alias = field.alias or name
        outer_type = next(iter(_unwrap_type(field.annotation)), None)

        if isinstance(outer_type, type) and issubclass(outer_type, BaseModel):
            if not outer_type.__name__.startswith(SUBSET_PREFIX):
                document_projection[alias] = 1
                continue
            nested_proj = get_projection(outer_type)
            if nested_proj is not None:
                document_projection[alias] = nested_proj
                continue

        document_projection[alias] = 1

    return document_projection
