from __future__ import annotations

import inspect
import logging
import typing
from types import GenericAlias
from typing import TYPE_CHECKING, Any, Dict, Iterator, Optional, Tuple, Type, Union

import pydantic
from fastapi import Depends, params
from pydantic import BaseModel, create_model
from pydantic.typing import NoneType

from beanie import Document, PydanticObjectId

if TYPE_CHECKING:
    from pydantic.fields import ModelField

from furiousapi.core.db.consts import ANNOTATIONS
from furiousapi.core.db.models import FuriousPydanticConfig
from furiousapi.core.db.utils import (
    _convert_pydantic,
    _remove_extra_data_from_signature,
    clean_dict,
    init_param,
)

logger = logging.getLogger(__name__)


class BeanieAllOptionalMeta(pydantic.main.ModelMetaclass):
    def __new__(mcs, name: str, bases: Tuple[type], namespaces: Dict[str, Any], **kwargs) -> Any:
        _convert_pydantic(name, namespaces, bases)
        new = super().__new__(mcs, name, bases, namespaces, **kwargs)
        _remove_extra_data_from_signature(new)
        return new

    @staticmethod
    def _convert_beanie(name: str, namespaces: dict, bases: tuple) -> None:
        annotations: dict[str, Any] = namespaces.get(ANNOTATIONS, {})
        annotations["id"] = Optional[PydanticObjectId]
        _convert_pydantic(name, namespaces, bases)
        for base in bases:
            for base_ in base.__mro__:
                if base_ is BaseModel or base_ is Document:
                    break

                annotations.update(base_.__annotations__)

        filter_ = [
            "metadata",
            "_previous_revision_id",
            "_saved_state",
            "_link_fields",
            "_cache",
            "_document_settings",
            "_hidden_fields",
        ]
        for field in annotations:
            if field.startswith("__") or field in filter_:
                continue
            annotations[field] = Optional[annotations[field]]

        namespaces[ANNOTATIONS] = annotations

    @classmethod
    def flatten_fields(  # noqa: C901, PLR0912
        mcs,
        model: Type[BaseModel],
        prefix: Optional[str] = None,
        alias_prefix_: Optional[str] = None,
        result: Optional[list] = None,
    ) -> list:
        result = result or []
        cls_params = dict(model.__signature__.parameters)
        cls_params.pop("args", None)
        cls_params.pop("kwargs", None)
        param_prefix = f"{prefix}__" if prefix else ""
        alias_prefix = f"{alias_prefix_}." if alias_prefix_ else ""
        for parameter, model_field in zip(cls_params.values(), model.__fields__.values()):
            if parameter.kind in (
                inspect.Parameter.VAR_KEYWORD,
                inspect.Parameter.VAR_POSITIONAL,
            ) or model_field.field_info.extra.get("hidden"):
                continue
            origin = typing.get_origin(model_field.annotation) or model_field.annotation

            new_param_name = f"{param_prefix}{model_field.name}"
            new_alias_name = f"{alias_prefix}{model_field.name}"
            if inspect.isclass(origin) and issubclass(origin, BaseModel):
                mcs.flatten_fields(origin, new_param_name, new_alias_name, result)

            elif origin is list:
                for arg in typing.get_args(origin):
                    if inspect.isclass(arg) and issubclass(arg, BaseModel):
                        mcs.flatten_fields(arg, new_param_name, new_alias_name, result)

                # TODO: implement list
            elif origin is typing.Union:
                added = False
                union_args = typing.get_args(model_field.annotation)
                for sub_origin in union_args:
                    if added:
                        break
                    try:
                        if any(map(lambda x: sub_origin is x, (dict, NoneType))):
                            # TODO: what to do here?
                            continue
                        if args := typing.get_args(sub_origin):
                            for arg in args:
                                if (
                                    inspect.isclass(arg)
                                    and type(sub_origin) != GenericAlias
                                    and issubclass(sub_origin, BaseModel)
                                ):
                                    added = True
                                    mcs.flatten_fields(arg, new_param_name, new_alias_name, result)
                                # else:
                                #     added=True
                                #     model_field.name = new_param_name
                                #     model_field.field_info.alias = new_alias_name
                                #     result.append(init_param(model_field, parameter))

                        elif (
                            inspect.isclass(sub_origin)
                            and not typing.get_args(sub_origin)
                            and issubclass(sub_origin, BaseModel)
                        ):
                            added = True
                            mcs.flatten_fields(sub_origin, new_param_name, new_alias_name, result)

                        elif not added:
                            added = True
                            # model_field.name = new_param_name
                            # model_field.field_info.alias = new_alias_name
                            result.append(init_param(model_field, new_param_name, new_alias_name, parameter))

                    except Exception:  # noqa: BLE001
                        logger.critical(
                            f"{model} cannot convert to filtering {parameter}, {sub_origin}",
                            extra={"parameter": parameter, "sub_origin": sub_origin},
                            exc_info=True,
                        )
                        # we need to check this manually

            elif not (inspect.isclass(origin) and issubclass(origin, BaseModel)):
                # model_field.name = new_param_name
                # model_field.field_info.alias = new_alias_name
                result.append(init_param(model_field, new_param_name, new_alias_name, parameter))
            else:
                logger.warning(f"could not set {origin}")
        return result

    @classmethod
    def handle_list(
        mcs, alias_name: str, param_name: str, origin: Union[Type, GenericAlias]
    ) -> Iterator[tuple[Type[BaseModel], str, str]]:
        for arg in typing.get_args(origin):
            if inspect.isclass(arg) and issubclass(arg, BaseModel):
                yield arg, param_name, alias_name

        # TODO: implement list

    @classmethod
    def handle_union(
        mcs,
        model: Type[BaseModel],
        model_field: ModelField,
        alias_name: str,
        param_name: str,
        parameter: inspect.Parameter,
    ) -> Tuple[list, list[Tuple[Type[BaseModel], str, str]]]:
        """
        Handles union types in a Pydantic model.

        :param model: The Pydantic model.
        :param model_field: The model field.
        :param alias_name: The new alias name.
        :param param_name: The new parameter name.
        :param parameter: The parameter.
        :return: A tuple of two lists. The first list contains the initialized parameters.
        The second list contains the remaining types that need to be handled.
        """
        union_args = typing.get_args(model_field.annotation)
        initialized_params = []
        remaining_types = []

        for sub_type in union_args:
            try:
                if sub_type in (dict, NoneType):
                    # TODO: what to do here?
                    continue

                sub_args = typing.get_args(sub_type)

                if sub_args:
                    for sub_arg in sub_args:
                        if issubclass(sub_arg, BaseModel) and not isinstance(sub_type, GenericAlias):
                            remaining_types.append((sub_arg, param_name, alias_name))
                            break
                elif issubclass(sub_type, BaseModel):
                    initialized_params.append(init_param(model_field, param_name, alias_name, parameter))
                    break

                initialized_params.append(init_param(model_field, param_name, alias_name, parameter))

            except Exception:  # noqa: BLE001
                logger.critical(
                    "cannot convert to filtering",
                    extra={
                        "model": model,
                        "parameter": parameter,
                        "sub_origin": sub_type,
                    },
                    exc_info=True,
                )

        return initialized_params, remaining_types


class FuriousMongoModel(Document):
    class Config(Document.Config, FuriousPydanticConfig):
        pass


def beanie_document_query(model: Type[BaseModel]) -> params.Depends:
    annotations = {k: (v.annotation, v.field_info) for k, v in model.__fields__.items()}
    cls = create_model(f"Optional{model.__name__}", **annotations)  # type: ignore[call-overload]

    def dependency(**kwargs) -> dict:
        from_dict = clean_dict(kwargs)
        for key, value in from_dict.copy().items():
            if "__" in key:
                from_dict[key.replace("__", ".")] = value
                from_dict.pop(key)

        return from_dict

    cls.__fields__["id"].required = False
    for field in cls.__fields__.values():
        field.required = False

    cls_params = dict(cls.__signature__.parameters)
    cls_params.pop("args", None)
    params = BeanieAllOptionalMeta.flatten_fields(model)

    dependency.__signature__ = inspect.Signature(  # type: ignore[attr-defined]
        parameters=params,
        return_annotation=cls,
        __validate_parameters__=True,
    )

    return Depends(dependency)
