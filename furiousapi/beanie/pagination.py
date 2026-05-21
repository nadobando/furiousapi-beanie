from __future__ import annotations

import json
import logging
from datetime import datetime
from enum import Enum
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ClassVar,
    List,
    Optional,
    Set,
    Tuple,
    get_type_hints,
    cast,
    Iterable,
    Type,
    Dict,
    Union,
)

from bson import ObjectId
from furiousapi.api.error_responses import BadRequestHttpErrorResponse
from furiousapi.api.exceptions import FuriousAPIError
from furiousapi.api.pagination import PaginatedResponse, PaginationStrategyEnum
from furiousapi.db.pagination import (
    OffsetPagination,
    BaseRelayPagination,
    Cursor,
    BasePagination,
)
from furiousapi.pydantic import PYDANTIC_V2

from . import utils
from .utils import alias_to_field

if PYDANTIC_V2:
    import pydantic_core
else:
    pass

from typing import get_args, get_origin

from beanie import Document, PydanticObjectId, SortDirection
from beanie.operators import And, Or

if TYPE_CHECKING:
    from pydantic import BaseConfig
    from types import GenericAlias
    from furiousapi.core.types import TEntity, Sorting

    from beanie.odm.documents import DocType
    from beanie.odm.fields import ExpressionField
    from beanie.odm.operators.find import BaseFindOperator
    from beanie.odm.queries.find import FindMany

LOGGER = logging.getLogger(__name__)


class BeanieLimitPagination(BasePagination):
    def __init__(self, model: Type[Document]) -> None:
        self.model = model

    async def get_page(self, query: FindMany, limit: int, *args, **kwargs) -> Tuple[List, bool]:
        query = query.limit(limit + 1)
        items = await query.to_list()

        if limit is not None and len(items) > limit:
            has_next_page = True
            items = items[:limit]
        else:
            has_next_page = False

        return items, has_next_page


def object_id_to_json(x: Union[str, PydanticObjectId]) -> bytes:
    if isinstance(x, PydanticObjectId):
        return str(x).encode()
    return pydantic_core.to_json(x)


class BeanieOffsetPagination(OffsetPagination):
    async def get_page(self, query: FindMany, limit: int, next_: int = 0, **kwargs) -> PaginatedResponse:
        query = query.skip(next_)
        res = await super().get_page(query, limit, **kwargs)
        return PaginatedResponse(items=res[0], index=next_, next=next_ + limit)


class BeanieCursorPagination(BeanieLimitPagination, BaseRelayPagination):
    mapping: ClassVar[Dict[Union[type, GenericAlias], Callable[..., object]]] = {
        datetime: datetime.fromisoformat,
        int: int,
        float: float,
        PydanticObjectId: ObjectId,
        bool: bool,
    }

    def __init__(
        self,
        # sort_enum: SortableFieldEnum,
        id_fields: Set[str],
        # sorting: List[SortableFieldEnum],
        model: Type[Document],
    ) -> None:

        if PYDANTIC_V2:
            self.__json_dumps__: Callable = object_id_to_json
            self.__json_loads__: Callable = pydantic_core.from_json
        else:
            config: Type[BaseConfig] = cast("Type[BaseConfig]", model.Config)
            self.__json_dumps__: Callable = (hasattr(config, "json_dumps") and config.json_dumps) or json.dumps
            self.__json_loads__: Callable = (hasattr(config, "json_loads") and config.json_loads) or json.loads

        super().__init__(model)
        super(BeanieLimitPagination, self).__init__(id_fields)
        self.alias_mapping = alias_to_field(model)

    @classmethod
    def _handle_nullable(
        cls,
        column: ExpressionField,
        value: Any,
        direction: SortDirection = SortDirection.ASCENDING,
        *,
        is_nullable: bool,
    ) -> BaseFindOperator:
        clause = cls.set_operator(column, value, direction)
        if is_nullable:
            return Or(column.__eq__(None), clause)

        return clause

    def get_field_orderings(self, query: FindMany) -> List:
        query_sorting = query.sort_expressions

        if query_sorting:
            last_dir = query_sorting[-1][1]
            op = SortDirection.ASCENDING if last_dir == SortDirection.ASCENDING else SortDirection.DESCENDING
        else:
            op = SortDirection.ASCENDING

        model_alias_sorting = {self.alias_mapping.get(field, field) for field, _ in query_sorting}

        missing_id_fields = self.id_fields - model_alias_sorting
        missing_sort_fields = [(getattr(self.model, field), op) for field in missing_id_fields]

        return query_sorting + missing_sort_fields

    @staticmethod
    def deep_getattr(obj: object, attr_path: str) -> object:
        for attr in attr_path.split("."):
            if obj is None:
                return None
            obj = getattr(obj, attr, None)
        return obj

    def render_cursor(self, item: TEntity, column_fields: Iterable[Sorting]) -> str:
        if PYDANTIC_V2:
            result = []
            for field, _ in column_fields:
                field_ = self.alias_mapping.get(field, field)
                value = self.deep_getattr(item, field_)
                if isinstance(value, (PydanticObjectId, ObjectId)):
                    value = (b'"' + self.__json_dumps__(value) + b'"').decode()
                else:
                    value = self.__json_dumps__(value).decode()
                result.append(value)
            cursor = tuple(result)
        else:
            cursor = tuple(self.__json_dumps__(getattr(item, field[0]), default=str) for field in column_fields)

        return self.encode_cursor(cursor)

    def cast(self, column_type: Union[type, GenericAlias], value: Any) -> Any:
        if get_origin(column_type) is Union:
            for _type in get_args(column_type):
                if _type in self.mapping:
                    return None if value is None else self.mapping[_type](value)
        elif column_type in self.mapping:
            return None if value is None else self.mapping[column_type](value)

        return value

    @staticmethod
    def set_operator(column: ExpressionField, value: Any, direction: SortDirection) -> BaseFindOperator:
        if direction == SortDirection.ASCENDING:
            return column > value

        return column < value

    def get_filter(
        self, field_orderings: list[Sorting], cursor: Cursor, *, is_index_query: bool = False
    ) -> BaseFindOperator:
        column_cursors = []
        for (field, direction), cursor_value in zip(field_orderings, cursor):
            column_cursors.append((getattr(self.model, self.alias_mapping.get(field, field)), direction, cursor_value))
        cursors = []
        for i in range(len(column_cursors)):
            clause = self.get_filter_clause(column_cursors[: i + 1], is_index_query=is_index_query)
            cursors.append(clause)

        return Or(*cursors)

    def get_filter_clause(
        self,
        column_cursors: list[tuple[ExpressionField, Sorting, tuple[str, ...]]],
        *,
        is_index_query: bool = False,
    ) -> BaseFindOperator:
        previous_clauses = self.get_previous_clause(column_cursors[:-1])
        column, sorting_direction, value = column_cursors[-1]

        current_clause = self._prepare_current_clause(column, sorting_direction, value, is_index_query=is_index_query)

        if previous_clauses is None:
            return current_clause

        return And(previous_clauses, current_clause)

    def get_previous_clause(
        self, column_cursors: list[tuple[ExpressionField, SortDirection, tuple[str, ...]]]
    ) -> BaseFindOperator:
        if not column_cursors:
            return None
        clauses = []
        for column, direction, cursor in column_cursors:
            hint = utils.get_model_field_hint(self.model, cursor[0][0])
            value = self.cast(hint, cursor[1])

            if cursor[0] not in self.id_fields:
                clauses.append(column == value)
            else:
                clauses.append(self.set_operator(column, cursor, direction))

        return And(*clauses)

    def _prepare_current_clause(
        self,
        column: ExpressionField,
        direction: SortDirection,
        cursor: Tuple[str, ...],
        *,
        is_index_query: bool = False,
    ) -> BaseFindOperator:

        field = self.alias_mapping.get(cursor[0][0], cursor[0][0])
        hint = self.get_model_field_hint(field)
        value = self.cast(hint, cursor[1])
        is_nullable = any(
            [issubclass(i, type(None)) for i in get_args(hint) if str(field) not in self.id_fields]  # noqa: C419
        )

        if direction == SortDirection.ASCENDING:
            if value is None:
                if is_index_query:
                    return Or(column != value, column > value)
                return column > value

            current_clause = (
                self._handle_nullable(column, value, is_nullable=is_nullable) if value is not None else column > value
            )
        else:  # noqa: PLR5501
            if value is None:
                if is_index_query:
                    return Or(column.__ne__(None), column < value)
                current_clause = Or(column.__ne__(None), column < value)
            else:
                current_clause = column < value
        return current_clause

    def get_model_field_hint(self, column: Union[ExpressionField, str]) -> Union[type, GenericAlias]:
        return get_type_hints(self.model)[column]

    @staticmethod
    def inverted_sort(
        sorting: List[Sorting],
    ) -> List[Tuple[Union[str, ExpressionField], SortDirection]]:
        reversed_sort = []
        for field, direction in sorting:
            if direction == SortDirection.ASCENDING:
                reversed_sort.append((field, SortDirection.DESCENDING))
            else:
                reversed_sort.append(((field, SortDirection.ASCENDING)))
        return reversed_sort

    async def get_page_info(
        self,
        query: FindMany,
        field_orderings: list[Sorting],
        cursor: Optional[tuple[tuple[str, ...]]],
        items: list[DocType],
    ) -> dict:
        total = await query.count()
        index: Optional[int] = 0
        if cursor:
            inverted_sorting = self.inverted_sort(field_orderings)
            index_filter = self.get_filter(inverted_sorting, cursor, is_index_query=True)
            filter_clause = (query.find_expressions and And(index_filter, *query.find_expressions)) or index_filter
            index = (await self.model.find(filter_clause).sort(*inverted_sorting).count()) + 1

            if self.reversed:
                before_index = total - index
                index = max(before_index - len(items), 0)

        if not items:
            index = None

        return {"index": index, "total": total}

    async def get_page(
        self, query: FindMany, limit: int, next_: Optional[str] = None, *args, **kwargs
    ) -> PaginatedResponse:
        sort = self.get_field_orderings(query)

        cursor_in = self.parse_cursor(next_, sort)

        if cursor_in is not None:
            page_query = self.get_filter(sort, cursor_in)
            page_query = And(page_query, *query.find_expressions)
            page_query = self.model.find(page_query).sort(*sort)
        else:
            query = query.sort(*[x for x in sort if x not in query.sort_expressions])
            page_query = query

        items, has_next_page = await super().get_page(page_query, limit, next_=next_)
        next_ = None

        if self.reversed:
            items.reverse()

        if items:
            cursors_out = self.make_cursors(items, sort)

            next_ = (has_next_page and cursors_out[-1]) or None

        page_info = await self.get_page_info(query, sort, cursor_in, items)

        return PaginatedResponse[query.get_projection_model()](
            next=next_, items=items, total=page_info["total"], index=page_info["index"]
        )


PAGINATION_MAPPING = {
    PaginationStrategyEnum.OFFSET: BeanieOffsetPagination,
    PaginationStrategyEnum.CURSOR: BeanieCursorPagination,
}

AllPaginationStrategies = Union[Type[BeanieCursorPagination]]


def get_paginator(
    strategy: Union[PaginationStrategyEnum, str] = PaginationStrategyEnum.CURSOR,
) -> AllPaginationStrategies:
    if not isinstance(strategy, Enum):
        strategy = PaginationStrategyEnum(strategy)
    if not (paginator := PAGINATION_MAPPING.get(strategy)):
        raise FuriousAPIError(BadRequestHttpErrorResponse(detail=f"pagination strategy {strategy} not found"))
    return paginator
