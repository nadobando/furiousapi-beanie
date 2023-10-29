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
    Dict,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    Union,
    get_args,
    get_origin,
    get_type_hints,
)

from bson import ObjectId
from furiousapi.core.api.error_details import BadRequestHttpErrorDetails
from furiousapi.core.api.exceptions import FuriousAPIError
from furiousapi.core.config import get_settings
from furiousapi.core.db.pagination import (
    BaseCursorPagination,
    BaseRelayPagination,
    Cursor,
    PaginatorMixin,
)
from furiousapi.core.fields import SortingDirection
from furiousapi.core.pagination import PaginatedResponse, PaginationStrategyEnum

from beanie import Document, PydanticObjectId
from beanie.operators import And, Or

from .sorting import _convert_sort

if TYPE_CHECKING:
    from types import GenericAlias

    from furiousapi.core.db.fields import SortableFieldEnum

    from beanie.odm.documents import DocType
    from beanie.odm.fields import ExpressionField
    from beanie.odm.operators.find import BaseFindOperator
    from beanie.odm.queries.find import FindMany

DEFAULT_PAGE_SIZE = get_settings().pagination.default_size
LOGGER = logging.getLogger(__name__)


class BeanieLimitPagination(PaginatorMixin):
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


class BeanieCursorPagination(BeanieLimitPagination, BaseCursorPagination):
    mapping: ClassVar[Dict[Union[type, GenericAlias], Callable[..., object]]] = {
        datetime: datetime.fromisoformat,
        int: int,
        float: float,
        PydanticObjectId: ObjectId,
        bool: bool,
    }

    def __init__(
        self, sort_enum: SortableFieldEnum, id_fields: Set[str], sorting: List[SortableFieldEnum], model: Type[Document]
    ) -> None:
        self.__json_dumps__ = hasattr(model.Config, "json_dumps") and model.Config.json_dumps or json.dumps
        self.__json_loads__ = hasattr(model.Config, "json_loads") and model.Config.json_loads or json.loads
        super().__init__(model)
        super(BeanieLimitPagination, self).__init__(sort_enum, id_fields, sorting)

    @classmethod
    def _handle_nullable(
        cls,
        column: ExpressionField,
        value: Any,
        direction: SortingDirection = SortingDirection.ASCENDING,
        *,
        is_nullable: bool,
    ) -> BaseFindOperator:
        clause = cls.set_operator(column, value, direction)
        if is_nullable:
            return Or(column.__eq__(None), clause)

        return clause

    def cast(self, column_type: Union[type, GenericAlias], value: Any) -> Any:
        if get_origin(column_type) is Union:
            for _type in get_args(column_type):
                if _type in self.mapping:
                    return None if value is None else self.mapping[_type](value)
        elif column_type in self.mapping:
            return None if value is None else self.mapping[column_type](value)

        return value

    @staticmethod
    def set_operator(column: ExpressionField, value: Any, direction: SortingDirection) -> BaseFindOperator:
        if direction == SortingDirection.ASCENDING:
            return column > value

        return column < value

    def get_filter(
        self, field_orderings: list[SortableFieldEnum], cursor: Cursor, *, is_index_query: bool = False
    ) -> BaseFindOperator:
        column_cursors = []
        for field, cursor_value in zip(field_orderings, cursor):
            column_cursors.append((getattr(self.model, field.name), field.direction, cursor_value))
        cursors = []
        for i in range(len(column_cursors)):
            clause = self.get_filter_clause(column_cursors[: i + 1], is_index_query=is_index_query)
            cursors.append(clause)

        return Or(*cursors)

    def get_filter_clause(
        self,
        column_cursors: list[tuple[ExpressionField, SortingDirection, tuple[str, ...]]],
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
        self, column_cursors: list[tuple[ExpressionField, SortingDirection, tuple[str, ...]]]
    ) -> BaseFindOperator:
        if not column_cursors:
            return None
        clauses = []
        for column, direction, cursor in column_cursors:
            hint = self.get_model_field_hint(cursor[0])
            value = self.cast(hint, cursor[1])

            if cursor[0] not in self.id_fields:
                clauses.append(column == value)
            else:
                clauses.append(self.set_operator(column, cursor, direction))

        return And(*clauses)

    def _prepare_current_clause(
        self,
        column: ExpressionField,
        direction: SortingDirection,
        cursor: Tuple[str, ...],
        *,
        is_index_query: bool = False,
    ) -> BaseFindOperator:
        hint = self.get_model_field_hint(cursor[0])
        value = self.cast(hint, cursor[1])
        is_nullable = any([issubclass(i, type(None)) for i in get_args(hint) if str(cursor[0]) not in self.id_fields])

        if direction == SortingDirection.ASCENDING:
            if value is None:
                if is_index_query:
                    return Or(column != value, column > value)
                return column > value

            current_clause = (
                self._handle_nullable(column, value, is_nullable=is_nullable) if value is not None else column > value
            )
        else: # noqa: PLR5501
            if value is None:
                if is_index_query:
                    return Or(column.__ne__(None), column < value)
                current_clause = Or(column.__ne__(None), column < value)
            else:
                current_clause = column < value
        return current_clause

    def get_model_field_hint(self, column: Union[ExpressionField, str]) -> Union[type, GenericAlias]:
        return get_type_hints(self.model)[column]

    async def get_page_info(
        self,
        query: FindMany,
        field_orderings: list[SortableFieldEnum],
        cursor: Optional[tuple[tuple[str, ...]]],
        items: list[DocType],
    ) -> dict:
        total = await query.count()
        index: Optional[int] = 0
        if cursor:
            inverted_sorting = [~field for field in field_orderings]
            index_filter = self.get_filter(inverted_sorting, cursor, is_index_query=True)
            filter_clause = query.find_expressions and And(index_filter, *query.find_expressions) or index_filter
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
        field_orderings = self.get_field_orderings()

        cursor_in = self.get_request_cursor(next_, field_orderings)
        sort = _convert_sort(self.model, tuple(field_orderings))

        if cursor_in is not None:
            page_query = self.get_filter(field_orderings, cursor_in)
            page_query = And(page_query, *query.find_expressions)
            page_query = self.model.find(page_query).sort(*sort).project(query.get_projection_model())
        else:
            query.sort(*sort)
            page_query = query

        items, has_next_page = await super().get_page(page_query, limit, next_=next_)
        next_ = None

        if self.reversed:
            items.reverse()

        if items:
            cursors_out = self.make_cursor(items[-1], field_orderings)
            next_ = has_next_page and cursors_out or None

        page_info = await self.get_page_info(query, field_orderings, cursor_in, items)

        return PaginatedResponse[query.get_projection_model()](
            next=next_, items=items, total=page_info["total"], index=page_info["index"]
        )


class BeanieRelayCursorPagination(BeanieCursorPagination, BaseRelayPagination):
    """A pagination scheme that works with the Relay specification.

    This pagination scheme assigns a cursor to each retrieved item. The page
    metadata will contain an array of cursors, one per item. The item metadata
    will include the cursor for the fetched item.

    For Relay Cursor Connections Specification, see
    https://facebook.github.io/relay/graphql/connections.htm.
    """

    async def get_page(
        self, query: FindMany, limit: int, next_: Optional[str] = None, *args, **kwargs
    ) -> PaginatedResponse:
        field_orderings = self.get_field_orderings()

        cursor_in = self.parse_cursor(next_, field_orderings)
        sort = _convert_sort(self.model, tuple(field_orderings))

        if cursor_in is not None:
            page_query = self.get_filter(field_orderings, cursor_in)
            page_query = And(page_query, *query.find_expressions)
            page_query = self.model.find(page_query).sort(*sort).project(query.get_projection_model())
        else:
            query.sort(*sort)
            page_query = query

        items, has_next_page = await super(BeanieCursorPagination, self).get_page(page_query, limit, next_=next_)
        next_ = None

        if self.reversed:
            items.reverse()

        if items:
            cursors_out = self.make_cursors(items, field_orderings)
            next_ = has_next_page and cursors_out[-1] or None

        page_info = await self.get_page_info(query, field_orderings, cursor_in, items)

        return PaginatedResponse[query.get_projection_model()](
            next=next_, items=items, total=page_info["total"], index=page_info["index"]
        )


PAGINATION_MAPPING = {
    # PaginationStrategyEnum.OFFSET: LimitOffsetPagination,
    PaginationStrategyEnum.CURSOR: BeanieRelayCursorPagination,
}

AllPaginationStrategies = Union[Type[BeanieCursorPagination]]


def get_paginator(
    strategy: Union[PaginationStrategyEnum, str] = PaginationStrategyEnum.CURSOR
) -> AllPaginationStrategies:
    if not isinstance(strategy, Enum):
        strategy = PaginationStrategyEnum[strategy]
    if not (paginator := PAGINATION_MAPPING.get(strategy)):
        raise FuriousAPIError(BadRequestHttpErrorDetails(detail=f"pagination strategy {strategy} not found"))
    return paginator
