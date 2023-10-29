from __future__ import annotations

from typing import TYPE_CHECKING, Tuple, Type

from furiousapi.core.exceptions import InvalidEnumFieldError

from furiousapi.beanie.constants import SORTING_DIRECTION_MAPPING

if TYPE_CHECKING:
    from furiousapi.core.types import TSortableFields
    from pydantic import BaseModel

    from beanie.odm.enums import SortDirection


def _convert_sort(
    model: Type[BaseModel], field_orderings: Tuple[TSortableFields, ...], *, invert: bool = False
) -> tuple[tuple[str, SortDirection], ...]:
    result = []
    for field in field_orderings or []:
        if not getattr(model, field.name, None):
            raise InvalidEnumFieldError(field.name)

        field_ = invert and ~field or field
        result.append((str(getattr(model, field_.name)), SORTING_DIRECTION_MAPPING[field_.direction]))

    return tuple(result)
