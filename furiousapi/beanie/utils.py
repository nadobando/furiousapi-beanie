from __future__ import annotations

from typing import TYPE_CHECKING, Type

from beanie.odm.operators.find.logical import And

if TYPE_CHECKING:
    from pymongo import IndexModel

    from beanie.odm.documents import DocType
    from beanie.odm.operators.find import BaseFindOperator


def _get_bulk_query_by_unique_index(
    model: Type[DocType], bulk: list[DocType], unique_index: IndexModel
) -> list[BaseFindOperator]:
    unique_keys = unique_index.document.get("key").keys()
    return [And(*[getattr(model, key) == getattr(item, key) for key in unique_keys for item in bulk])]
