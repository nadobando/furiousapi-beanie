from __future__ import annotations

from typing import List

from furiousapi.api import ModelController, action

from example.dependencies import item_repository, review_repository
from example.models import ItemRead, ReviewCreate, Item
from furiousapi.beanie.query.model import RQLModelMongo


class ItemRQL(RQLModelMongo):
    __model__ = Item


class ItemController(ModelController, prefix="/item", tags=["Items"]):  # type: ignore[call-arg]
    repository = item_repository()
    get_model = ItemRead
    __filtering__ = ItemRQL

    @action("/proj")
    async def get_with_projection(self) -> List[Item]:
        return await self.repository.get_with_projection()


class ReviewController(ModelController, prefix="/review", tags=["Reviews"]):  # type: ignore[call-arg]
    repository = review_repository()
    create_model = ReviewCreate
