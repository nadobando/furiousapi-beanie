from typing import Optional, Iterable

from furiousapi.api.pagination import CursorPaginationParams
from pydantic import BaseModel

from example.models import Item, Review
from furiousapi.beanie import MongoRepository


class Details1(BaseModel):
    data2: str


class Item1(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    details: Optional[Details1] = None


class ItemRepository(MongoRepository[Item]):
    async def get_with_projection(self) -> Iterable[Item]:
        return await self.query(self.__model__.find_many().project(Item1), CursorPaginationParams())


class ReviewRepository(MongoRepository[Review]):
    pass
