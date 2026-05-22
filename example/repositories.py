from pydantic import BaseModel

from example.models import Item, Review
from furiousapi.beanie import MongoRepository


class Details1(BaseModel):
    data2: str


class Item1(BaseModel):
    name: str | None = None
    description: str | None = None
    details: Details1 | None = None


class ItemRepository(MongoRepository[Item]): ...


class ReviewRepository(MongoRepository[Review]):
    pass
