from typing import Optional


from pydantic import BaseModel

from example.models import Item, Review
from furiousapi.beanie import MongoRepository


class Details1(BaseModel):
    data2: str


class Item1(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    details: Optional[Details1] = None


class ItemRepository(MongoRepository[Item]): ...


class ReviewRepository(MongoRepository[Review]):
    pass
