from __future__ import annotations


from beanie import Document, Link, PydanticObjectId
from furiousapi.pydantic import PYDANTIC_V2
from pydantic import BaseModel


class Details(BaseModel):
    data1: str
    data2: str


class BaseReview(BaseModel):
    text: str


class Item(Document):
    name: str
    description: str | None = None
    details: Details


class Review(Document):
    text: str
    item: Link[Item]


class ReviewItemRead(BaseReview):
    id: PydanticObjectId


class ReviewRead(ReviewItemRead):
    item: PydanticObjectId


class ReviewCreate(BaseReview):
    item: PydanticObjectId


class BaseItem(BaseModel):
    name: str
    description: str | None = None


class ItemCreate(BaseItem):
    pass


class ItemRead(BaseItem):
    id: PydanticObjectId
    # Note: Reviews are not embedded by default; you would populate them manually
    reviews: list[ReviewItemRead] | None = None


if PYDANTIC_V2:
    ItemRead.model_rebuild()
    Item.model_rebuild()
    ItemCreate.model_rebuild()
else:
    ItemRead.update_forward_refs()
    Item.update_forward_refs()
    ItemCreate.update_forward_refs()
