from __future__ import annotations

from typing import Optional, List

from beanie import Document, Link, PydanticObjectId
from pydantic import BaseModel


class Details(BaseModel):
    data1: str
    data2: str


class BaseReview(BaseModel):
    text: str


class Item(Document):
    name: str
    description: Optional[str] = None
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
    description: Optional[str] = None


class ItemCreate(BaseItem):
    pass


class ItemRead(BaseItem):
    id: PydanticObjectId
    # Note: Reviews are not embedded by default; you would populate them manually
    reviews: Optional[List[ReviewItemRead]] = None
