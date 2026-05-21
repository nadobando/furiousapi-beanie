from typing import Annotated

from fastapi import Depends
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorClientSession

from example.config import MONGO_URL
from example.repositories import ItemRepository, ReviewRepository

client = AsyncIOMotorClient(MONGO_URL)


async def motor_session() -> AsyncIOMotorClientSession:
    async with await client.start_session() as s:
        yield s


SessionDep = Annotated[AsyncIOMotorClientSession, Depends(motor_session)]


def item_repository() -> ItemRepository:
    def dep(_: SessionDep) -> ItemRepository:
        return ItemRepository()  # todo: pass session over

    return Depends(dep)


def review_repository() -> ReviewRepository:
    def dep(_: SessionDep) -> ReviewRepository:  # todo: pass session over
        return ReviewRepository()

    return Depends(dep)
