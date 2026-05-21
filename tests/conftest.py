import asyncio

import beanie
import beanie.odm.queries.find
import motor
import pytest
import pytest_asyncio
from fastapi import FastAPI
from furiousapi.api.exception_handling import furious_db_exception_handler, furious_api_exception_handler
from furiousapi.api.exceptions import FuriousAPIError
from furiousapi.db.exceptions import FuriousEntityError
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from testcontainers.mongodb import MongoDbContainer

import tests.listeners
from furiousapi.beanie.utils import get_projection
from tests.models import MyModel, Foreign, OneToMany

beanie.odm.queries.find.get_projection = get_projection
LISTENERS = [tests.listeners.CommandLogger(["find"])]


@pytest.fixture(scope="session")
def mongo_uri():
    with MongoDbContainer("mongo:7") as container:
        yield container.get_connection_url()


@pytest.fixture(autouse=True)
def anyio_backend():
    return "asyncio"


@pytest.fixture(scope="session", autouse=True)
def event_loop():
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
    yield loop
    loop.close()


async def drop_collections(db: AsyncIOMotorDatabase):
    col_names = await db.list_collection_names()
    for col in col_names:
        await db.drop_collection(col)


@pytest.fixture
def motor_client_(mongo_uri: str) -> AsyncIOMotorClient:
    return AsyncIOMotorClient(mongo_uri, event_listeners=LISTENERS)


@pytest.fixture
async def mocked_motor_client(mongo_uri: str) -> AsyncIOMotorDatabase:
    db_name = "test_db_function"
    db = AsyncIOMotorClient(mongo_uri, event_listeners=LISTENERS)[db_name]
    await drop_collections(db)
    db.get_io_loop = asyncio.get_event_loop
    yield db
    await drop_collections(db)


@pytest_asyncio.fixture(scope="session")
async def mocked_motor_client_session(mongo_uri: str) -> AsyncIOMotorDatabase:
    db_name = "test_db_session"
    client = AsyncIOMotorClient(mongo_uri, event_listeners=LISTENERS)
    db = client[db_name]
    await drop_collections(db)
    db.get_io_loop = asyncio.get_event_loop
    return db


@pytest_asyncio.fixture(scope="session")
async def _init_my_model(mocked_motor_client_session: motor.motor_asyncio.AsyncIOMotorDatabase) -> None:
    await beanie.init_beanie(mocked_motor_client_session, document_models=[MyModel, Foreign, OneToMany])


@pytest_asyncio.fixture(autouse=True)
async def init_my_model(mocked_motor_client: motor.motor_asyncio.AsyncIOMotorDatabase) -> None:
    await beanie.init_beanie(mocked_motor_client, document_models=[MyModel, Foreign, OneToMany])


@pytest.fixture
def app() -> FastAPI:
    app = FastAPI()
    app.add_exception_handler(FuriousEntityError, furious_db_exception_handler)
    app.add_exception_handler(FuriousAPIError, furious_api_exception_handler)

    return app
