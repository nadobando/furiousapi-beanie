import asyncio

import pytest
from mongomock_motor import AsyncMongoMockClient


@pytest.fixture(scope="session", autouse=True)
def event_loop():
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture()
def mocked_motor_client():
    db = "test_db"
    return AsyncMongoMockClient()[db]


@pytest.fixture(scope="session")
def mocked_motor_client_session():
    db = "test_db"
    return AsyncMongoMockClient()[db]
