import datetime
import json
import logging
from http import HTTPStatus
from typing import TYPE_CHECKING

import pytest
from bson import ObjectId
from fastapi import FastAPI
from furiousapi.pydantic import PYDANTIC_V2

import tests.text_queries
from furiousapi.beanie.query.model import RQLModelMongo, MongoTransformerConfig
from tests.models import MyModel, MyController

if TYPE_CHECKING:
    from beanie import Document
    from starlette.testclient import TestClient
from starlette.testclient import TestClient


@pytest.fixture
def test_client(app: FastAPI) -> TestClient:
    app.include_router(MyController.api_router, prefix="/model1")
    return TestClient(app)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "path",
    [
        "/model1",
    ],
)
async def test_create(
    test_client: "TestClient",
    path: str,
) -> None:
    model = MyModel(
        another_id=1,
        created_at=datetime.datetime(2023, 1, 1, 0, 1),
        int_number=1,
        float_number=1,
        is_boolean=True,
    )
    await create_model(model, path, test_client)
    response = test_client.get(path + f"/{model.id}")
    json = response.json()

    actual = MyModel.parse_obj(
        json,
    )
    assert actual == model


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "path",
    [
        "/model1",
    ],
)
async def test_get(test_client: "TestClient", path: str) -> None:
    model = MyModel(
        another_id=1, created_at=datetime.datetime(2023, 1, 1, 0, 1), int_number=1, float_number=1, is_boolean=True
    )
    await create_model(model, path, test_client)
    response = test_client.get(f"{path}/{model.id}")
    assert response.status_code == HTTPStatus.OK
    assert response.text == model.model_dump_json(by_alias=True)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "path",
    [
        "/model1",
    ],
)
async def test_list(test_client: "TestClient", path: str) -> None:
    model = MyModel(
        another_id=1, created_at=datetime.datetime(2023, 1, 1, 0, 1), int_number=1, float_number=1, is_boolean=True
    )
    await create_model(model, path, test_client)
    list_response = test_client.get(path)
    assert list_response.status_code == HTTPStatus.OK, list_response.json()
    assert model.parse_obj(list_response.json()["items"][0]) == model


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("path", "param"),
    [
        ("/model1", "another_id"),
    ],
)
async def test_update(test_client: "TestClient", path: str, param: str) -> None:
    model = MyModel(
        another_id=1,
        created_at=datetime.datetime(2023, 1, 1, 0, 1),
        int_number=1,
        float_number=1,
        is_boolean=True,
    )
    await create_model(model, path, test_client)
    assert model.id
    setattr(model, param, 100)
    if PYDANTIC_V2:
        d = model.model_dump(
            by_alias=True,
            mode="json",
            exclude_unset=True,
        )
        expected = model.model_dump(by_alias=True, mode="json")
    else:
        d = model.dict(by_alias=True, exclude_unset=True)
        expected = model.dict(by_alias=True)
    response = test_client.put(path + f"/{model.id}", json=d)
    assert response.status_code == HTTPStatus.OK
    assert response.json() == expected


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "path",
    [
        "/model1",
    ],
)
async def test_delete(test_client: "TestClient", path: str) -> None:
    model = MyModel(
        another_id=1,
        created_at=datetime.datetime(2023, 1, 1, 0, 1),
        int_number=1,
        float_number=1,
        is_boolean=False,
    )
    await create_model(model, path, test_client)

    response = test_client.delete(f"{path}/{model.id}")
    assert response.status_code == HTTPStatus.OK

    response = test_client.get(f"{path}/{model.id}")
    assert response.status_code == HTTPStatus.NOT_FOUND


async def create_model(model: "Document", path: str, test_client: "TestClient") -> None:
    if PYDANTIC_V2:
        create_response = test_client.post(path, data=model.model_dump_json(by_alias=True))
    else:
        create_response = test_client.post(path, data=model.dict(by_alias=True))

    assert create_response.status_code == HTTPStatus.OK
    json = create_response.json()
    model.id = ObjectId(json["_id"])


class MyModelRQL(RQLModelMongo):
    __model__ = MyModel
    __transformer_params__ = MongoTransformerConfig(fetch_links=False)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("rql", "expected"), [pytest.param(*x["params"], id=x["id"]) for x in tests.text_queries.ALL_TEST_CASES]
)
async def test_query(test_client: TestClient, caplog: pytest.LogCaptureFixture, rql: str, expected: str) -> None:
    caplog.set_level(logging.DEBUG, logger="furiousapi.beanie.pymongo")
    model1 = MyModel(another_id=1, int_number=1, float_number=2, is_boolean=True)
    await create_model(model1, "/model1", test_client)
    test_client.get(f"/model1?q={rql}").json()
    actual = get_listener_command(caplog)
    assert actual == expected


def get_listener_command(caplog: pytest.LogCaptureFixture, index: int = 0):
    command = json.loads(caplog.records[index].command)

    return {"filter": command.get("filter"), "projection": command.get("projection"), "sort": command.get("sort")}
