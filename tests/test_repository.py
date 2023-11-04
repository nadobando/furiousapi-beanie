from __future__ import annotations

import datetime
from typing import TYPE_CHECKING, Any, List, Optional, Tuple

import pytest
import pytest_asyncio
from bson import ObjectId

from furiousapi.db import EntityAlreadyExistsError, EntityNotFoundError
from furiousapi.api.pagination import CursorPaginationParams
from furiousapi.api.responses import BulkItemStatusEnum

import beanie
from beanie import Document, PydanticObjectId
from furiousapi.beanie.models import FuriousMongoModel
from furiousapi.beanie.repository import BaseMongoRepository
from tests.utils import get_first_doc_from_cache

if TYPE_CHECKING:
    import motor.core
    from _pytest.fixtures import FixtureRequest
    from furiousapi.db.fields import SortableFieldEnum

PAGINATION = 5
CACHE_KEY = "mongo_docs"


class MyModel(Document):
    created_at: datetime.datetime
    another_id: int
    int_number: int
    float_number: int
    is_boolean: bool
    nullable: Optional[int]

    class Settings:
        name = "my_model"
        use_revision = False

    class Config(FuriousMongoModel.Config):
        pass


class MyRepository(BaseMongoRepository[MyModel]): ...


@pytest_asyncio.fixture(scope="session")
async def _init_my_model(mocked_motor_client_session: motor.AgnosticClientSession) -> None:
    await beanie.init_beanie(mocked_motor_client_session, document_models=[MyModel])


@pytest_asyncio.fixture(scope="session", autouse=True)
async def init_data(_init_my_model: None, request: FixtureRequest):
    repository = MyRepository()
    result = []
    for i in range(PAGINATION):
        model = MyModel(
            another_id=i + 1,
            created_at=datetime.datetime(2023, 1, 1, 0, i + 1),
            int_number=i + 1,
            float_number=(i % 4) + 1,
            is_boolean=True,
        )
        doc = await repository.add(model)
        result.append(doc.json())
    nullable_alternator = 0
    for i in range(PAGINATION):
        model = MyModel(
            another_id=i + PAGINATION + 1,
            created_at=datetime.datetime(2023, 1, 1, 0, i + 1),
            int_number=i + 1,
            float_number=(i % 4) + 1,
            is_boolean=False,
            nullable=nullable_alternator % 2 and 1 or None,
        )
        nullable_alternator += 1
        doc = await repository.add(model)
        result.append(doc.json())

    request.config.cache.set(CACHE_KEY, result)
    yield
    await repository.__model__.get_motor_collection().drop()


@pytest.mark.parametrize("limit", [2, 5, 10], ids=["limit 2", "limit 5", "limit 10"])
@pytest.mark.parametrize(
    ("sorting", "filtering", "expected"),
    [
        pytest.param([], None, list(range(1, (PAGINATION * 2) + 1)), id="sort by id asc"),
        pytest.param([("id", "__neg__")], None, list(range((PAGINATION * 2), 0, -1)), id="sort by id desc"),
        pytest.param(
            [("another_id", "__pos__")],
            None,
            list(range(1, (PAGINATION * 2) + 1)),
            id="sort by another_id asc",
        ),
        pytest.param(
            [("another_id", "__neg__")],
            None,
            list(range((PAGINATION * 2), 0, -1)),
            id="sort by another_id desc",
        ),
        pytest.param([("int_number", "__pos__")], None, [1, 6, 2, 7, 3, 8, 4, 9, 5, 10], id="sort by int_number asc"),
        pytest.param([("int_number", "__neg__")], None, [10, 5, 9, 4, 8, 3, 7, 2, 6, 1], id="sort by int_number desc"),
        pytest.param(
            [("float_number", "__pos__")],
            None,
            [1, 5, 6, 10, 2, 7, 3, 8, 4, 9],
            id="sort by float_number asc",
        ),
        pytest.param(
            [("float_number", "__neg__")],
            None,
            [9, 4, 8, 3, 7, 2, 10, 6, 5, 1],
            id="sort by float_number desc",
        ),
        pytest.param([("created_at", "__pos__")], None, [1, 6, 2, 7, 3, 8, 4, 9, 5, 10], id="sort by created_at asc"),
        pytest.param([("created_at", "__neg__")], None, [10, 5, 9, 4, 8, 3, 7, 2, 6, 1], id="sort by created_at desc"),
        pytest.param([("is_boolean", "__pos__")], None, [6, 7, 8, 9, 10, 1, 2, 3, 4, 5], id="sort by is_boolean asc"),
        pytest.param([("is_boolean", "__neg__")], None, [5, 4, 3, 2, 1, 10, 9, 8, 7, 6], id="sort by is_boolean desc"),
        pytest.param(
            [
                ("float_number", "__pos__"),
                ("int_number", "__pos__"),
            ],
            None,
            [1, 6, 5, 10, 2, 7, 3, 8, 4, 9],
            id="sort by (float_number:asc,int_number:asc)",
        ),
        pytest.param(
            [
                ("float_number", "__pos__"),
                ("int_number", "__neg__"),
            ],
            None,
            [10, 5, 6, 1, 7, 2, 8, 3, 9, 4],
            id="sort by (float_number:asc,int_number:desc)",
        ),
        pytest.param(
            [
                ("float_number", "__neg__"),
                ("int_number", "__pos__"),
            ],
            None,
            [4, 9, 3, 8, 2, 7, 1, 6, 5, 10],
            id="sort by (float_number:desc,int_number:asc)",
        ),
        pytest.param(
            [
                ("float_number", "__neg__"),
                ("int_number", "__neg__"),
            ],
            None,
            [9, 4, 8, 3, 7, 2, 10, 5, 6, 1],
            id="sort by (float_number:desc,int_number:desc)",
        ),
        pytest.param(
            [
                ("float_number", "__pos__"),
                ("int_number", "__pos__"),
                ("is_boolean", "__pos__"),
            ],
            None,
            [6, 1, 10, 5, 7, 2, 8, 3, 9, 4],
            id="sort by (float_number:asc, int_number:asc, is_boolean:asc)",
        ),
        pytest.param(
            [
                ("float_number", "__pos__"),
                ("int_number", "__neg__"),
                ("is_boolean", "__pos__"),
            ],
            None,
            [10, 5, 6, 1, 7, 2, 8, 3, 9, 4],
            id="sort by (float_number:asc, int_number:desc, is_boolean:asc)",
        ),
        pytest.param(
            [
                ("float_number", "__pos__"),
                ("int_number", "__pos__"),
                ("is_boolean", "__neg__"),
            ],
            None,
            [1, 6, 5, 10, 2, 7, 3, 8, 4, 9],
            id="sort by (float_number:asc, int_number:asc, is_boolean:desc)",
        ),
        pytest.param(
            [
                ("float_number", "__neg__"),
                ("int_number", "__neg__"),
                ("is_boolean", "__pos__"),
            ],
            None,
            [9, 4, 8, 3, 7, 2, 10, 5, 6, 1],
            id="sort by (float_number:desc, int_number:desc, is_boolean:asc)",
        ),
        pytest.param(
            [
                ("float_number", "__neg__"),
                ("int_number", "__pos__"),
                ("is_boolean", "__neg__"),
            ],
            None,
            [4, 9, 3, 8, 2, 7, 1, 6, 5, 10],
            id="sort by (float_number:desc, int_number:asc, is_boolean:desc)",
        ),
        pytest.param(
            [
                ("float_number", "__pos__"),
                ("int_number", "__neg__"),
                ("is_boolean", "__neg__"),
            ],
            None,
            [5, 10, 1, 6, 2, 7, 3, 8, 4, 9],
            id="sort by (float_number:asc, int_number:desc, is_boolean:desc)",
        ),
        pytest.param(
            [
                ("float_number", "__neg__"),
                ("int_number", "__neg__"),
                ("is_boolean", "__neg__"),
            ],
            None,
            [4, 9, 3, 8, 2, 7, 5, 10, 1, 6],
            id="sort by (float_number:desc, int_number:desc, is_boolean:desc)",
        ),
        pytest.param([], {"is_boolean": False}, [6, 7, 8, 9, 10], id="sort by id asc and filter is_boolean False"),
        pytest.param(
            [("id", "__neg__")],
            {"is_boolean": False},
            list(reversed([6, 7, 8, 9, 10])),
            id="sort by id desc and filter is_boolean False",
        ),
        pytest.param(
            [],
            {"is_boolean": False, "float_number": 1},
            [6, 10],
            id="sort by id asc and filter is_boolean=False and float_number=1",
        ),
        pytest.param(
            [("id", "__neg__")],
            {"is_boolean": False, "float_number": 1},
            list(reversed([6, 10])),
            id="sort by id desc and filter is_boolean=False and float_number=1",
        ),
        # TODO:
        #   pytest.param([
        #     ("nullable", "__neg__")],
        #     None,
        #     id="sort by nullable",)
    ],
)
@pytest.mark.asyncio()
async def test_list_with_sorting_and_filter(
    limit: int,
    sorting: List[Tuple[str, str]],
    filtering: dict[str, Any],
    expected: list[int],
):
    repository = MyRepository()
    next_ = None
    result = []
    index_counter = 0
    while response := await repository.list(
        CursorPaginationParams(limit=limit, next=next_),
        sorting=[
            getattr(getattr(repository.__sort__, field), op)() for field, op in sorting
        ],  # TODO: this list needs to be initialized here... unidentified bug
        filtering=filtering,
    ):
        result += [i.another_id for i in response.items]
        assert response.index == index_counter
        index_counter += limit
        if not response.next:
            break

        next_ = response.next

    assert result == expected


@pytest.mark.parametrize(
    "projection",
    [
        [MyRepository.__fields__.is_boolean],
        [MyRepository.__fields__.is_boolean, MyRepository.__fields__.float_number],
        [MyRepository.__fields__.is_boolean, MyRepository.__fields__.float_number, MyRepository.__fields__.int_number],
    ],
)
@pytest.mark.asyncio()
async def test_list_with_projection(projection: List[SortableFieldEnum]):
    repository = MyRepository()
    next_ = None

    while response := await repository.list(
        CursorPaginationParams(limit=10, next=next_),
        projection=projection,
    ):
        for item in response.items:
            item_dict = item.dict()
            assert len(item_dict) == len(projection)
            for field in projection:
                assert field.value in item_dict
            # TODO: need to test nested projection

        if not response.next:
            break

        next_ = response.next


@pytest.mark.asyncio()
async def test_get(request: FixtureRequest):
    repository = MyRepository()
    first_doc = get_first_doc_from_cache(request, CACHE_KEY, MyModel)
    doc = await repository.get(first_doc.id)
    assert doc == first_doc


@pytest.mark.asyncio()
async def test_exists(request: FixtureRequest):
    repository = MyRepository()
    first_doc = get_first_doc_from_cache(request, CACHE_KEY)
    response = await repository.exists(first_doc["id"])
    assert response is True


@pytest.mark.asyncio()
async def test_get__when_entity__does_not_exists__raises_entity_not_found_error():
    repository = MyRepository()
    with pytest.raises(EntityNotFoundError):
        await repository.exists(PydanticObjectId(), should_error=True)


@pytest.mark.asyncio()
async def test_get__when_entity__does_not_exists__and_should_error_false_does_not_raises_entity_not_found_error():
    repository = MyRepository()
    try:
        doc = await repository.exists(PydanticObjectId(), should_error=False)
        assert doc is False
    except EntityNotFoundError:
        pytest.fail(f"test should not raise {EntityNotFoundError.__name__}")


@pytest.mark.asyncio()
async def test_add__when__entity_already_exists__raises_entity_already_exists_error(request: FixtureRequest):
    repository = MyRepository()
    entity = get_first_doc_from_cache(request, CACHE_KEY, MyModel)
    with pytest.raises(EntityAlreadyExistsError):
        await repository.add(entity)


@pytest.mark.asyncio()
async def test_bulk_create():
    repository = MyRepository()
    size = 10
    a = MyModel(
        another_id=1,
        created_at=datetime.datetime(2023, 1, 1, 0, 1),
        int_number=1,
        float_number=1,
        is_boolean=True,
    )
    bulk = [a.copy() for _ in range(size)]
    response = await repository.bulk_create(bulk)

    assert not response.has_errors
    for i in range(size):
        assert response.items[i].status == BulkItemStatusEnum.OK
        assert isinstance(response.items[i].id, ObjectId)


@pytest.mark.asyncio()
async def test_bulk_create__when_has_errors__then_mixed_response_returned():
    repository = MyRepository()
    size = 10
    id_ = PydanticObjectId()
    a = MyModel(
        id=id_,
        another_id=1,
        created_at=datetime.datetime(2023, 1, 1, 0, 1),
        int_number=1,
        float_number=1,
        is_boolean=True,
    )
    bulk = [a.copy() for _ in range(size)]
    response = await repository.bulk_create(bulk)

    assert response.has_errors
    # the first will always success because it's the first item inserted with `id_`
    first = response.items[0]
    assert first.status == BulkItemStatusEnum.OK
    assert isinstance(first.id, PydanticObjectId)
    for i in range(1, size):
        assert response.items[i].status == BulkItemStatusEnum.ERROR
        assert response.items[i].message
