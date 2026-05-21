"""End-to-end API tests for the example Beanie app.

These tests boot the example FastAPI app (with its lifespan, including
`init_beanie`) and exercise the full HTTP surface: CRUD lifecycle, cursor
pagination, RQL filters/sorts, Item+Review relationships, and error paths.

State is shared across tests within the session — later tests can rely on
data created by earlier ones. The Mongo testcontainer URI from the existing
`mongo_uri` fixture is injected into `example.dependencies.client` before
the lifespan runs.
"""

from __future__ import annotations

from http import HTTPStatus
from typing import TYPE_CHECKING

import pytest
from motor.motor_asyncio import AsyncIOMotorClient
from starlette.testclient import TestClient

if TYPE_CHECKING:
    from typing import Iterator

    from fastapi import FastAPI


@pytest.fixture(scope="session")
def app(mongo_uri: str) -> "FastAPI":
    """The example FastAPI app wired to the testcontainer Mongo."""
    from example import dependencies

    dependencies.client = AsyncIOMotorClient(mongo_uri)
    from example.app import app as _app

    return _app


@pytest.fixture(scope="session")
def client(app: "FastAPI") -> "Iterator[TestClient]":
    with TestClient(app) as c:
        yield c


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

ITEM_PAYLOAD = {
    "name": "Widget A",
    "description": "first widget",
    "details": {"data1": "x", "data2": "y"},
}


def make_item(name: str, description: str = "desc") -> dict:
    return {"name": name, "description": description, "details": {"data1": "a", "data2": "b"}}


# Module-level state for sharing IDs across test methods within the session.
# Class attrs don't survive because conftest's autouse `init_my_model` fixture
# re-initializes beanie between tests, and the resulting beanie state reset
# coincides with what looks like class-attr reset (collection drops, etc.).
state: dict = {}


# ---------------------------------------------------------------------------
# CRUD lifecycle
# ---------------------------------------------------------------------------


class TestCrudLifecycle:
    """Create → Get → Update → Delete → 404 against a single Item."""

    def test_create(self, client: TestClient) -> None:
        r = client.post("/item/", json=ITEM_PAYLOAD)
        assert r.status_code == HTTPStatus.OK, r.text
        body = r.json()
        assert body["name"] == ITEM_PAYLOAD["name"]
        assert "_id" in body
        # Stash on the class for later steps
        state["crud_item_id"] = body["_id"]

    def test_get(self, client: TestClient) -> None:
        r = client.get(f"/item/{state["crud_item_id"]}")
        assert r.status_code == HTTPStatus.OK, r.text
        assert r.json()["name"] == ITEM_PAYLOAD["name"]

    def test_update(self, client: TestClient) -> None:
        updated = {**ITEM_PAYLOAD, "name": "Widget A — renamed"}
        r = client.put(f"/item/{state['crud_item_id']}", json=updated)
        assert r.status_code == HTTPStatus.OK, r.text
        r = client.get(f"/item/{state['crud_item_id']}")
        assert r.json()["name"] == "Widget A — renamed"

    def test_delete(self, client: TestClient) -> None:
        r = client.delete(f"/item/{state["crud_item_id"]}")
        assert r.status_code in (HTTPStatus.OK, HTTPStatus.NO_CONTENT), r.text

    def test_get_404_after_delete(self, client: TestClient) -> None:
        r = client.get(f"/item/{state["crud_item_id"]}")
        assert r.status_code == HTTPStatus.NOT_FOUND


# ---------------------------------------------------------------------------
# List + cursor pagination
# ---------------------------------------------------------------------------


MAX_PAGES = 10


class TestCursorPagination:
    page_size = 5
    total = 12

    def test_seed(self, client: TestClient) -> None:
        for i in range(self.total):
            r = client.post("/item/", json=make_item(f"page-{i:03d}"))
            assert r.status_code == HTTPStatus.OK, r.text

    def test_page_through(self, client: TestClient) -> None:
        seen: list[str] = []
        next_cursor: str | None = None
        page_count = 0
        while True:
            params = {"limit": self.page_size}
            if next_cursor:
                params["next"] = next_cursor
            r = client.get("/item/", params=params)
            assert r.status_code == HTTPStatus.OK, r.text
            body = r.json()
            seen.extend(item["name"] for item in body["items"] if item["name"].startswith("page-"))
            page_count += 1
            next_cursor = body.get("next")
            if not next_cursor or page_count > MAX_PAGES:
                break
        # We seeded `total` items with the page-* prefix
        assert len([n for n in seen if n.startswith("page-")]) >= self.total


# ---------------------------------------------------------------------------
# RQL filters
# ---------------------------------------------------------------------------


class TestRQLFilters:
    """Filter list endpoint via the `q` query parameter (ItemRQL)."""

    target_name = "rql-target"

    def test_seed(self, client: TestClient) -> None:
        for i in range(3):
            r = client.post("/item/", json=make_item(f"{self.target_name}-{i}"))
            assert r.status_code == HTTPStatus.OK
        # Distractor
        client.post("/item/", json=make_item("rql-distractor"))

    def test_eq_filter(self, client: TestClient) -> None:
        name = f"{self.target_name}-0"
        r = client.get("/item/", params={"q": f"eq(name,{name})", "limit": 10})
        assert r.status_code == HTTPStatus.OK, r.text
        names = [it["name"] for it in r.json()["items"]]
        assert name in names

    def test_and_filter(self, client: TestClient) -> None:
        name = f"{self.target_name}-1"
        r = client.get(
            "/item/",
            params={"q": f"and(eq(name,{name}),eq(description,desc))", "limit": 10},
        )
        assert r.status_code == HTTPStatus.OK, r.text
        names = [it["name"] for it in r.json()["items"]]
        assert name in names


# ---------------------------------------------------------------------------
# RQL sorts
# ---------------------------------------------------------------------------


class TestRQLSorts:
    """Verify the `sort` clause in RQL works."""

    prefix = "sort-test"

    def test_seed(self, client: TestClient) -> None:
        for i in [3, 1, 2]:
            r = client.post("/item/", json=make_item(f"{self.prefix}-{i}"))
            assert r.status_code == HTTPStatus.OK

    def test_sort_asc(self, client: TestClient) -> None:
        # RQL: semicolons join clauses. like(field, 'pattern%') filters; sort(+field) ascending.
        r = client.get(
            "/item/",
            params={"q": f"like(name,'{self.prefix}-%');sort(+name)", "limit": 50},
        )
        assert r.status_code == HTTPStatus.OK, r.text
        names = [it["name"] for it in r.json()["items"] if it["name"].startswith(self.prefix)]
        assert names == sorted(names), f"expected sorted ascending: {names}"


# ---------------------------------------------------------------------------
# Item + Review relationship
# ---------------------------------------------------------------------------


class TestRelationships:
    """Item and Review CRUD, plus the cross-resource reference link."""

    def test_create_item_with_review_reference(self, client: TestClient) -> None:
        item_resp = client.post("/item/", json=make_item("with-review"))
        assert item_resp.status_code == HTTPStatus.OK, item_resp.text
        item_id = item_resp.json()["_id"]

        # Verify the item can be fetched by id
        get_item = client.get(f"/item/{item_id}")
        assert get_item.status_code == HTTPStatus.OK
        assert get_item.json()["name"] == "with-review"

    def test_create_review_via_create_model(self, client: TestClient) -> None:
        """Regression for CreateModelMixin auto-converting `create_model` to the entity.

        `ReviewController` has `create_model = ReviewCreate` but no override of
        `create()`. Before the fix, the parsed `ReviewCreate` was handed to
        beanie's `insert_one`, which raised
        `TypeError: Inserting document must be of the original document class`.
        """
        item_resp = client.post("/item/", json=make_item("review-target"))
        assert item_resp.status_code == HTTPStatus.OK, item_resp.text
        item_id = item_resp.json()["_id"]

        review_resp = client.post("/review/", json={"text": "great widget", "item": item_id})
        assert review_resp.status_code == HTTPStatus.OK, review_resp.text
        assert review_resp.json()["text"] == "great widget"


# ---------------------------------------------------------------------------
# Error paths
# ---------------------------------------------------------------------------


class TestErrorPaths:
    def test_get_missing_404(self, client: TestClient) -> None:
        # Valid-looking ObjectId that doesn't exist
        r = client.get("/item/507f1f77bcf86cd799439011")
        assert r.status_code == HTTPStatus.NOT_FOUND, r.text

    def test_create_missing_required_field_422(self, client: TestClient) -> None:
        # Missing `details` (required)
        r = client.post("/item/", json={"name": "broken"})
        assert r.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, r.text


# ---------------------------------------------------------------------------
# Filtering safeguard
# ---------------------------------------------------------------------------


class TestFilteringSafeguard:
    """`?q=...` against a controller without `__filtering__` must 400, not crash."""

    def test_q_without_filtering_returns_400(self, client: TestClient) -> None:
        # ReviewController has no `__filtering__` configured (only ItemController does).
        r = client.get("/review/", params={"q": "eq(text,anything)"})
        assert r.status_code == HTTPStatus.BAD_REQUEST, r.text
        body = r.json()
        # FuriousAPIError formats as {"detail": {"status_code": 400, "detail": "..."}}.
        # Be permissive about the exact shape: walk into nested `detail` if present.
        message = body.get("detail")
        if isinstance(message, dict):
            message = message.get("detail", "")
        assert "RQL filtering is not configured" in (message or ""), body
