import logging
from functools import cached_property
from typing import (
    TYPE_CHECKING,
    Any,
    TypeVar,
    Union,
    cast,
)
from collections.abc import Iterable

from beanie import BulkWriter, Document, PydanticObjectId, WriteRules
from beanie.exceptions import DocumentNotFound
from beanie.odm.operators.find.logical import Or
from beanie.odm.operators.update.general import Set
from beanie.odm.queries.find import FindMany
from flatten_dict import unflatten
from furiousapi.api.pagination import PaginationStrategyEnum
from furiousapi.api.responses import (
    BulkItemError,
    BulkItemSuccess,
    BulkResponseModel,
    BulkResponseModelUnion,
)
from furiousapi.db.exceptions import (
    EntityAlreadyExistsError,
    EntityNotFoundError,
    FuriousBulkError,
)
from furiousapi.db.repository import BaseRepository
from furiousapi.pydantic import PYDANTIC_V2

from pydantic import BaseModel, Field
from pymongo import IndexModel
from pymongo.errors import BulkWriteError, DuplicateKeyError

from .pagination import BeanieCursorPagination, BeanieOffsetPagination
from .utils import _get_bulk_query_by_unique_index, create_subset_model

if TYPE_CHECKING:
    from motor.motor_asyncio import AsyncIOMotorClientSession
    from furiousapi.core.types import TModelFields
    from collections.abc import Callable

    from beanie.odm.operators.find import BaseFindOperator

logger = logging.getLogger(__name__)


def model_fields_to_projection(projection: "Iterable[TModelFields]") -> dict | None:
    return (projection and unflatten({x.value: 1 for x in projection}, splitter=lambda x: x.split("."))) or None


class IdProjectedModel(BaseModel):
    id: Union[str, PydanticObjectId] = Field(alias="_id")  # noqa: UP007


TDocument = TypeVar("TDocument", bound=Document)


class BaseMongoRepository(BaseRepository[TDocument]):
    __model__: type[TDocument]

    def __init_paginators__(self) -> None:
        self.__paginators__[PaginationStrategyEnum.CURSOR] = BeanieCursorPagination(
            self.__primary_keys__, self.__model__
        )
        self.__paginators__[PaginationStrategyEnum.OFFSET] = BeanieOffsetPagination()

    @cached_property
    def __primary_keys__(self):
        return {"id"}

    @cached_property
    def __unique_keys__(self) -> IndexModel | None:
        for i in self.__model__.get_settings().indexes:
            if isinstance(i, IndexModel) and i.document.get("unique"):
                return i
        return None

    async def exists(
        self,
        identifiers: PydanticObjectId | int | str | dict[str, Any] | tuple[Any],
        *,
        should_error: bool = False,
    ) -> bool:
        # PydanticObjectId.is_valid guards the str/ObjectId/bytes path; otherwise we pass
        # the identifier through unchanged (e.g. composite-key dict / tuple).
        id_: Any = (
            PydanticObjectId.is_valid(identifiers) and PydanticObjectId(cast("Any", identifiers))
        ) or identifiers
        count = await self.__model__.find_one(self.__model__.id == id_).count()
        if count > 0:
            return True

        if should_error:
            raise EntityNotFoundError(self.__model__, identifiers)
        return False

    async def get(
        self,
        identifiers: PydanticObjectId | int | str | dict[str, Any] | tuple[Any],
        projection: "Iterable[TModelFields] | None" = None,
        *,
        should_error: bool = True,
    ) -> TDocument | None:
        projection_dict: dict | None = model_fields_to_projection(projection) if projection else None
        id_: Any = (
            PydanticObjectId.is_valid(identifiers) and PydanticObjectId(cast("Any", identifiers))
        ) or identifiers
        model: TDocument | None = await self.__model__.get(
            id_,
            projection_model=create_subset_model(self.__model__, projection_dict) if projection_dict else None,
        )
        if not model and should_error:
            raise EntityNotFoundError(self.__model__, identifiers)

        return model

    async def find_one(self, criteria: "BaseFindOperator") -> TDocument | None:
        return await self.__model__.find_one(criteria)

    async def add(
        self,
        entity: TDocument,
        session: "AsyncIOMotorClientSession | None" = None,
        **kwargs,
    ) -> TDocument:
        try:
            inserted = await self.__model__.insert_one(entity, session=session, **kwargs, link_rule=WriteRules.WRITE)
        except DuplicateKeyError as exc:
            raise EntityAlreadyExistsError(self.__model__, entity.id) from exc
        if inserted is None:
            raise EntityNotFoundError(self.__model__, entity.id)
        return inserted

    async def delete(self, id_: str | PydanticObjectId, **_) -> None:
        if isinstance(id_, str):
            id_ = PydanticObjectId(id_)
        await self.__model__.find_one(self.__model__.id == id_).delete()

    async def _load_persisted(self, id_: PydanticObjectId | str, expected_id: PydanticObjectId | None) -> TDocument:
        if expected_id and str(id_) != str(expected_id):
            raise EntityNotFoundError(self.__model__, id_)
        oid = PydanticObjectId(id_) if isinstance(id_, str) else id_
        existing = await self.__model__.get(oid)
        if existing is None:
            raise EntityNotFoundError(self.__model__, id_)
        return existing

    # noinspection PyMethodOverriding
    async def patch(
        self,
        id_: PydanticObjectId | str,
        partial: TDocument,
        bulk_writer: BulkWriter | None = None,
    ) -> TDocument | None:
        """Partial update — only fields explicitly set on `partial` are written."""
        existing = await self._load_persisted(id_, partial.id)
        if PYDANTIC_V2:
            d = partial.model_dump(by_alias=True, exclude_unset=True, exclude={"id"})
        else:
            d = partial.dict(by_alias=True, exclude_unset=True, exclude={"id"})
        try:
            await existing.update(Set(d), bulk_writer=bulk_writer)
        except DocumentNotFound as e:
            raise EntityNotFoundError(self.__model__, id_) from e
        return existing

    # noinspection PyMethodOverriding
    async def replace(
        self,
        id_: PydanticObjectId | str,
        entity: TDocument,
        bulk_writer: BulkWriter | None = None,
    ) -> TDocument | None:
        """Full replacement — every field on the entity is written, defaults included."""
        existing = await self._load_persisted(id_, entity.id)
        if PYDANTIC_V2:
            d = entity.model_dump(by_alias=True, exclude={"id"})
        else:
            d = entity.dict(by_alias=True, exclude={"id"})
        try:
            await existing.update(Set(d), bulk_writer=bulk_writer)
        except DocumentNotFound as e:
            raise EntityNotFoundError(self.__model__, id_) from e
        return existing

    async def bulk_create(self, bulk: list[Document]) -> BulkResponseModel:
        bulk_copy = bulk.copy()
        try:
            insert_many_result = await self.__model__.insert_many(bulk_copy, ordered=False)
        except BulkWriteError as e:
            logger.exception("mongo bulk create error")

            filter_by_uniq = self.__unique_keys__

            error_indexes = [(error["index"], error["errmsg"]) for error in e.details["writeErrors"]]
            reversed_errors_indexes = sorted((x[0] for x in error_indexes), reverse=True)
            for i in reversed_errors_indexes:
                bulk_copy.pop(i)

            find_queries: list[BaseFindOperator]
            if filter_by_uniq:
                find_queries = _get_bulk_query_by_unique_index(self.__model__, bulk_copy, filter_by_uniq)
            else:
                if not all(hasattr(x, "id") for x in bulk_copy):
                    msg = f"{self.__model__.name_} bulk error no unique index and _id is generated by mongoDB"
                    raise FuriousBulkError(msg) from e

                # Document.id == ... is overloaded to return a BaseFindOperator at runtime.
                find_queries = [self.__model__.id == item.id for item in bulk_copy]  # type: ignore[misc]

            if find_queries:
                success_result: list[IdProjectedModel] = await self.__model__.find(
                    Or(*find_queries), projection_model=IdProjectedModel
                ).to_list()

            else:
                success_result = []

            result: list[BulkResponseModelUnion] = [BulkItemSuccess(id=i.id) for i in success_result]

            for error_index, error_msg in error_indexes:
                result.insert(error_index, BulkItemError(detail=f"mongodb: {error_msg}"))

            return BulkResponseModel(items=result, has_errors=True)
        else:
            result: list[BulkItemSuccess] = [BulkItemSuccess(id=i) for i in insert_many_result.inserted_ids]
            return BulkResponseModel(items=result)

    async def bulk_update(self, bulk: list[Document], *, upsert: bool = False) -> Any:
        async with BulkWriter() as bulk_writer:
            for i in bulk:
                self.__model__.find_one(self.__model__.id == i.id).update(
                    Set(i.dict()),
                    upsert=upsert,
                    bulk_writer=bulk_writer,
                )
            return await bulk_writer.commit()

    async def bulk_delete(self, bulk: list[str | PydanticObjectId]) -> Any:
        async with BulkWriter() as bulk_writer:
            for i in bulk:
                await self.__model__.find_one(self.__model__.id == i).delete(bulk_writer=bulk_writer)
            return await bulk_writer.commit()

    async def bulk_upsert(self, bulk: list[Document], upsert_factory: "Callable[..., TDocument] | None" = None) -> Any:
        async with BulkWriter() as bulk_writer:
            for i in bulk:
                # Beanie's upsert.on_insert is typed as a DocType, but this codebase
                # passes a factory callable. Behavior is preserved; the type variance
                # is suppressed.
                await self.__model__.find_one(self.__model__.id == i).upsert(  # type: ignore[type-var]
                    bulk_writer=bulk_writer,
                    on_insert=upsert_factory,
                )
            return await bulk_writer.commit()

    async def session(self) -> "AsyncIOMotorClientSession":
        motor_db = self.__model__.get_settings().motor_db
        if motor_db is None:
            raise RuntimeError(f"{self.__model__.__name__} is not bound to a motor database")
        return await motor_db.client.start_session()

    def query(self, query: Any = None, filter_: "BaseFindOperator | None" = None, **kwargs) -> "FindMany":
        if not query:
            query = self.__model__.find()
        if filter_:
            query = query.find(filter_)

        return query

    async def execute(self, query: FindMany) -> Iterable[TDocument]:
        return await query.to_list()
