import logging
from functools import cached_property
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
    Iterable,
)

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

    from pymongo.client_session import ClientSession

    from beanie.odm.operators.find import BaseFindOperator

logger = logging.getLogger(__name__)


def model_fields_to_projection(projection: "Iterable[TModelFields]") -> Optional[dict]:
    return (projection and unflatten({x.value: 1 for x in projection}, splitter=lambda x: x.split("."))) or None


class IdProjectedModel(BaseModel):
    id: Union[str, PydanticObjectId] = Field(alias="_id")


TDocument = TypeVar("TDocument", bound=Document)


class BaseMongoRepository(BaseRepository[TDocument]):
    __model__: Type[TDocument]

    def __init_paginators__(self) -> None:
        self.__paginators__[PaginationStrategyEnum.CURSOR] = BeanieCursorPagination(
            self.__primary_keys__, self.__model__
        )
        self.__paginators__[PaginationStrategyEnum.OFFSET] = BeanieOffsetPagination()

    @cached_property
    def __primary_keys__(self):
        return {"id"}

    @cached_property
    def __unique_keys__(self) -> Optional[IndexModel]:
        for i in self.__model__.get_settings().indexes:
            if isinstance(i, IndexModel) and i.document.get("unique"):
                return i
        return None

    async def exists(
        self,
        identifiers: Union[PydanticObjectId, int, str, Dict[str, Any], Tuple[Any]],
        *,
        should_error: bool = False,
    ) -> bool:
        id_ = (PydanticObjectId.is_valid(identifiers) and PydanticObjectId(identifiers)) or identifiers
        count = await self.__model__.find_one(self.__model__.id == id_).count()
        if count > 0:
            return True

        if should_error:
            raise EntityNotFoundError(self.__model__, identifiers)
        return False

    async def get(
        self,
        identifiers: Union[PydanticObjectId, int, str, Dict[str, Any], Tuple[Any]],
        projection: "Optional[Iterable[TModelFields]]" = None,
        *,
        should_error: bool = True,
    ) -> Optional[TDocument]:
        projection = projection and model_fields_to_projection(projection)
        id_ = (PydanticObjectId.is_valid(identifiers) and PydanticObjectId(identifiers)) or identifiers
        model: TDocument = await self.__model__.get(
            id_,
            projection_model=(projection and create_subset_model(self.__model__, projection)) or None,
        )
        if not model and should_error:
            raise EntityNotFoundError(self.__model__, identifiers)

        return model

    async def find_one(self, criteria: "BaseFindOperator") -> Optional[TDocument]:
        return await self.__model__.find_one(criteria)

    async def add(self, entity: TDocument, session: "ClientSession" = None, **kwargs) -> TDocument:
        try:
            return await self.__model__.insert_one(entity, session=session, **kwargs, link_rule=WriteRules.WRITE)
        except DuplicateKeyError as exc:
            raise EntityAlreadyExistsError(self.__model__, entity.id) from exc

    async def delete(self, id_: Union[str, PydanticObjectId], **_) -> None:
        if isinstance(id_, str):
            id_ = PydanticObjectId(id_)
        await self.__model__.find_one(self.__model__.id == id_).delete()

    async def _load_persisted(
        self, id_: Union[PydanticObjectId, str], expected_id: Optional[PydanticObjectId]
    ) -> TDocument:
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
        id_: Union[PydanticObjectId, str],
        partial: TDocument,
        bulk_writer: Optional[BulkWriter] = None,
    ) -> Optional[TDocument]:
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
        id_: Union[PydanticObjectId, str],
        entity: TDocument,
        bulk_writer: Optional[BulkWriter] = None,
    ) -> Optional[TDocument]:
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

    async def bulk_create(self, bulk: List[Document]) -> BulkResponseModel:
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

            if filter_by_uniq:
                find_queries = _get_bulk_query_by_unique_index(self.__model__, bulk_copy, filter_by_uniq)
            else:
                if not all(hasattr(x, "id") for x in bulk_copy):
                    msg = f"{self.__model__.name_} bulk error no unique index and _id is generated by mongoDB"
                    raise FuriousBulkError(msg) from e

                find_queries = [self.__model__.id == item.id for item in bulk_copy]

            if find_queries:
                success_result: List[IdProjectedModel] = await self.__model__.find(
                    Or(*find_queries), projection_model=IdProjectedModel
                ).to_list()

            else:
                success_result = []

            result: List[BulkResponseModelUnion] = [BulkItemSuccess(id=i.id) for i in success_result]

            for error_index, error_msg in error_indexes:
                result.insert(error_index, BulkItemError(detail=f"mongodb: {error_msg}"))

            return BulkResponseModel(items=result, has_errors=True)
        else:
            result: List[BulkItemSuccess] = [BulkItemSuccess(id=i) for i in insert_many_result.inserted_ids]
            return BulkResponseModel(items=result)

    async def bulk_update(self, bulk: List[Document], *, upsert: bool = False) -> Any:
        async with BulkWriter() as bulk_writer:
            for i in bulk:
                self.__model__.find_one(self.__model__.id == i.id).update(
                    Set(i.dict()),
                    upsert=upsert,
                    bulk_writer=bulk_writer,
                )
            return await bulk_writer.commit()

    async def bulk_delete(self, bulk: List[Union[str, PydanticObjectId]]) -> Any:
        async with BulkWriter() as bulk_writer:
            for i in bulk:
                await self.__model__.find_one(self.__model__.id == i).delete(bulk_writer=bulk_writer)
            return await bulk_writer.commit()

    async def bulk_upsert(
        self, bulk: List[Document], upsert_factory: "Optional[Callable[..., TDocument]]" = None
    ) -> Any:
        async with BulkWriter() as bulk_writer:
            for i in bulk:
                await self.__model__.find_one(self.__model__.id == i).upsert(
                    bulk_writer=bulk_writer,
                    on_insert=upsert_factory,
                )
            return await bulk_writer.commit()

    async def session(self) -> "AsyncIOMotorClientSession":
        return await self.__model__.get_settings().motor_db.client.start_session()

    def query(self, query: Any = None, filter_: "BaseFindOperator" = None, **kwargs) -> "FindMany":
        if not query:
            query = self.__model__.find()
        if filter_:
            query = query.find(filter_)

        return query

    async def execute(self, query: FindMany) -> Iterable[TDocument]:
        return await query.to_list()
