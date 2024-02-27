import functools
import logging
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Dict,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
)

from flatten_dict import unflatten
from furiousapi.api.pagination import (
    AllPaginationStrategies,
    PaginatedResponse,
    PaginationStrategyEnum,
)
from furiousapi.api.responses import (
    BulkItemError,
    BulkItemSuccess,
    BulkResponseModel,
    BulkResponseModelUnion,
)
from furiousapi.core.types import TModelFields, TSortableFields
from furiousapi.db.exceptions import (
    EntityAlreadyExistsError,
    EntityNotFoundError,
    FuriousBulkError,
)
from furiousapi.db.repository import BaseRepository, ModelDependency, RepositoryConfig
from furiousapi.db.utils import create_subset_model
from pydantic import BaseModel, Field
from pymongo import IndexModel
from pymongo.errors import BulkWriteError, DuplicateKeyError

from beanie import BulkWriter, Document, PydanticObjectId
from beanie.exceptions import DocumentNotFound
from beanie.odm.operators.find.logical import Or
from beanie.odm.operators.update.general import Set
from beanie.operators import Eq
from furiousapi.beanie.models import BeanieAllOptionalMeta, beanie_document_query
from furiousapi.beanie.pagination import get_paginator

from .utils import _get_bulk_query_by_unique_index

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

    from pymongo.client_session import ClientSession

    from beanie.odm.operators.find import BaseFindOperator
    from furiousapi.beanie.models import FuriousMongoModel

logger = logging.getLogger(__name__)


def model_fields_to_projection(projection: "Iterable[TModelFields]") -> Optional[dict]:
    return projection and unflatten({x.value: 1 for x in projection}, splitter=lambda x: x.split(".")) or None


class IdProjectedModel(BaseModel):
    id: Union[str, PydanticObjectId] = Field(alias="_id")


TDocument = TypeVar("TDocument", bound=Document)


class BaseMongoRepository(BaseRepository[TDocument]):
    __model__: Type[TDocument]

    class Config(RepositoryConfig):
        fields_exclude = ("revision_id",)
        sort_exclude = ("revision_id",)
        model_to_query: ClassVar[ModelDependency] = beanie_document_query
        filter_model = BeanieAllOptionalMeta

    @functools.cached_property
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
        id_ = PydanticObjectId.is_valid(identifiers) and PydanticObjectId(identifiers) or identifiers
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
        id_ = PydanticObjectId.is_valid(identifiers) and PydanticObjectId(identifiers) or identifiers
        model: TDocument = await self.__model__.get(
            id_,
            projection_model=projection and create_subset_model(self.__model__, projection) or None,
        )
        if not model and should_error:
            raise EntityNotFoundError(self.__model__, identifiers)

        return model

    async def find_one(self, criteria: "BaseFindOperator") -> Optional[TDocument]:
        return await self.__model__.find_one(criteria)

    async def list(
        self,
        pagination: AllPaginationStrategies,
        projection: Optional[List[TModelFields]] = None,
        sorting: Optional[List["TSortableFields"]] = None,
        filtering: Optional[Union[dict, "FuriousMongoModel"]] = None,
    ) -> PaginatedResponse[TDocument]:
        if filtering:
            where = {}
            for i in [Eq(k, v) for k, v in filtering.items()]:
                where.update(i.query)

            query = self.__model__.find(where)
        else:
            query = self.__model__.find()

        if not sorting and pagination.pagination_type == PaginationStrategyEnum.CURSOR:
            sorting = [+self.__sort__("id")]

        for i in cast(List["TSortableFields"], sorting):
            if projection and i not in projection:
                projection.append(i)

        projection = projection and unflatten({x.value: 1 for x in projection}, splitter=lambda x: x.split("."))
        if projection and pagination.pagination_type == PaginationStrategyEnum.CURSOR:
            projection[self.__model__.id] = 1

        returned_model = projection and create_subset_model(self.__model__, projection) or self.__model__
        query = query.project(returned_model)

        init_params = {
            "model": self.__model__,
            "sorting": sorting,
            "id_fields": ["id"],
            "sort_enum": self.__sort__,
        }
        paginator = get_paginator(pagination.pagination_type)(**init_params)
        return await paginator.get_page(query, pagination.limit, pagination.next)

    async def add(self, entity: TDocument, session: "ClientSession" = None, **kwargs) -> TDocument:
        try:
            return await self.__model__.insert_one(entity, session=session, **kwargs)
        except DuplicateKeyError as exc:
            raise EntityAlreadyExistsError(self.__model__, entity.id) from exc

    async def delete(self, entity: TDocument, **_) -> None:
        await self.__model__.delete(entity)

    # noinspection PyMethodOverriding
    async def update(
        self, entity: TDocument, changes: Set, bulk_writer: Optional[BulkWriter] = None
    ) -> Optional[TDocument]:
        try:
            return await entity.update(changes, bulk_writer=bulk_writer)
        except DocumentNotFound as e:
            raise EntityNotFoundError(self.__model__, entity.id) from e

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
            return bulk_writer.commit()

    async def bulk_upsert(
        self, bulk: List[Document], upsert_factory: "Optional[Callable[..., TDocument]]" = None
    ) -> Any:
        async with BulkWriter() as bulk_writer:
            for i in bulk:
                await self.__model__.find_one(self.__model__.id == i).upsert(
                    bulk_writer=bulk_writer,
                    on_insert=upsert_factory,
                )
            return bulk_writer.commit()

    def session(self) -> "ClientSession":
        return self.__model__.get_settings().motor_db.client.start_session()
