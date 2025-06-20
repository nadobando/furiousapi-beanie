import datetime
from typing import Optional

from beanie import Document, Link
from beanie.odm.settings.document import DocumentSettings
from fastapi.params import Depends
from furiousapi.api import ModelController
from furiousapi.pydantic import PYDANTIC_V2
from pydantic import BaseModel

from furiousapi.beanie.models import FuriousMongoModel
from furiousapi.beanie.query.model import RQLModelMongo
from furiousapi.beanie.repository import BaseMongoRepository


class InnerDoc(BaseModel):
    name: str

    class Settings:
        use_revision = False


class Foreign(Document):
    name: str
    inner: Optional["InnerDoc"] = None
    _document_settings = DocumentSettings(use_revision=False, name="foreign")


class OneToMany(Document):
    name: str

    _document_settings = DocumentSettings(use_revision=False, name="otm")


class MyModel(Document):
    created_at: Optional[datetime.datetime] = None
    another_id: int
    int_number: int
    float_number: int
    is_boolean: bool
    nullable: Optional[int] = None
    foreign: Optional[Link[Foreign]] = None
    inner: Optional[InnerDoc] = None

    _document_settings = DocumentSettings(use_revision=True, name="my_model")

    if PYDANTIC_V2:
        model_config = FuriousMongoModel.model_config
    else:

        class Config(FuriousMongoModel.Config):
            pass


class MyRepository(BaseMongoRepository[MyModel]): ...


class MyModelRQL(RQLModelMongo):
    __model__ = MyModel


class MyController(ModelController):
    repository = Depends(MyRepository)
    __filtering__ = MyModelRQL
