import datetime
from typing import Optional

from beanie import Document
from fastapi.params import Depends
from furiousapi.api import ModelController
from furiousapi.pydantic import PYDANTIC_V2

from furiousapi.beanie.models import FuriousMongoModel
from furiousapi.beanie.repository import BaseMongoRepository


class MyModel(Document):
    created_at: datetime.datetime
    another_id: int
    int_number: int
    float_number: int
    is_boolean: bool
    nullable: Optional[int] = None

    class Settings:
        name = "my_model"
        use_revision = False

    if PYDANTIC_V2:
        model_config = FuriousMongoModel.model_config
    else:

        class Config(FuriousMongoModel.Config):
            pass


class MyRepository(BaseMongoRepository[MyModel]): ...


class MyController(ModelController):
    repository = Depends(MyRepository)
