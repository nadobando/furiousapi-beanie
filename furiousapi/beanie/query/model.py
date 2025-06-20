import dataclasses
from typing import Type, ClassVar, Dict, Any, Optional

from furiousapi.core.types import TEntity
from furiousapi.rql.models import ModelRQL, TransformerConfig

from furiousapi.beanie.query.transform import MongoRQLTransform


@dataclasses.dataclass
class MongoTransformerConfig(TransformerConfig):
    fetch_links: Optional[bool] = None


class RQLModelMongo(ModelRQL[TEntity]):
    __model__: Type[TEntity]
    __transformer__ = MongoRQLTransform
    __transformer_params__: MongoTransformerConfig = MongoTransformerConfig()
    __repository_query_params__: ClassVar[Dict[str, Any]] = {}
