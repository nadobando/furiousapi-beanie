import dataclasses

from furiousapi.core.types import TEntity
from furiousapi.rql.models import ModelRQL, TransformerConfig

from furiousapi.beanie.query.transform import MongoRQLTransform


@dataclasses.dataclass
class MongoTransformerConfig(TransformerConfig):
    fetch_links: bool | None = None


class RQLModelMongo(ModelRQL[TEntity]):
    __model__: type[TEntity]
    __transformer__ = MongoRQLTransform
    __transformer_params__: MongoTransformerConfig = MongoTransformerConfig()
