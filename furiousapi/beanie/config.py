from furiousapi.core.config import BaseConnectionSettings
from pydantic import BaseModel, Field
from pydantic.networks import MultiHostDsn


class MongoDBDsn(MultiHostDsn):
    allowed_schemes = "mongodb"
    user_required = False


class MongoDBConnectionOptions(BaseModel):
    server_selection_timeout_ms: int = 5000


class MongoDBConnectionSettings(BaseConnectionSettings[MongoDBDsn, MongoDBConnectionOptions]):
    options: MongoDBConnectionOptions = Field(default_factory=MongoDBConnectionOptions)
    should_drop_indexes: bool = False
