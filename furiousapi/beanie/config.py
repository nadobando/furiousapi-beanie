from furiousapi.core.config import BaseConnectionSettings, MongoDBDsn
from pydantic import BaseModel, Field


class MongoDBConnectionOptions(BaseModel):
    server_selection_timeout_ms: int = 5000


class MongoDBConnectionSettings(BaseConnectionSettings[MongoDBDsn, MongoDBConnectionOptions]):
    options: MongoDBConnectionOptions = Field(default_factory=MongoDBConnectionOptions)
    should_drop_indexes: bool = False
