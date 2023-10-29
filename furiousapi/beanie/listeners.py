from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any, Iterable, Optional, Union

from pymongo import monitoring

if TYPE_CHECKING:
    from pymongo.monitoring import (
        CommandFailedEvent,
        CommandStartedEvent,
        CommandSucceededEvent,
    )

logger = logging.getLogger("pymongo")


class StrJSONEncoder(json.JSONEncoder):
    def default(self, o: Any) -> Any:
        try:
            return super().default(o)
        except TypeError:
            return repr(o)


class CommandLogger(monitoring.CommandListener):
    def __init__(
        self,
        commands_to_log: Optional[Iterable] = None,
        *,
        started: bool = True,
        succeeded: bool = False,
        failed: bool = True,
    ) -> None:
        super().__init__()
        self._failed = failed
        self._succeeded = succeeded
        self._started = started
        self.commands_to_log = commands_to_log

    def _should_log(self, event: Union[CommandStartedEvent, CommandSucceededEvent, CommandFailedEvent]) -> bool:
        return self.commands_to_log is None or (
            isinstance(
                self.commands_to_log,
                (list, tuple),
            )
            and event.command_name in self.commands_to_log
        )

    def started(self, event: CommandStartedEvent) -> None:
        if self._started and self._should_log(event):
            logger.debug(
                f"mongo {event.command_name} started",
                extra={
                    "database_name": event.database_name,
                    "connection_id": event.connection_id,
                    "operation_id": event.operation_id,
                    "request_id": event.request_id,
                    "service_id": event.service_id,
                    "command": (
                        hasattr(event.command, "to_dict")
                        and json.dumps(event.command.to_dict(), cls=StrJSONEncoder)
                        or event.command
                    ),
                },
            )

    def succeeded(self, event: CommandSucceededEvent) -> None:
        if self._succeeded and self._should_log(event):
            logger.debug(
                f"mongo {event.command_name} success",
                extra={
                    "command_name": event.command_name,
                    "service_id": event.service_id,
                    "request_id": event.request_id,
                    "operation_id": event.operation_id,
                    "connection_id": event.connection_id,
                    "duration_micros": event.duration_micros,
                    "reply": event.reply,
                },
            )

    def failed(self, event: CommandFailedEvent) -> None:
        if self._failed and self._should_log(event):
            logger.error(
                f"mongo {event.command_name} failed",
                extra={
                    "duration_micros": event.duration_micros,
                    "operation_id": event.operation_id,
                    "request_id": event.request_id,
                    "connection_id": event.connection_id,
                    "service_id": event.service_id,
                    "failure": event.failure,
                },
            )
