from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, UTC
import asyncio
import logging
from typing import Any, Optional, TypeVar, Generic, Callable, Awaitable

logger = logging.getLogger(__name__)

T = TypeVar("T")

class Command(ABC):
    @abstractmethod
    def execute(self) -> Any:
        """Execute the command logic."""
        ...

    async def execute_async(self) -> Any:
        """Asynchronous version of execute. Defaults to calling execute()."""
        return self.execute()

    def undo(self) -> None:
        """Reverse the effects of the command."""
        raise NotImplementedError(f"{self.__class__.__name__} does not support undo")

@dataclass
class CommandRecord:
    command: Command
    params: dict
    result: Any
    executed_at: datetime
    success: bool
    error: Optional[str] = None

class CommandRunner:
    def __init__(self, max_retries: int = 3):
        self.history: list[CommandRecord] = []
        self.max_retries = max_retries

    def run(self, cmd: Command, retryable: bool = False) -> Any:
        attempts = self.max_retries if retryable else 1
        last_err = None
        
        for attempt in range(attempts):
            try:
                result = cmd.execute()
                self._record_success(cmd, result)
                return result
            except Exception as e:
                last_err = e
                logger.warning(
                    "Command %s failed (attempt %d/%d): %s",
                    cmd.__class__.__name__, attempt + 1, attempts, e
                )
                if attempt < attempts - 1:
                    continue
        
        self._record_failure(cmd, last_err)
        raise last_err

    async def run_async(self, cmd: Command, retryable: bool = False) -> Any:
        attempts = self.max_retries if retryable else 1
        last_err = None
        
        for attempt in range(attempts):
            try:
                result = await cmd.execute_async()
                self._record_success(cmd, result)
                return result
            except Exception as e:
                last_err = e
                logger.warning(
                    "Async command %s failed (attempt %d/%d): %s",
                    cmd.__class__.__name__, attempt + 1, attempts, e
                )
                if attempt < attempts - 1:
                    # Optional: add exponential backoff for async retries
                    await asyncio.sleep(0.5 * (2 ** attempt))
                    continue
        
        self._record_failure(cmd, last_err)
        raise last_err

    def undo_last(self) -> None:
        for record in reversed(self.history):
            if record.success:
                logger.info("Undoing command: %s", record.command.__class__.__name__)
                record.command.undo()
                # We might want to mark the record as undone or remove it
                return
        raise RuntimeError("Nothing to undo")

    def _record_success(self, cmd: Command, result: Any) -> None:
        self.history.append(CommandRecord(
            command=cmd,
            params=self._get_params(cmd),
            result=result,
            executed_at=datetime.now(UTC),
            success=True,
        ))

    def _record_failure(self, cmd: Command, error: Exception) -> None:
        self.history.append(CommandRecord(
            command=cmd,
            params=self._get_params(cmd),
            result=None,
            executed_at=datetime.now(UTC),
            success=False,
            error=str(error),
        ))

    def _get_params(self, cmd: Command) -> dict:
        # Exclude private fields (starting with _)
        return {k: v for k, v in vars(cmd).items() if not k.startswith("_")}
