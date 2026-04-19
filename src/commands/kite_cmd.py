from dataclasses import dataclass
from typing import Any, TYPE_CHECKING
from src.commands.base import Command

if TYPE_CHECKING:
    from src.clients.mcp_client import KiteMCPClient

@dataclass
class KiteToolCommand(Command):
    client: "KiteMCPClient"
    tool_name: str
    params: dict

    async def execute_async(self) -> Any:
        return await self.client._call_tool(self.tool_name, self.params)

    def execute(self) -> Any:
        """KiteToolCommand is inherently async as it calls a web API."""
        raise RuntimeError("KiteToolCommand must be executed via run_async()")

    # No undo — Kite tool calls (mostly reads) are idempotent or don't support undo easily
