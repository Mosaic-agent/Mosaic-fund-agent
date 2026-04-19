import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock
from src.commands.kite_cmd import KiteToolCommand
from src.commands.base import CommandRunner

@pytest.mark.asyncio
async def test_kite_cmd_executes_tool_async():
    client = MagicMock()
    client._call_tool = AsyncMock(return_value={"holdings": []})
    
    cmd = KiteToolCommand(client, "get_holdings", {"param": 1})
    result = await cmd.execute_async()
    
    assert result == {"holdings": []}
    client._call_tool.assert_awaited_once_with("get_holdings", {"param": 1})

@pytest.mark.asyncio
async def test_kite_cmd_retry_on_failure():
    client = MagicMock()
    client._call_tool = AsyncMock(side_effect=[ValueError("fail"), {"holdings": []}])
    
    cmd = KiteToolCommand(client, "get_holdings", {})
    runner = CommandRunner(max_retries=3)
    
    result = await runner.run_async(cmd, retryable=True)
    
    assert result == {"holdings": []}
    assert client._call_tool.await_count == 2
    assert len(runner.history) == 1
    assert runner.history[0].success is True

@pytest.mark.asyncio
async def test_kite_cmd_failure_after_max_retries():
    client = MagicMock()
    client._call_tool = AsyncMock(side_effect=ValueError("constant failure"))
    
    cmd = KiteToolCommand(client, "get_holdings", {})
    runner = CommandRunner(max_retries=2)
    
    with pytest.raises(ValueError, match="constant failure"):
        await runner.run_async(cmd, retryable=True)
    
    assert client._call_tool.await_count == 2
    assert len(runner.history) == 1
    assert runner.history[0].success is False
    assert runner.history[0].error == "constant failure"
