"""
tests/test_agent_server.py
──────────────────────────
Unit tests for the agent_server HTTP API endpoints (specifically chat history).
"""
import json
import unittest
from unittest.mock import patch, MagicMock
from io import BytesIO

# Import the handler from the ui package
from src.ui.agent_server import StudioRequestHandler


class MockRequest:
    def __init__(self, rfile_bytes=b""):
        self.rfile = BytesIO(rfile_bytes)
        self.wfile = BytesIO()

    def makefile(self, *args, **kwargs):
        return self.rfile

    def sendall(self, bytes_data):
        self.wfile.write(bytes_data)

    def send(self, bytes_data):
        self.wfile.write(bytes_data)
        return len(bytes_data)


class TestAgentServerChatHistory(unittest.TestCase):
    @patch("src.ui.agent_server._get_chat_checkpointer")
    def test_api_chat_threads_success(self, mock_get_checkpointer):
        # Set up mock checkpoints returned by checkpointer.list(None)
        mock_checkpointer = MagicMock()
        mock_get_checkpointer.return_value = mock_checkpointer

        # Mock the checkpoint tuples
        from langchain_core.messages import HumanMessage
        mock_tuple = MagicMock()
        mock_tuple.config = {"configurable": {"thread_id": "thread-abc"}}
        mock_tuple.checkpoint = {
            "ts": "2026-07-09T10:15:00",
            "channel_values": {
                "messages": [HumanMessage(content="Explain gold trends")]
            }
        }
        mock_checkpointer.list.return_value = [mock_tuple]

        # Prepare request handler
        req = MockRequest()
        client_address = ("127.0.0.1", 12345)
        server = MagicMock()

        # Instantiate handler with mocks
        with patch.object(StudioRequestHandler, "send_response") as mock_send_resp, \
             patch.object(StudioRequestHandler, "send_header") as mock_send_header, \
             patch.object(StudioRequestHandler, "end_headers") as mock_end_headers:
            
            handler = StudioRequestHandler(req, client_address, server)
            handler.path = "/api/chat/threads"
            handler.do_GET()

            # Verify response was successful
            mock_send_resp.assert_called_with(200)
            mock_send_header.assert_any_call("Content-Type", "application/json")

            # Parse the output JSON
            response_bytes = req.wfile.getvalue()
            response_data = json.loads(response_bytes.decode("utf-8"))

            self.assertIn("threads", response_data)
            self.assertEqual(len(response_data["threads"]), 1)
            self.assertEqual(response_data["threads"][0]["thread_id"], "thread-abc")
            self.assertEqual(response_data["threads"][0]["prompt"], "Explain gold trends")

    @patch("src.ui.agent_server._get_chat_checkpointer")
    def test_api_chat_messages_success(self, mock_get_checkpointer):
        # Set up mock checkpointer get() returned value
        mock_checkpointer = MagicMock()
        mock_get_checkpointer.return_value = mock_checkpointer

        from langchain_core.messages import HumanMessage, AIMessage
        mock_checkpoint_tuple = MagicMock()
        mock_checkpoint_tuple.checkpoint = {
            "channel_values": {
                "messages": [
                    HumanMessage(content="Hi"),
                    AIMessage(content="Hello! How can I help you today?")
                ]
            }
        }
        mock_checkpointer.get.return_value = mock_checkpoint_tuple

        # Prepare request handler
        req = MockRequest()
        client_address = ("127.0.0.1", 12345)
        server = MagicMock()

        # Instantiate handler
        with patch.object(StudioRequestHandler, "send_response") as mock_send_resp, \
             patch.object(StudioRequestHandler, "send_header") as mock_send_header, \
             patch.object(StudioRequestHandler, "end_headers") as mock_end_headers:
            
            handler = StudioRequestHandler(req, client_address, server)
            handler.path = "/api/chat/messages?thread_id=thread-abc"
            handler.do_GET()

            # Verify response
            mock_send_resp.assert_called_with(200)

            # Parse the output JSON
            response_bytes = req.wfile.getvalue()
            response_data = json.loads(response_bytes.decode("utf-8"))

            self.assertEqual(response_data["status"], "success")
            self.assertEqual(response_data["thread_id"], "thread-abc")
            self.assertEqual(len(response_data["messages"]), 2)
            self.assertEqual(response_data["messages"][0]["role"], "user")
            self.assertEqual(response_data["messages"][0]["content"], "Hi")
            self.assertEqual(response_data["messages"][1]["role"], "agent")
            self.assertEqual(response_data["messages"][1]["content"], "Hello! How can I help you today?")


if __name__ == "__main__":
    unittest.main()
