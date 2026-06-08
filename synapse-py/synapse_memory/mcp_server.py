"""MCP Tool Server for Synapse Memory Protocol.

Exposes memory operations as MCP tools:
- memory_store: Store a memory for future recall
- memory_recall: Search memories relevant to a query
- memory_forget: Remove a specific memory
- memory_update: Update an existing memory

Run as: python -m synapse_memory.mcp_server --endpoint localhost:9090
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from typing import Any, Optional

from mcp.server import Server
from mcp.server.stdio import run_server
from mcp.types import TextContent, Tool

from .client import SynapseClient
from .models import MemoryKind, Scope, Visibility
from .scope import parse_scope

logger = logging.getLogger(__name__)

# Tool definitions matching §6 MCP Compatibility Layer
TOOLS: list[Tool] = [
    Tool(
        name="memory_store",
        description="Store a memory for future recall",
        inputSchema={
            "type": "object",
            "properties": {
                "content": {
                    "type": "string",
                    "description": "The memory to store",
                },
                "kind": {
                    "type": "string",
                    "enum": ["fact", "preference", "episode", "rule", "relation", "correction", "summary"],
                    "description": "Type of memory (auto-classified if omitted)",
                },
                "tags": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Labels for organizing the memory",
                },
                "scope": {
                    "type": "string",
                    "description": "Scope path, e.g. 'org:acme/team:support/user:wang'",
                },
            },
            "required": ["content"],
        },
    ),
    Tool(
        name="memory_recall",
        description="Search memories relevant to a query",
        inputSchema={
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "What to search for",
                },
                "top_k": {
                    "type": "integer",
                    "default": 5,
                    "description": "Maximum number of results to return",
                },
                "kinds": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Filter by memory kinds",
                },
                "scope": {
                    "type": "string",
                    "description": "Scope filter path",
                },
            },
            "required": ["query"],
        },
    ),
    Tool(
        name="memory_forget",
        description="Remove a specific memory",
        inputSchema={
            "type": "object",
            "properties": {
                "id": {
                    "type": "string",
                    "description": "Memory ID to forget",
                },
                "reason": {
                    "type": "string",
                    "description": "Why this memory should be removed",
                },
            },
            "required": ["id"],
        },
    ),
    Tool(
        name="memory_update",
        description="Update an existing memory with new information",
        inputSchema={
            "type": "object",
            "properties": {
                "id": {
                    "type": "string",
                    "description": "Memory ID to update",
                },
                "content": {
                    "type": "string",
                    "description": "Updated memory content",
                },
            },
            "required": ["id", "content"],
        },
    ),
]


class SynapseMCPServer:
    """MCP server bridging MCP tool calls to the Synapse client.

    Args:
        endpoint: Synapse server endpoint
        token: Authentication token
        default_scope: Default scope for operations
    """

    def __init__(
        self,
        endpoint: str,
        token: Optional[str] = None,
        default_scope: Optional[str] = None,
    ) -> None:
        self._endpoint = endpoint
        self._token = token
        self._default_scope_path = default_scope
        self._client: Optional[SynapseClient] = None
        self._server = Server("synapse-memory")
        self._setup_handlers()

    def _setup_handlers(self) -> None:
        """Register MCP tool handlers."""

        @self._server.list_tools()
        async def list_tools() -> list[Tool]:
            return TOOLS

        @self._server.call_tool()
        async def call_tool(name: str, arguments: dict[str, Any]) -> list[TextContent]:
            try:
                result = await self._dispatch_tool(name, arguments)
                return [TextContent(type="text", text=result)]
            except Exception as e:
                logger.exception("Tool call failed: %s", name)
                return [TextContent(type="text", text=f"Error: {e}")]

    async def _get_client(self) -> SynapseClient:
        """Get or create the Synapse client."""
        if self._client is None:
            default_scope: Optional[Scope] = None
            if self._default_scope_path:
                default_scope = parse_scope(self._default_scope_path)

            self._client = SynapseClient(
                self._endpoint,
                token=self._token,
                default_scope=default_scope,
                transport="auto",
            )
            await self._client.connect()
        return self._client

    async def _dispatch_tool(self, name: str, arguments: dict[str, Any]) -> str:
        """Dispatch a tool call to the appropriate handler."""
        client = await self._get_client()

        if name == "memory_store":
            return await self._handle_store(client, arguments)
        elif name == "memory_recall":
            return await self._handle_recall(client, arguments)
        elif name == "memory_forget":
            return await self._handle_forget(client, arguments)
        elif name == "memory_update":
            return await self._handle_update(client, arguments)
        else:
            raise ValueError(f"Unknown tool: {name}")

    async def _handle_store(self, client: SynapseClient, args: dict[str, Any]) -> str:
        """Handle memory_store tool call."""
        content = args["content"]

        # Parse optional scope
        scope: Optional[Scope] = None
        if "scope" in args and args["scope"]:
            scope = parse_scope(args["scope"])

        # Parse optional kind
        kind: Optional[MemoryKind] = None
        if "kind" in args and args["kind"]:
            kind = MemoryKind(args["kind"])

        tags = args.get("tags")

        record = await client.add(
            content,
            scope=scope,
            kind=kind,
            tags=tags,
        )

        return (
            f"Memory stored successfully.\n"
            f"ID: {record.id}\n"
            f"Kind: {record.kind.value}\n"
            f"Confidence: {record.confidence}"
        )

    async def _handle_recall(self, client: SynapseClient, args: dict[str, Any]) -> str:
        """Handle memory_recall tool call."""
        query = args["query"]
        top_k = args.get("top_k", 5)

        # Parse optional scope
        scope: Optional[Scope] = None
        if "scope" in args and args["scope"]:
            scope = parse_scope(args["scope"])

        # Parse optional kinds
        kinds: Optional[list[MemoryKind]] = None
        if "kinds" in args and args["kinds"]:
            kinds = [MemoryKind(k) for k in args["kinds"]]

        results = await client.search(
            query,
            scope=scope,
            top_k=top_k,
            kinds=kinds,
        )

        if not results:
            return "No memories found matching your query."

        # Format results using to_context for LLM-friendly output
        return client.to_context(results)

    async def _handle_forget(self, client: SynapseClient, args: dict[str, Any]) -> str:
        """Handle memory_forget tool call."""
        memory_id = args["id"]
        reason = args.get("reason", "")

        deleted = await client.forget(id=memory_id)

        if deleted > 0:
            msg = f"Memory {memory_id} forgotten."
            if reason:
                msg += f" Reason: {reason}"
            return msg
        else:
            return f"Memory {memory_id} not found."

    async def _handle_update(self, client: SynapseClient, args: dict[str, Any]) -> str:
        """Handle memory_update tool call."""
        memory_id = args["id"]
        content = args["content"]

        record = await client.update(memory_id, content)

        return (
            f"Memory updated successfully.\n"
            f"ID: {record.id}\n"
            f"Version: {record.version}\n"
            f"Content: {record.content[:100]}{'...' if len(record.content) > 100 else ''}"
        )

    async def run(self) -> None:
        """Run the MCP server on stdio."""
        logger.info("Starting Synapse MCP server (endpoint: %s)", self._endpoint)
        try:
            await run_server(self._server)
        finally:
            if self._client:
                await self._client.close()


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Synapse Memory Protocol MCP Server",
        prog="synapse-mcp",
    )
    parser.add_argument(
        "--endpoint",
        default="localhost:9090",
        help="Synapse server endpoint (default: localhost:9090)",
    )
    parser.add_argument(
        "--token",
        default=None,
        help="Authentication token",
    )
    parser.add_argument(
        "--scope",
        default=None,
        help="Default scope path (e.g. 'org:acme/user:wang')",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Log level (default: INFO)",
    )
    return parser.parse_args()


def main() -> None:
    """Entry point for the MCP server."""
    args = parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        stream=sys.stderr,
    )

    server = SynapseMCPServer(
        endpoint=args.endpoint,
        token=args.token,
        default_scope=args.scope,
    )

    asyncio.run(server.run())


if __name__ == "__main__":
    main()
