"""Example: Using Synapse as an MCP (Model Context Protocol) server.

This shows how to configure Synapse as an MCP tool server so that
any MCP-compatible AI client (Claude Desktop, etc.) can use memory tools.

Setup:
  1. Start the Synapse server: cargo run (or via Docker)
  2. Run the MCP server: python -m synapse_memory.mcp_server --endpoint localhost:9090
  3. Configure your MCP client to connect to this server

For Claude Desktop, add to claude_desktop_config.json:
{
  "mcpServers": {
    "synapse": {
      "command": "python",
      "args": ["-m", "synapse_memory.mcp_server", "--endpoint", "localhost:9090"]
    }
  }
}
"""

import asyncio
from synapse_memory import SynapseClient, Scope, MemoryKind


async def simulate_mcp_tools():
    """Simulate what the MCP tools do internally."""

    client = SynapseClient(endpoint="localhost:9090", transport="rest")
    scope = Scope(user="davinci")

    # === memory_store tool ===
    print("=== Tool: memory_store ===")
    record = await client.add(
        content="Davinci prefers concise, professional answers in Chinese",
        scope=scope,
        kind=MemoryKind.PREFERENCE,
        tags=["language", "style"],
    )
    print(f"Stored memory: {record.id}")

    # === memory_recall tool ===
    print("\n=== Tool: memory_recall ===")
    results = await client.search(
        query="What language does Davinci prefer?",
        scope=scope,
        top_k=3,
    )
    context = client.to_context(results)
    print(f"Recalled:\n{context}")

    # === memory_update tool ===
    print("\n=== Tool: memory_update ===")
    updated = await client.update(
        id=record.id,
        content="Davinci prefers concise, professional answers in Chinese. No filler words.",
    )
    print(f"Updated: {updated.content}")

    # === memory_forget tool ===
    print("\n=== Tool: memory_forget ===")
    count = await client.forget(id=record.id)
    print(f"Forgotten: {count} record(s)")


async def main():
    print("Synapse MCP Integration Example")
    print("=" * 40)
    print()
    print("In production, the MCP server exposes these as tools:")
    print("  - memory_store: Store a new memory")
    print("  - memory_recall: Search for relevant memories")
    print("  - memory_update: Update an existing memory")
    print("  - memory_forget: Remove a memory")
    print()
    print("Below we simulate what these tools do internally:")
    print()

    await simulate_mcp_tools()


if __name__ == "__main__":
    asyncio.run(main())
