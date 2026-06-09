# Getting Started with Synapse Memory

## Quick Start (Local Mode — Zero Config)

```bash
# Install
pip install synapse-memory

# Run MCP server (any MCP client can connect)
synapse-mcp
```

That's it. Memories are stored locally at `~/.synapse/memories.db`.

## MCP Client Configuration

Add to your AI client's MCP config:

```json
{
  "mcpServers": {
    "synapse": {
      "command": "synapse-mcp"
    }
  }
}
```

Works with: Claude Desktop, Cursor, Windsurf, Cline, OpenClaw, and any MCP-compatible client.

## Available Tools

Once connected, your AI agent has these tools:

| Tool | Description |
|------|-------------|
| `memory_store` | Store a new memory with optional tags, scope, and kind |
| `memory_recall` | Search memories by semantic query or keywords |
| `memory_update` | Update an existing memory's content or metadata |
| `memory_forget` | Delete a specific memory by ID |
| `memory_list` | Browse memories with filters |

## Options

```bash
# Custom database path
synapse-mcp --db ~/my-project/memories.db

# Use OpenAI embeddings for better search (requires OPENAI_API_KEY)
synapse-mcp --embedding openai

# Use local sentence-transformers (no API key, ~80MB download first time)
pip install synapse-memory[local]
synapse-mcp --embedding local

# Keyword-only search (no embedding model needed)
synapse-mcp --embedding none

# Set default scope
synapse-mcp --scope "org:acme/team:engineering"
```

## Distributed Mode

For multi-agent or multi-machine setups:

```bash
# 1. Deploy the Synapse server
docker run -p 9090:9090 ghcr.io/hydavinci/synapse:latest

# 2. Connect MCP server to remote
synapse-mcp --endpoint your-server:9090 --token YOUR_TOKEN
```

See [Distributed Guide](distributed-guide.md) for cluster setup.

## Embedding Providers

| Provider | Install | Needs API Key | Quality |
|----------|---------|---------------|---------|
| `none` | — | No | ⭐ (keyword only) |
| `local` | `pip install synapse-memory[local]` | No | ⭐⭐⭐ |
| `openai` | — | Yes (`OPENAI_API_KEY`) | ⭐⭐⭐⭐ |

## Memory Kinds

Memories can be classified:

- `fact` — Factual knowledge
- `episode` — Event or experience
- `preference` — User preference
- `skill` — Learned procedure
- `relationship` — Connection between entities

## Scopes & Visibility

Organize memories hierarchically:

```
org:acme / team:eng / user:alice / agent:assistant
```

Visibility levels:
- `private` — Only the exact scope can see it
- `scope_up` — Parent scopes can see it
- `scope_down` — Child scopes can see it
- `shared` — Same organization can see it
- `public` — Everyone can see it

## Next Steps

- [API Reference](api-reference.md)
- [Distributed Deployment](distributed-guide.md)
- [Protocol Specification](../PROTOCOL.md)
