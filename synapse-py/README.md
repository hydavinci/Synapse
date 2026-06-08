# synapse-memory

Persistent memory for AI agents. Works as an MCP server with any AI client (Claude, Cursor, Windsurf, Cline, OpenClaw, etc.) or as a Python library.

## Install

```bash
pip install synapse-memory
```

## Use as MCP Server

```bash
synapse-mcp
```

Add to any MCP client config:
```json
{"mcpServers": {"synapse": {"command": "synapse-mcp"}}}
```

## Use as Python Library

```python
from synapse_memory import LocalStore, MemoryKind

store = LocalStore()
store.add("User prefers dark mode", kind=MemoryKind.PREFERENCE)
results = store.search("user preferences")
```

## Embedding Options

```bash
# Auto-detect (recommended)
synapse-mcp

# With OpenAI embeddings
OPENAI_API_KEY=sk-xxx synapse-mcp

# With local embeddings (free, no API key)
pip install synapse-memory[local]
synapse-mcp

# Keyword search only (no numpy/ML needed)
synapse-mcp --embedding none
```

## Distributed Mode

```bash
synapse-mcp --endpoint your-server:9090
```

See [full documentation](https://github.com/hydavinci/Synapse) for details.
