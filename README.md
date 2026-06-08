# 🧠 Synapse

**Persistent memory for AI agents — local or distributed.**

Give any AI agent long-term memory with one command. Works with Claude, GPT, Cursor, Windsurf, Cline, OpenClaw, and any MCP-compatible client.

```bash
pip install synapse-memory
```

That's it. Your AI now has persistent memory.

---

## Quick Start

### As MCP Server (recommended)

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

Your AI gets 4 tools:
- **`memory_store`** — Save a memory (facts, preferences, episodes, rules)
- **`memory_recall`** — Search for relevant memories by natural language
- **`memory_update`** — Update existing memories (versioned)
- **`memory_forget`** — Remove memories

### Client-Specific Setup

<details>
<summary><b>Claude Desktop</b></summary>

Edit `~/Library/Application Support/Claude/claude_desktop_config.json`:
```json
{
  "mcpServers": {
    "synapse": {
      "command": "synapse-mcp"
    }
  }
}
```
</details>

<details>
<summary><b>Cursor</b></summary>

Settings → Features → MCP Servers → Add:
```json
{
  "synapse": {
    "command": "synapse-mcp"
  }
}
```
</details>

<details>
<summary><b>Windsurf</b></summary>

Edit `~/.codeium/windsurf/mcp_config.json`:
```json
{
  "mcpServers": {
    "synapse": {
      "command": "synapse-mcp"
    }
  }
}
```
</details>

<details>
<summary><b>Cline / Continue</b></summary>

Edit MCP settings:
```json
{
  "mcpServers": {
    "synapse": {
      "command": "synapse-mcp"
    }
  }
}
```
</details>

<details>
<summary><b>OpenClaw</b></summary>

```bash
openclaw mcp set synapse '{"command":"synapse-mcp"}'
```
</details>

---

## How It Works

```
┌──────────────────┐     MCP (stdio)     ┌──────────────────┐
│   AI Client      │◄───────────────────►│   synapse-mcp    │
│ (Claude/Cursor/  │                      │                  │
│  Windsurf/etc)   │                      │  ┌────────────┐  │
└──────────────────┘                      │  │  SQLite +  │  │
                                          │  │  Vectors   │  │
                                          │  └────────────┘  │
                                          └──────────────────┘
                                                   │
                                          (optional remote)
                                                   │
                                                   ▼
                                          ┌──────────────────┐
                                          │  Synapse Server  │
                                          │  (distributed)   │
                                          └──────────────────┘
```

**Local mode (default):** Memories stored in `~/.synapse/memories.db`. Zero external dependencies. Works offline.

**Remote mode:** Connect to a Synapse server for multi-agent memory sharing, conflict resolution, and distributed sync.

---

## Features

| Feature | Local | Remote |
|---------|:-----:|:------:|
| Persistent memory across sessions | ✅ | ✅ |
| Semantic search (vector similarity) | ✅ | ✅ |
| Keyword search fallback | ✅ | ✅ |
| Memory versioning | ✅ | ✅ |
| Scope-based namespacing | ✅ | ✅ |
| Multiple memory types | ✅ | ✅ |
| Tag-based filtering | ✅ | ✅ |
| Multi-agent sharing | — | ✅ |
| Conflict detection & resolution | — | ✅ |
| Distributed sync (vector clocks) | — | ✅ |
| Real-time event subscription | — | ✅ |

---

## Embedding Providers

Synapse auto-detects the best embedding provider:

| Priority | Provider | Setup | Quality |
|----------|----------|-------|---------|
| 1 | OpenAI | Set `OPENAI_API_KEY` | ★★★★★ |
| 2 | Local (sentence-transformers) | `pip install synapse-memory[local]` | ★★★★☆ |
| 3 | Keyword search | Nothing needed | ★★★☆☆ |

```bash
# Best quality (needs API key)
OPENAI_API_KEY=sk-xxx synapse-mcp

# Free, local, no API key (first run downloads ~80MB model)
pip install synapse-memory[local]
synapse-mcp

# No embeddings, keyword search only
synapse-mcp --embedding none
```

---

## Advanced: Remote / Distributed Mode

For multi-agent setups, shared memory, and conflict resolution:

```bash
# Start the Synapse server (Rust, high-performance)
docker compose up -d

# Connect MCP to remote server
synapse-mcp --endpoint your-server:9090
```

Or in MCP config:
```json
{
  "mcpServers": {
    "synapse": {
      "command": "synapse-mcp",
      "args": ["--endpoint", "your-server:9090"]
    }
  }
}
```

### Distributed Features

- **Multi-agent memory sharing**: Agents in the same scope can see each other's memories
- **Conflict resolution**: Vector clock detection + configurable strategies (LLM merge, last-writer-wins, etc.)
- **Scope visibility**: Fine-grained access control (`org:acme/team:eng/user:bob`)
- **Real-time sync**: Subscribe to memory change events across agents

---

## Python SDK

Use Synapse directly in your code:

```python
from synapse_memory import LocalStore, MemoryKind, Scope

# Local usage (zero config)
store = LocalStore()

# Store memories
store.add("User prefers dark mode", kind=MemoryKind.PREFERENCE, tags=["ui"])
store.add("Project deadline is July 15", kind=MemoryKind.FACT, tags=["project"])

# Search
results = store.search("what are the user's preferences?")
for r in results:
    print(f"[{r.score:.2f}] {r.record.content}")

# Scoped access for multi-agent
scope = Scope(org="acme", team="support", agent="billing-bot")
store.add("Customer VIP level: Gold", scope=scope, kind=MemoryKind.FACT)
```

For distributed mode:
```python
from synapse_memory import SynapseClient

async with SynapseClient("your-server:9090") as client:
    await client.add("Important fact", kind=MemoryKind.FACT)
    results = await client.search("important")
```

---

## CLI Options

```
synapse-mcp [OPTIONS]

Options:
  --endpoint HOST:PORT   Remote Synapse server (omit for local mode)
  --token TOKEN          Auth token for remote mode
  --db PATH              SQLite path (default: ~/.synapse/memories.db)
  --scope SCOPE          Default scope (e.g. 'user:alice')
  --embedding PROVIDER   auto|openai|local|none (default: auto)
  --log-level LEVEL      DEBUG|INFO|WARNING|ERROR (default: INFO)
```

---

## Architecture

Synapse is designed as a **protocol-first** system:

- **Protocol layer** (proto/): gRPC service definitions, language-agnostic
- **Rust server** (synapse-server/): High-performance distributed backend
- **Python SDK** (synapse-py/): Client library + MCP server + local store

The Python package works standalone (local mode) or as a client to the Rust server (remote mode). You can start local and upgrade to distributed when you need it.

---

## Project Structure

```
├── proto/                    # Protocol Buffer definitions
│   └── synapse/v1/
│       ├── memory.proto      # Memory service
│       ├── conflict.proto    # Conflict resolution
│       └── cluster.proto     # Cluster management
├── synapse-server/           # Rust server (distributed mode)
│   └── src/
│       ├── main.rs
│       ├── api/grpc.rs       # gRPC implementation
│       ├── storage/          # Storage backends
│       ├── search/           # Vector search
│       ├── conflict/         # Conflict detection & resolution
│       ├── scope/            # Scope visibility rules
│       └── cluster/          # Distributed node management
├── synapse-py/               # Python SDK + MCP server
│   └── synapse_memory/
│       ├── mcp_server.py     # MCP tool server (entry point)
│       ├── local_store.py    # SQLite + vector search
│       ├── embeddings.py     # Embedding providers
│       ├── client.py         # Remote async client
│       ├── models.py         # Data models
│       └── scope.py          # Scope parsing
├── examples/                 # Usage examples
├── docs/                     # Documentation
├── Dockerfile                # Server container
└── docker-compose.yml        # Multi-node cluster
```

---

## License

Apache 2.0
