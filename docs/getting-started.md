# Getting Started with Synapse

This guide walks you through setting up and running Synapse — the distributed memory protocol for AI agents.

## Prerequisites

| Component | Version | Required For |
|-----------|---------|--------------|
| Docker + Docker Compose | ≥ 24.0 | Container deployment |
| Rust | ≥ 1.79 | Building from source |
| Python | ≥ 3.10 | Python SDK |
| Protobuf Compiler | ≥ 3.21 | Rebuilding proto files |

## Quick Start with Docker

The fastest way to get a Synapse cluster running:

```bash
# Clone the repository
git clone https://github.com/your-org/synapse.git
cd synapse

# Start a 3-node cluster with Qdrant backend
docker compose up -d

# Verify all nodes are healthy
docker compose ps

# Check cluster status
curl http://localhost:9091/cluster/status | jq
```

The cluster exposes:
- **Node 1 gRPC:** `localhost:9090`
- **Node 1 HTTP:** `localhost:9091`
- **Node 2 gRPC:** `localhost:9092`
- **Node 3 gRPC:** `localhost:9094`
- **Qdrant:** `localhost:6333` (REST) / `localhost:6334` (gRPC)

### Single Node (Development)

For local development, run a single node with the embedded vector store:

```bash
docker run -d --name synapse-dev \
  -p 9090:9090 \
  -p 9091:9091 \
  -e SYNAPSE_NODE_ID=dev-1 \
  -e SYNAPSE_LOG_LEVEL=debug \
  synapse-server
```

## Building from Source

### 1. Install Dependencies

```bash
# Ubuntu/Debian
sudo apt-get install -y protobuf-compiler libprotobuf-dev cmake pkg-config libssl-dev

# macOS
brew install protobuf cmake openssl

# Arch
sudo pacman -S protobuf cmake openssl
```

### 2. Build the Server

```bash
# Clone
git clone https://github.com/your-org/synapse.git
cd synapse

# Build release binary
cargo build --release --bin synapse-server

# The binary is at: target/release/synapse-server
```

### 3. Run the Server

```bash
# Using default config
./target/release/synapse-server --config config/default.toml

# Override settings via environment
SYNAPSE_NODE_ID=local-1 \
SYNAPSE_LOG_LEVEL=debug \
./target/release/synapse-server

# Override via CLI flags
./target/release/synapse-server \
  --server.port=9090 \
  --cluster.peers="peer1:9090,peer2:9090" \
  --cluster.consistency.default=strong
```

## Python SDK

### Installation

```bash
pip install synapse-memory
```

### Quick Start

```python
import asyncio
from synapse import SynapseClient, Scope, MemoryKind, SearchMode

async def main():
    # Connect to a Synapse node
    client = SynapseClient(
        endpoint="localhost:9090",
        default_scope=Scope(org="my-org", agent="my-agent"),
    )

    # Store a memory
    record = await client.add(
        content="The project deadline has been moved to August 15th, 2026.",
        kind=MemoryKind.FACT,
        tags=["project", "deadline"],
        confidence=0.95,
    )
    print(f"Stored memory: {record.id}")

    # Search for relevant memories
    results = await client.search(
        query="When is the project due?",
        top_k=5,
        mode=SearchMode.HYBRID,
    )
    for r in results:
        print(f"  [{r.score:.2f}] {r.record.content}")

    # Subscribe to real-time updates
    async for event in client.subscribe():
        print(f"Event: {event.type} — {event.record.content[:50]}")

asyncio.run(main())
```

## MCP Integration

Synapse can be used as an MCP tool server, allowing any MCP-compatible agent to store and recall memories.

### Configuration

Add Synapse to your MCP client configuration:

```json
{
  "mcpServers": {
    "synapse": {
      "transport": "sse",
      "url": "http://localhost:9092/mcp",
      "env": {
        "SYNAPSE_SCOPE": "org:my-org/agent:my-agent"
      }
    }
  }
}
```

### Using with Claude Desktop

In your Claude Desktop MCP config (`claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "synapse-memory": {
      "command": "synapse-mcp",
      "args": ["--endpoint", "localhost:9090"],
      "env": {
        "SYNAPSE_TOKEN": "your-api-token",
        "SYNAPSE_DEFAULT_SCOPE": "user:claude-user"
      }
    }
  }
}
```

### Available MCP Tools

Once connected, agents can use these tools:

| Tool | Description |
|------|-------------|
| `memory_store` | Store a new memory with optional kind, tags, and scope |
| `memory_recall` | Semantic search across stored memories |
| `memory_update` | Update an existing memory's content |
| `memory_forget` | Remove a memory by ID |

### Example MCP Tool Call

```json
{
  "name": "memory_store",
  "arguments": {
    "content": "User prefers concise responses without code blocks unless asked.",
    "kind": "preference",
    "tags": ["user-preference", "response-style"],
    "scope": "user:davinci"
  }
}
```

## Configuration Reference

Synapse uses a layered configuration system:

```
CLI flags → Environment variables → Config file → Defaults
```

See `config/default.toml` for all options with documentation. Key environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `SYNAPSE_NODE_ID` | auto-generated | Unique node identifier |
| `SYNAPSE_HOST` | `0.0.0.0` | Bind address |
| `SYNAPSE_GRPC_PORT` | `9090` | gRPC port |
| `SYNAPSE_HTTP_PORT` | `9091` | HTTP/REST port |
| `SYNAPSE_CLUSTER_PEERS` | (empty) | Comma-separated peer addresses |
| `SYNAPSE_CONSISTENCY` | `eventual` | Default consistency level |
| `SYNAPSE_LOG_LEVEL` | `info` | Log level |
| `SYNAPSE_DATA_DIR` | `/opt/synapse/data` | Data storage directory |

## Next Steps

- **[Architecture Overview](./architecture.md)** — Understand how Synapse works internally
- **[API Reference](./api-reference.md)** — Complete API documentation
- **[Examples](../examples/)** — More code examples
- **[PROTOCOL.md](../PROTOCOL.md)** — Full protocol specification
