# Synapse — Distributed Memory Protocol for AI Agents

> Universal memory layer that makes AI agents remember, share, and learn across distributed systems.

## What is Synapse?

Synapse is a **protocol + runtime** for AI agent memory. It solves the problem that every agent forgets everything between sessions, and multiple agents can't share knowledge without stepping on each other.

## Key Differentiators

- 🧠 **Protocol-first** — Standard interface, any framework can plug in
- 🌐 **Distributed-native** — Multi-node replication with vector clocks
- ⚡ **Conflict-aware** — Semantic merge via LLM, not just last-writer-wins
- 🔒 **Scoped & isolated** — Multi-level namespace with fine-grained ACL
- 📡 **Real-time** — Push-based event subscriptions
- 📦 **MCP compatible** — Drop-in MCP tool server for any MCP client

## Quick Start

```python
from synapse import SynapseClient

mem = SynapseClient("localhost:9090")

# Store a memory
await mem.add("用户偏好简洁专业的回答", scope={"user": "davinci"}, kind="preference")

# Recall relevant memories
results = await mem.search("用户的沟通风格偏好", scope={"user": "davinci"})

# Inject into LLM prompt
context = mem.to_context(results)
```

## Architecture

```
┌──────────────────────────────────────────────────────┐
│                   Agent Frameworks                     │
│  (LangGraph / CrewAI / AutoGen / OpenClaw / Custom)  │
└──────────────────────┬───────────────────────────────┘
                       │  gRPC / REST / MCP
                       ▼
┌──────────────────────────────────────────────────────┐
│                 Synapse Protocol Layer                 │
│  ┌─────────┐  ┌──────────┐  ┌───────────────────┐   │
│  │  Add    │  │  Search  │  │  Subscribe/Events │   │
│  │  Update │  │  List    │  │  Conflict Resolve │   │
│  │  Forget │  │  History │  │  Cluster Sync     │   │
│  └─────────┘  └──────────┘  └───────────────────┘   │
└──────────────────────┬───────────────────────────────┘
                       │
┌──────────────────────▼───────────────────────────────┐
│              Distributed Storage Engine                │
│  ┌────────┐    ┌────────────┐    ┌────────────────┐  │
│  │  Hot   │    │    Warm    │    │      Cold      │  │
│  │ Redis  │    │  Qdrant/   │    │  Object Store  │  │
│  │        │    │  Milvus    │    │  / Archive     │  │
│  └────────┘    └────────────┘    └────────────────┘  │
└──────────────────────────────────────────────────────┘
```

## Documentation

- [Protocol Specification](./PROTOCOL.md) — Full protocol spec
- Architecture (TBD)
- SDK Reference (TBD)
- Deployment Guide (TBD)

## Status

🚧 **Early stage** — Protocol spec drafted, implementation not started.

## License

Apache 2.0
