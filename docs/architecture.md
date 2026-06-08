# Architecture Overview

This document describes the internal architecture of Synapse — a distributed memory protocol for AI agents.

## System Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Client Layer                                     │
│                                                                               │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────────┐  │
│  │ Python   │  │ Rust     │  │ MCP      │  │ REST     │  │ gRPC         │  │
│  │ SDK      │  │ SDK      │  │ Client   │  │ Client   │  │ Client       │  │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘  └─────┬────────┘  │
│       │              │              │              │               │           │
└───────┼──────────────┼──────────────┼──────────────┼───────────────┼───────────┘
        │              │              │              │               │
        ▼              ▼              ▼              ▼               ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Gateway Layer                                    │
│                                                                               │
│  ┌───────────────────┐  ┌───────────────────┐  ┌───────────────────────┐    │
│  │   gRPC Server     │  │  HTTP/REST Gateway │  │  MCP Tool Server      │    │
│  │   (port 9090)     │  │  (port 9091)       │  │  (port 9092)          │    │
│  └─────────┬─────────┘  └─────────┬─────────┘  └──────────┬────────────┘    │
│            │                       │                        │                  │
│            └───────────────────────┼────────────────────────┘                  │
│                                    ▼                                           │
│                      ┌─────────────────────────┐                              │
│                      │    Auth & Rate Limiter   │                              │
│                      └────────────┬────────────┘                              │
└───────────────────────────────────┼───────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Core Engine                                       │
│                                                                               │
│  ┌────────────────┐  ┌────────────────┐  ┌────────────────┐                 │
│  │  Memory CRUD   │  │ Search Engine  │  │ Event Bus      │                 │
│  │  (add/get/     │  │ (semantic,     │  │ (pub/sub,      │                 │
│  │   update/del)  │  │  hybrid, kw)   │  │  subscriptions)│                 │
│  └───────┬────────┘  └───────┬────────┘  └───────┬────────┘                 │
│          │                    │                    │                           │
│          ▼                    ▼                    ▼                           │
│  ┌────────────────────────────────────────────────────────────┐              │
│  │                  Scope Resolution Engine                     │              │
│  │  (ACL check, visibility rules, namespace isolation)         │              │
│  └────────────────────────────────┬───────────────────────────┘              │
│                                   │                                           │
│          ┌────────────────────────┼────────────────────────┐                 │
│          ▼                        ▼                        ▼                  │
│  ┌──────────────┐  ┌──────────────────────┐  ┌──────────────────┐           │
│  │  Embedding   │  │  Conflict Detector   │  │   Compaction     │           │
│  │  Service     │  │  & Resolver          │  │   Engine         │           │
│  └──────────────┘  └──────────────────────┘  └──────────────────┘           │
└───────────────────────────────────┼───────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                            Storage Layer                                       │
│                                                                               │
│  ┌────────────────┐  ┌──────────────────┐  ┌────────────────────────┐       │
│  │  Record Store  │  │  Vector Index    │  │  Cluster State         │       │
│  │  (RocksDB /   │  │  (Qdrant /       │  │  (Vector Clocks,       │       │
│  │   Sled)        │  │   embedded HNSW) │  │   Merkle Trees)        │       │
│  └────────────────┘  └──────────────────┘  └────────────────────────┘       │
└───────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Distribution Layer                                   │
│                                                                               │
│  ┌────────────────────────────────────────────────────────────────────┐      │
│  │                     Gossip Protocol                                  │      │
│  │  ┌──────────┐    ┌──────────┐    ┌──────────┐                      │      │
│  │  │  Node 1  │◄──►│  Node 2  │◄──►│  Node 3  │                      │      │
│  │  │(seed)    │    │          │    │          │                      │      │
│  │  └──────────┘    └──────────┘    └──────────┘                      │      │
│  └────────────────────────────────────────────────────────────────────┘      │
└───────────────────────────────────────────────────────────────────────────────┘
```

## Memory Lifecycle

A memory record passes through several stages from ingestion to eventual archival:

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         MEMORY LIFECYCLE                                  │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│   Agent writes content                                                   │
│       │                                                                  │
│       ▼                                                                  │
│   ┌─────────────┐                                                        │
│   │   INGEST    │  Receive raw content + metadata from client            │
│   └──────┬──────┘                                                        │
│          │                                                               │
│          ▼                                                               │
│   ┌─────────────┐     ┌──────────────────────────────────────┐          │
│   │   PROCESS   │────►│ • Generate embedding vector           │          │
│   └──────┬──────┘     │ • Auto-classify kind (if not set)    │          │
│          │            │ • Deduplication check                 │          │
│          │            │ • Assign ULID                         │          │
│          │            └──────────────────────────────────────┘          │
│          ▼                                                               │
│   ┌─────────────┐     ┌──────────────────────────────────────┐          │
│   │   STORE     │────►│ • Persist to local store (RocksDB)    │          │
│   └──────┬──────┘     │ • Update vector clock                 │          │
│          │            │ • Write to WAL                         │          │
│          │            │ • Trigger async replication            │          │
│          │            └──────────────────────────────────────┘          │
│          ▼                                                               │
│   ┌─────────────┐     ┌──────────────────────────────────────┐          │
│   │   INDEX     │────►│ • Insert into vector index            │          │
│   └──────┬──────┘     │ • Update graph index (if relation)    │          │
│          │            │ • Emit MEMORY_ADDED event             │          │
│          │            └──────────────────────────────────────┘          │
│          ▼                                                               │
│   ┌─────────────┐                                                        │
│   │   SERVE     │  Available for search/retrieval                        │
│   └──────┬──────┘                                                        │
│          │                                                               │
│          │  (time passes, access patterns observed)                      │
│          ▼                                                               │
│   ┌─────────────┐     ┌──────────────────────────────────────┐          │
│   │  COMPACT    │────►│ • Merge near-duplicates               │          │
│   └──────┬──────┘     │ • Summarize old episodes              │          │
│          │            │ • Decay low-confidence memories        │          │
│          │            │ • Promote/demote tier placement        │          │
│          │            └──────────────────────────────────────┘          │
│          ▼                                                               │
│   ┌─────────────┐     ┌──────────────────────────────────────┐          │
│   │  ARCHIVE    │────►│ • Move to cold storage                │          │
│   └─────────────┘     │ • Or permanent deletion (forget)      │          │
│                        └──────────────────────────────────────┘          │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

### Memory Tiers

Synapse implements a three-tier memory hierarchy inspired by computer architecture:

| Tier | Storage | Access Latency | Capacity | Promotion Criteria |
|------|---------|---------------|----------|-------------------|
| **Hot** | In-memory cache | < 1ms | ~10K records | Accessed ≥ 5 times |
| **Warm** | Local SSD (RocksDB) | ~1-5ms | ~10M records | Default tier |
| **Cold** | Archive (S3/GCS/local) | ~50-200ms | Unlimited | Unaccessed for 180 days |

## Distributed Protocol

### Cluster Topology

Synapse uses a **peer-to-peer** topology with no designated leader. Every node can accept reads and writes.

```
                    ┌───────────────────────────┐
                    │       Client Request       │
                    └─────────────┬─────────────┘
                                  │
                   ┌──────────────┼──────────────┐
                   ▼              ▼              ▼
            ┌──────────┐  ┌──────────┐  ┌──────────┐
            │  Node A  │  │  Node B  │  │  Node C  │
            │  (seed)  │  │          │  │          │
            ├──────────┤  ├──────────┤  ├──────────┤
            │ Records  │  │ Records  │  │ Records  │
            │ Vec Index│  │ Vec Index│  │ Vec Index│
            │ Vec Clock│  │ Vec Clock│  │ Vec Clock│
            └────┬─────┘  └────┬─────┘  └────┬─────┘
                 │              │              │
                 └──────────────┼──────────────┘
                                │
                   ┌────────────┼────────────┐
                   │    Gossip Protocol       │
                   │ (Anti-Entropy Sync)      │
                   │ (Merkle Tree Diffs)      │
                   └─────────────────────────┘
```

### Replication Flow

1. **Write arrives** at any node (Node A)
2. Node A **increments its vector clock**: `{A: 4, B: 3, C: 2}` → `{A: 5, B: 3, C: 2}`
3. Node A **persists locally** + acknowledges the client
4. Node A **propagates asynchronously** to peers (B, C)
5. Peers **apply the write** and update their clocks
6. **Anti-entropy**: Periodically, nodes compare Merkle tree hashes to detect missed updates

### Vector Clock Comparison

```
Clock X dominates Clock Y iff:
  ∀ node n: X[n] ≥ Y[n]  AND  ∃ node m: X[m] > Y[m]

If neither dominates → CONCURRENT → CONFLICT
```

Example:
```
Record v1: {A:3, B:2, C:1}  —  written by Node A
Record v2: {A:2, B:4, C:1}  —  written by Node B

A:3 > A:2 but B:2 < B:4 → Neither dominates → CONFLICT!
```

## Scope Resolution

Scopes form a hierarchical namespace:

```
            ┌─────────────────────────────┐
            │         PUBLIC               │  (visible to all)
            └──────────────┬──────────────┘
                           │
            ┌──────────────▼──────────────┐
            │      org: "acme"             │  SHARED within org
            └──────────────┬──────────────┘
                           │
              ┌────────────┼────────────┐
              ▼                         ▼
    ┌─────────────────┐      ┌─────────────────┐
    │ team: "support" │      │ team: "sales"   │
    └────────┬────────┘      └────────┬────────┘
             │                        │
       ┌─────┼─────┐           ┌─────┼─────┐
       ▼           ▼           ▼           ▼
  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐
  │agent:   │ │agent:   │ │agent:   │ │agent:   │
  │billing  │ │triage   │ │outreach │ │quotes   │
  └────┬────┘ └─────────┘ └─────────┘ └─────────┘
       │
  ┌────▼────┐
  │user:    │
  │wang     │
  └────┬────┘
       │
  ┌────▼─────────┐
  │session:abc123│  (most specific, PRIVATE by default)
  └──────────────┘
```

### Visibility Rules

When a query is issued at a given scope, the following records are visible:

| Visibility | Rule | Example |
|------------|------|---------|
| `PRIVATE` | Only exact scope match | Only `team:support/agent:billing` sees its own private memories |
| `SCOPE_UP` | Visible to parent scopes | Agent memories visible to team and org |
| `SCOPE_DOWN` | Visible to child scopes | Org-level policies visible to all teams |
| `SHARED` | Visible within same org | Cross-team knowledge sharing |
| `PUBLIC` | Visible to everyone | Published APIs, documentation facts |

### Resolution Algorithm

```python
def resolve_visible_records(query_scope, all_records):
    visible = []
    for record in all_records:
        if matches(record.scope, query_scope):
            # Exact match — always visible
            visible.append(record)
        elif record.visibility == PUBLIC:
            visible.append(record)
        elif record.visibility == SHARED and same_org(record.scope, query_scope):
            visible.append(record)
        elif record.visibility == SCOPE_DOWN and is_ancestor(record.scope, query_scope):
            visible.append(record)
        elif record.visibility == SCOPE_UP and is_descendant(record.scope, query_scope):
            visible.append(record)
    return visible
```

## Conflict Resolution Pipeline

When concurrent writes are detected, Synapse processes them through a multi-stage pipeline:

```
┌─────────────────────────────────────────────────────────────────────┐
│                    CONFLICT RESOLUTION PIPELINE                       │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  ┌──────────────────────────────────────────────────────┐           │
│  │ 1. DETECTION                                          │           │
│  │                                                       │           │
│  │  Two writes are conflicting if:                       │           │
│  │  • Same record ID (explicit update conflict)  OR      │           │
│  │  • Semantic similarity > 0.85 (implicit conflict)     │           │
│  │  AND                                                  │           │
│  │  • Vector clocks are concurrent (neither dominates)   │           │
│  └────────────────────────────┬─────────────────────────┘           │
│                               │                                      │
│                               ▼                                      │
│  ┌──────────────────────────────────────────────────────┐           │
│  │ 2. POLICY CHECK (Fast Path)                           │           │
│  │                                                       │           │
│  │  ┌─ FORCE ─────────► Accept incoming, discard old    │           │
│  │  ├─ REJECT ────────► Reject incoming, keep old       │           │
│  │  ├─ LAST_WRITER ──► Compare timestamps               │           │
│  │  ├─ FIRST_WRITER ─► Keep original                    │           │
│  │  ├─ CONFIDENCE ───► Higher confidence wins           │           │
│  │  └─ AUTO_MERGE / DETECT_AND_QUEUE ──► Continue...    │           │
│  └────────────────────────────┬─────────────────────────┘           │
│                               │                                      │
│                               ▼                                      │
│  ┌──────────────────────────────────────────────────────┐           │
│  │ 3. SEMANTIC DIFF                                      │           │
│  │                                                       │           │
│  │  Analyze whether the two versions are:                │           │
│  │  • Contradictory → needs resolution                   │           │
│  │  • Complementary → can merge trivially               │           │
│  │  • Overlapping   → partial merge needed              │           │
│  └────────────────────────────┬─────────────────────────┘           │
│                               │                                      │
│                               ▼                                      │
│  ┌──────────────────────────────────────────────────────┐           │
│  │ 4. LLM ARBITER (for AUTO_MERGE)                       │           │
│  │                                                       │           │
│  │  Prompt: "Given two versions of a memory, produce    │           │
│  │  a merged version that preserves all accurate         │           │
│  │  information. Explain your reasoning."                │           │
│  │                                                       │           │
│  │  Input:  Version A + Version B + context              │           │
│  │  Output: Merged content + reasoning                   │           │
│  └────────────────────────────┬─────────────────────────┘           │
│                               │                                      │
│                               ▼                                      │
│  ┌──────────────────────────────────────────────────────┐           │
│  │ 5. STORE & NOTIFY                                     │           │
│  │                                                       │           │
│  │  • Create resolved record with merged vector clock    │           │
│  │  • Update lineage (point to parent versions)          │           │
│  │  • Emit CONFLICT_RESOLVED event                       │           │
│  │  • Replicate resolution to all nodes                  │           │
│  └──────────────────────────────────────────────────────┘           │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

## Component Details

### Embedding Service

The embedding service converts text content into vector representations:

- **Local models** (default): Runs on-server using `ort` (ONNX Runtime)
  - `all-MiniLM-L6-v2` — 384 dimensions, fast, good general purpose
  - `bge-small-en-v1.5` — 384 dimensions, optimized for retrieval
  - `nomic-embed-text-v1` — 768 dimensions, longer context window

- **Remote models**: Delegates to external APIs
  - OpenAI `text-embedding-3-small` / `text-embedding-3-large`
  - Cohere `embed-english-v3.0`

### Search Engine

Supports multiple search modes:

| Mode | Technique | Best For |
|------|-----------|----------|
| `SEMANTIC` | Vector similarity (cosine/dot) | Conceptual queries |
| `KEYWORD` | BM25 inverted index | Exact terms, names, IDs |
| `HYBRID` | RRF fusion of semantic + keyword | General purpose (default) |
| `GRAPH` | Traverse relation edges from matches | Connected knowledge |

### Event Bus

Internal pub/sub system for real-time notifications:

- Backed by a lock-free MPMC channel (Tokio broadcast)
- Subscribers receive events matching their scope filter
- Events are: `MEMORY_ADDED`, `MEMORY_UPDATED`, `MEMORY_FORGOTTEN`, `CONFLICT_DETECTED`, `CONFLICT_RESOLVED`, `MEMORY_EXPIRED`
- Delivered via gRPC server-streaming or HTTP SSE

### Compaction Engine

Runs periodically (configurable, default: every 24h):

1. **Dedup pass** — Find records with similarity > 0.95 → merge into one
2. **Summarize pass** — Cluster old episodes (> 30 days) → create summary record
3. **Decay pass** — Reduce confidence of unaccessed records (> 90 days)
4. **Tier pass** — Promote frequently-accessed to hot cache; demote cold to archive

## Data Flow: Write Path

```
Client                    Node A                    Node B
  │                         │                         │
  │── Add("deadline=Aug")──►│                         │
  │                         │── generate embedding ──►│(local)
  │                         │── check dedup ─────────►│(local)
  │                         │── persist to RocksDB ──►│(local)
  │                         │── index in vector db ──►│(local)
  │                         │── increment vclock ────►│(local)
  │◄── AddResponse ────────│                         │
  │                         │                         │
  │                         │── gossip(record) ──────►│
  │                         │                         │── apply + check conflicts
  │                         │                         │── update local clock
  │                         │◄── ack ────────────────│
```

## Data Flow: Search Path

```
Client                    Node A                    Vector Index
  │                         │                         │
  │── Search("deadline?") ─►│                         │
  │                         │── embed query ─────────►│(local)
  │                         │── scope resolution ────►│(local)
  │                         │                         │
  │                         │── vector search ───────►│
  │                         │◄── top-K candidates ───│
  │                         │                         │
  │                         │── keyword BM25 ────────►│(local)
  │                         │── RRF fusion ──────────►│(local)
  │                         │── optional LLM rerank ─►│(local/remote)
  │                         │                         │
  │◄── SearchResponse ─────│                         │
```
