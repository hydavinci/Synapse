# Synapse Memory Protocol Specification

**Version:** 0.1.0-draft  
**Date:** 2026-06-08  
**Status:** Draft  
**Author:** Davinci

---

## 1. Overview

Synapse is a **distributed memory protocol for AI agents**. It defines a standard interface for agents to store, retrieve, share, and synchronize memories across distributed systems — regardless of the agent framework used.

### Design Goals

1. **Universal** — Framework-agnostic; works with LangGraph, CrewAI, AutoGen, OpenClaw, or bare LLM calls
2. **Distributed-first** — Multi-node, eventually consistent by default, strong consistency optional
3. **Conflict-aware** — Semantic conflict detection and resolution, not just last-writer-wins
4. **Scoped** — Fine-grained access control with multi-level namespace isolation
5. **Real-time** — Push-based event subscriptions, not just request-response
6. **Layered** — Hot/warm/cold memory tiers with automatic promotion/demotion

### Non-Goals

- Not a full Agent framework (no planning, no tool-calling, no orchestration)
- Not a vector database (uses existing ones as backends)
- Not a chat history store (memories are distilled knowledge, not raw transcripts)

---

## 2. Core Concepts

### 2.1 Memory Record

A Memory Record is the atomic unit of knowledge stored in Synapse.

```
MemoryRecord {
  id:          string          // Globally unique (ULID)
  content:     string          // The actual memory text
  embedding:   float[]         // Vector representation (auto-generated)
  
  // Metadata
  scope:       Scope           // Ownership & visibility
  tags:        string[]        // User-defined labels
  kind:        MemoryKind      // Classification
  confidence:  float (0-1)     // How certain is this memory
  source:      Source          // Where it came from
  
  // Temporal
  created_at:  timestamp       // When first stored
  updated_at:  timestamp       // Last modification
  accessed_at: timestamp       // Last retrieval
  expires_at:  timestamp?      // Optional TTL
  
  // Versioning
  version:     uint64          // Monotonic version counter
  vector_clock: VectorClock    // For distributed conflict detection
  lineage:     string[]        // Parent record IDs (if merged/derived)
}
```

### 2.2 Memory Kind

Classifies the type of knowledge stored:

| Kind | Description | Example |
|------|-------------|---------|
| `fact` | Objective knowledge | "项目截止日期是7月15日" |
| `preference` | User/agent preference | "用户偏好简洁回复" |
| `episode` | Event that happened | "2026-06-01 与客户开了需求会议" |
| `rule` | Operational rule/constraint | "超过5万的订单需要VP审批" |
| `relation` | Entity relationship | "Alice是项目Alpha的负责人" |
| `correction` | Error correction | "之前说截止6月30日是错的，已延期" |
| `summary` | Compressed/aggregated knowledge | "过去一周共处理了12个客户工单" |

### 2.3 Scope

Scope defines **ownership** and **visibility** of a memory record.

```
Scope {
  // Ownership (who created it)
  org:       string?    // Organization level
  team:      string?    // Team level
  agent:     string?    // Specific agent
  user:      string?    // Specific user
  session:   string?    // Specific session/run
  
  // Visibility (who can access it)
  visibility: Visibility
}

enum Visibility {
  PRIVATE      // Only the owner scope can access
  SCOPE_UP     // Visible to parent scopes (agent→team→org)
  SCOPE_DOWN   // Visible to child scopes (org→team→agent)
  SHARED       // Visible to all within the same org
  PUBLIC       // Visible to all (cross-org)
}
```

**Scope Resolution Rules:**
- A query at scope `{org: "acme", team: "support"}` sees:
  - All records with matching scope
  - All records with `visibility: SCOPE_DOWN` from parent `{org: "acme"}`
  - All records with `visibility: SCOPE_UP` from children (e.g., specific agents in support team)
  - All `SHARED` records within org "acme"
  - All `PUBLIC` records

### 2.4 Conflict

A Conflict arises when two or more agents write to the same logical memory concurrently.

```
Conflict {
  id:           string
  records:      MemoryRecord[]   // The conflicting versions
  detected_at:  timestamp
  status:       ConflictStatus
  resolution:   Resolution?
}

enum ConflictStatus {
  PENDING       // Detected, awaiting resolution
  AUTO_RESOLVED // Resolved by policy/LLM
  MANUAL        // Requires human intervention
  DISCARDED     // Conflict deemed irrelevant
}

Resolution {
  strategy:     ResolutionStrategy
  result:       MemoryRecord      // The merged/winning record
  reasoning:    string?           // Why this resolution was chosen
  resolved_by:  string            // "system", "llm", or agent/user id
  resolved_at:  timestamp
}

enum ResolutionStrategy {
  LAST_WRITER_WINS    // Simple timestamp-based
  FIRST_WRITER_WINS   // Preserve original
  LLM_MERGE           // Use LLM to semantically merge
  KEEP_BOTH           // Store as separate records
  MANUAL              // Escalate to human
  CONFIDENCE_WINS     // Higher confidence score wins
  CUSTOM              // User-defined resolver function
}
```

---

## 3. Data Model — Wire Format

All messages use Protocol Buffers for gRPC transport, with JSON mapping for REST/HTTP.

### 3.1 Protobuf Schema (core types)

```protobuf
syntax = "proto3";
package synapse.v1;

import "google/protobuf/timestamp.proto";

// === Core Types ===

message MemoryRecord {
  string id = 1;
  string content = 2;
  repeated float embedding = 3;
  
  Scope scope = 4;
  repeated string tags = 5;
  MemoryKind kind = 6;
  float confidence = 7;
  Source source = 8;
  
  google.protobuf.Timestamp created_at = 10;
  google.protobuf.Timestamp updated_at = 11;
  google.protobuf.Timestamp accessed_at = 12;
  google.protobuf.Timestamp expires_at = 13;
  
  uint64 version = 20;
  VectorClock vector_clock = 21;
  repeated string lineage = 22;
}

message Scope {
  string org = 1;
  string team = 2;
  string agent = 3;
  string user = 4;
  string session = 5;
  Visibility visibility = 6;
}

enum Visibility {
  VISIBILITY_UNSPECIFIED = 0;
  PRIVATE = 1;
  SCOPE_UP = 2;
  SCOPE_DOWN = 3;
  SHARED = 4;
  PUBLIC = 5;
}

enum MemoryKind {
  KIND_UNSPECIFIED = 0;
  FACT = 1;
  PREFERENCE = 2;
  EPISODE = 3;
  RULE = 4;
  RELATION = 5;
  CORRECTION = 6;
  SUMMARY = 7;
}

message Source {
  string agent_id = 1;        // Which agent created this
  string session_id = 2;      // In which session
  string input_hash = 3;      // Hash of the input that triggered this memory
  string tool_call_id = 4;    // If memory came from a tool result
}

message VectorClock {
  map<string, uint64> clock = 1;  // node_id → logical timestamp
}
```

---

## 4. Service API

### 4.1 Memory Service (Core CRUD + Search)

```protobuf
service MemoryService {
  // Write
  rpc Add(AddRequest) returns (AddResponse);
  rpc Update(UpdateRequest) returns (UpdateResponse);
  rpc Forget(ForgetRequest) returns (ForgetResponse);
  
  // Read
  rpc Get(GetRequest) returns (GetResponse);
  rpc Search(SearchRequest) returns (SearchResponse);
  rpc List(ListRequest) returns (ListResponse);
  rpc History(HistoryRequest) returns (HistoryResponse);
  
  // Real-time
  rpc Subscribe(SubscribeRequest) returns (stream MemoryEvent);
  
  // Bulk
  rpc BatchAdd(BatchAddRequest) returns (BatchAddResponse);
  rpc Export(ExportRequest) returns (stream MemoryRecord);
  rpc Import(stream MemoryRecord) returns (ImportResponse);
}
```

#### Add

```protobuf
message AddRequest {
  string content = 1;               // Required: the memory text
  Scope scope = 2;                  // Required: ownership scope
  repeated string tags = 3;         // Optional
  MemoryKind kind = 4;              // Optional (auto-classified if unset)
  float confidence = 5;             // Optional (default: 1.0)
  google.protobuf.Timestamp expires_at = 6;  // Optional TTL
  
  // Deduplication
  bool deduplicate = 10;            // Check for near-duplicates before inserting
  float dedup_threshold = 11;       // Similarity threshold (default: 0.92)
  
  // Conflict behavior
  ConflictPolicy on_conflict = 12;  // What to do if conflicts detected
}

message AddResponse {
  MemoryRecord record = 1;          // The stored record
  bool deduplicated = 2;            // True if merged with existing
  string merged_with = 3;           // ID of the record it was merged into
}

enum ConflictPolicy {
  CONFLICT_POLICY_UNSPECIFIED = 0;
  DETECT_AND_QUEUE = 1;    // Store + flag conflict for resolution
  AUTO_MERGE = 2;          // Use LLM to merge immediately
  REJECT = 3;              // Reject write if conflict detected
  FORCE = 4;               // Overwrite regardless
}
```

#### Search

```protobuf
message SearchRequest {
  string query = 1;                 // Semantic search query
  Scope scope = 2;                  // Scope filter
  uint32 top_k = 3;                 // Max results (default: 10)
  float min_score = 4;              // Minimum similarity score
  
  // Filters
  repeated MemoryKind kinds = 5;    // Filter by kind
  repeated string tags = 6;         // Filter by tags (AND)
  TimeRange time_range = 7;         // Filter by time
  string agent_id = 8;             // Filter by source agent
  
  // Options
  bool include_expired = 10;        // Include expired records
  bool rerank = 11;                 // Apply LLM reranking
  SearchMode mode = 12;
}

enum SearchMode {
  SEMANTIC = 0;        // Vector similarity (default)
  KEYWORD = 1;         // BM25 keyword search
  HYBRID = 2;          // Semantic + keyword fusion
  GRAPH = 3;           // Graph traversal from matched nodes
}

message SearchResponse {
  repeated SearchResult results = 1;
  uint32 total = 2;
}

message SearchResult {
  MemoryRecord record = 1;
  float score = 2;                  // Relevance score
  string explanation = 3;           // Optional: why this matched
}
```

#### Subscribe (Real-time Events)

```protobuf
message SubscribeRequest {
  Scope scope = 1;                  // Which scope to watch
  repeated EventType event_types = 2; // Filter event types
}

enum EventType {
  EVENT_ALL = 0;
  MEMORY_ADDED = 1;
  MEMORY_UPDATED = 2;
  MEMORY_FORGOTTEN = 3;
  CONFLICT_DETECTED = 4;
  CONFLICT_RESOLVED = 5;
  MEMORY_EXPIRED = 6;
}

message MemoryEvent {
  EventType type = 1;
  MemoryRecord record = 2;
  Conflict conflict = 3;            // Only for conflict events
  google.protobuf.Timestamp timestamp = 4;
  string source_node = 5;           // Which cluster node emitted this
}
```

### 4.2 Conflict Service

```protobuf
service ConflictService {
  rpc ListConflicts(ListConflictsRequest) returns (ListConflictsResponse);
  rpc ResolveConflict(ResolveConflictRequest) returns (ResolveConflictResponse);
  rpc SetPolicy(SetPolicyRequest) returns (SetPolicyResponse);
}

message ResolveConflictRequest {
  string conflict_id = 1;
  ResolutionStrategy strategy = 2;  // Which strategy to apply
  string manual_content = 3;        // For MANUAL strategy: the human-provided resolution
}
```

### 4.3 Cluster Service (Distributed Operations)

```protobuf
service ClusterService {
  rpc Join(JoinRequest) returns (JoinResponse);
  rpc Leave(LeaveRequest) returns (LeaveResponse);
  rpc Status(StatusRequest) returns (ClusterStatus);
  rpc Sync(SyncRequest) returns (stream SyncEvent);
}

message ClusterStatus {
  repeated Node nodes = 1;
  ConsistencyLevel consistency = 2;
  uint64 total_records = 3;
  uint64 pending_syncs = 4;
  uint64 active_conflicts = 5;
}

message Node {
  string id = 1;
  string address = 2;
  NodeState state = 3;
  google.protobuf.Timestamp last_heartbeat = 4;
  VectorClock clock = 5;
}

enum ConsistencyLevel {
  EVENTUAL = 0;          // Async replication, read-local
  BOUNDED_STALENESS = 1; // Reads guaranteed fresh within N ms
  STRONG = 2;            // Linearizable (higher latency)
}
```

---

## 5. Distributed Protocol

### 5.1 Replication

- Default: **async replication** with vector clocks for causality tracking
- Each node maintains a local replica; writes are accepted locally and propagated
- Anti-entropy protocol (Merkle tree gossip) ensures eventual convergence

### 5.2 Conflict Detection

Two writes conflict if:
1. They target the same logical memory (detected via semantic similarity > threshold OR same explicit ID)
2. Their vector clocks are concurrent (neither dominates the other)

```
Node A writes: "截止日期是6月30日"  clock: {A:3, B:2}
Node B writes: "截止日期是7月15日"  clock: {A:2, B:4}

→ Concurrent! Neither clock dominates → CONFLICT
```

### 5.3 Conflict Resolution Pipeline

```
Conflict Detected
       │
       ▼
┌──────────────────┐
│  Check Policy    │ → FORCE/REJECT/LAST_WRITER_WINS (fast path)
└────────┬─────────┘
         │ (policy = AUTO_MERGE or DETECT_AND_QUEUE)
         ▼
┌──────────────────┐
│  Semantic Diff   │ → Are they actually contradictory?
└────────┬─────────┘   (might be complementary → merge trivially)
         │
         ▼
┌──────────────────┐
│  LLM Arbiter     │ → Merge with reasoning
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│  Store + Notify  │ → Emit CONFLICT_RESOLVED event
└──────────────────┘
```

### 5.4 Consistency Levels

| Level | Guarantee | Latency | Use Case |
|-------|-----------|---------|----------|
| `EVENTUAL` | Reads may be stale, writes always accepted | ~1ms | High-throughput, latency-sensitive |
| `BOUNDED_STALENESS` | Reads guaranteed fresh within configured window | ~10-50ms | Balance of freshness and performance |
| `STRONG` | Linearizable, all nodes agree | ~50-200ms | Critical decisions, financial data |

Configurable per-scope: e.g., `preference` memories use EVENTUAL, `rule` memories use STRONG.

---

## 6. MCP Compatibility Layer

Synapse exposes itself as an MCP Tool Server for plug-and-play integration:

### Tools Exposed

```json
{
  "tools": [
    {
      "name": "memory_store",
      "description": "Store a memory for future recall",
      "inputSchema": {
        "type": "object",
        "properties": {
          "content": {"type": "string", "description": "The memory to store"},
          "kind": {"type": "string", "enum": ["fact","preference","episode","rule","relation","correction","summary"]},
          "tags": {"type": "array", "items": {"type": "string"}},
          "scope": {"type": "string", "description": "Scope path, e.g. 'org:acme/team:support/user:wang'"}
        },
        "required": ["content"]
      }
    },
    {
      "name": "memory_recall",
      "description": "Search memories relevant to a query",
      "inputSchema": {
        "type": "object",
        "properties": {
          "query": {"type": "string", "description": "What to search for"},
          "top_k": {"type": "integer", "default": 5},
          "kinds": {"type": "array", "items": {"type": "string"}},
          "scope": {"type": "string"}
        },
        "required": ["query"]
      }
    },
    {
      "name": "memory_forget",
      "description": "Remove a specific memory",
      "inputSchema": {
        "type": "object",
        "properties": {
          "id": {"type": "string", "description": "Memory ID to forget"},
          "reason": {"type": "string", "description": "Why this memory should be removed"}
        },
        "required": ["id"]
      }
    },
    {
      "name": "memory_update",
      "description": "Update an existing memory with new information",
      "inputSchema": {
        "type": "object",
        "properties": {
          "id": {"type": "string"},
          "content": {"type": "string", "description": "Updated memory content"}
        },
        "required": ["id", "content"]
      }
    }
  ]
}
```

### Scope Path Syntax

For MCP (string-based), scopes use path notation:

```
org:acme/team:support/agent:billing/user:wang/session:abc123
```

Partial paths are valid: `user:wang` means "this user, any org/team/agent".

---

## 7. Memory Lifecycle

```
         ┌───────────┐
         │  Ingest   │  ← Agent writes raw content
         └─────┬─────┘
               │
               ▼
         ┌───────────┐
         │  Process  │  ← Embedding + classification + dedup check
         └─────┬─────┘
               │
               ▼
         ┌───────────┐
     ┌───│   Store   │  ← Persist + replicate to cluster
     │   └─────┬─────┘
     │         │
     │         ▼
     │   ┌───────────┐
     │   │   Index   │  ← Vector index + graph index (if relation)
     │   └─────┬─────┘
     │         │
     │         ▼
     │   ┌───────────┐
     │   │   Serve   │  ← Available for search/retrieval
     │   └─────┬─────┘
     │         │
     │         ▼ (time passes)
     │   ┌───────────┐
     │   │  Compact  │  ← Merge related memories, summarize old ones
     │   └─────┬─────┘
     │         │
     │         ▼ (TTL reached or explicit forget)
     │   ┌───────────┐
     └──▶│  Archive  │  ← Cold storage or deletion
         └───────────┘
```

### Automatic Compaction

Over time, memories accumulate. Synapse periodically runs compaction:

1. **Deduplication** — Near-identical memories merged
2. **Summarization** — Many episode memories → one summary
3. **Decay** — Low-confidence, never-accessed memories expire
4. **Promotion/Demotion** — Frequently accessed → hot tier; rarely accessed → cold

---

## 8. Authentication & Authorization

```protobuf
message AuthContext {
  string token = 1;             // Bearer token / API key
  string agent_id = 2;          // Authenticated agent identity
  repeated string roles = 3;    // Granted roles
}

// Access Control Entry
message ACL {
  string scope_pattern = 1;     // Glob pattern: "org:acme/team:*"
  repeated Permission perms = 2;
}

enum Permission {
  READ = 0;
  WRITE = 1;
  DELETE = 2;
  ADMIN = 3;      // Can modify ACLs
  SUBSCRIBE = 4;  // Can subscribe to events
}
```

**Default Policy:** An agent can read/write within its own scope. Cross-scope access requires explicit ACL grants.

---

## 9. SDK Interface (Python Reference)

```python
class SynapseClient:
    def __init__(self, endpoint: str, token: str = None, default_scope: Scope = None): ...
    
    # Core operations
    async def add(self, content: str, *, scope: Scope = None, kind: MemoryKind = None,
                  tags: list[str] = None, confidence: float = 1.0,
                  deduplicate: bool = True, on_conflict: ConflictPolicy = None) -> MemoryRecord: ...
    
    async def search(self, query: str, *, scope: Scope = None, top_k: int = 10,
                     min_score: float = 0.0, kinds: list[MemoryKind] = None,
                     tags: list[str] = None, mode: SearchMode = SearchMode.HYBRID) -> list[SearchResult]: ...
    
    async def get(self, id: str) -> MemoryRecord: ...
    async def update(self, id: str, content: str) -> MemoryRecord: ...
    async def forget(self, id: str = None, *, scope: Scope = None, before: datetime = None) -> int: ...
    
    # Real-time
    async def subscribe(self, scope: Scope = None, event_types: list[EventType] = None) -> AsyncIterator[MemoryEvent]: ...
    
    # Convenience
    def to_context(self, results: list[SearchResult], max_tokens: int = 2000) -> str:
        """Format search results as an LLM-friendly context string."""
        ...
    
    # Bulk
    async def batch_add(self, records: list[dict]) -> list[MemoryRecord]: ...
    async def export_all(self, scope: Scope = None) -> AsyncIterator[MemoryRecord]: ...
```

---

## 10. Open Questions

- [ ] Graph memory: should relations be first-class edges in a property graph, or just tagged records?
- [ ] Embedding model: should the server enforce one model, or accept pre-computed embeddings?
- [ ] Compaction triggers: time-based? count-based? agent-requested?
- [ ] Cross-org federation: is it needed in v1, or deferred?
- [ ] Memory encryption: at-rest encryption per-scope?
- [ ] Quotas: per-scope storage limits?

---

## Appendix A: Comparison with Existing Protocols

| Aspect | MCP | Synapse Protocol |
|--------|-----|------------------|
| Purpose | Tool/resource discovery | Memory persistence & sync |
| Transport | stdio / HTTP+SSE | gRPC (primary) + REST + MCP adapter |
| State | Stateless tool calls | Stateful, distributed |
| Multi-agent | Not addressed | Core design goal |
| Conflict resolution | N/A | Built-in semantic merge |

Synapse is **complementary** to MCP: it exposes itself as an MCP tool server, while providing a richer native protocol for advanced use cases.

## Appendix B: Inspirations

- Computer architecture memory hierarchy (L1/L2/L3/DRAM/Disk)
- CRDT (Conflict-free Replicated Data Types)
- Git's content-addressable storage + merge strategies
- Dynamo-style vector clocks for causality
- MemGPT's "LLM as OS" memory management metaphor
- arxiv:2603.10062 (Multi-Agent Memory from a Computer Architecture Perspective)
