# API Reference

## gRPC Services

Synapse exposes three gRPC services on the configured port (default: 9090).

---

## MemoryService

### `Add(AddRequest) → AddResponse`

Store a new memory record.

| Field | Type | Description |
|-------|------|-------------|
| `content` | string | **Required.** The memory text |
| `scope` | Scope | **Required.** Ownership scope |
| `tags` | string[] | Optional labels |
| `kind` | MemoryKind | Optional classification (auto-detected if unset) |
| `confidence` | float | Confidence score 0-1 (default: 1.0) |
| `expires_at` | Timestamp | Optional TTL |
| `deduplicate` | bool | Check for near-duplicates before inserting |
| `dedup_threshold` | float | Similarity threshold for dedup (default: 0.92) |
| `on_conflict` | ConflictPolicy | Conflict behavior |

**Response:** The stored `MemoryRecord` with generated ID, timestamps, and vector clock.

---

### `Get(GetRequest) → GetResponse`

Retrieve a single record by ID.

| Field | Type | Description |
|-------|------|-------------|
| `id` | string | **Required.** Record ID |

**Response:** The `MemoryRecord`, or `NOT_FOUND` error.

---

### `Search(SearchRequest) → SearchResponse`

Semantic search over stored memories.

| Field | Type | Description |
|-------|------|-------------|
| `query` | string | Search query text |
| `query_embedding` | float[] | Pre-computed query embedding (bypasses internal embedding) |
| `scope` | Scope | Scope filter |
| `top_k` | uint32 | Max results (default: 10) |
| `min_score` | float | Minimum similarity score |
| `kinds` | MemoryKind[] | Filter by kind |
| `tags` | string[] | Filter by tags (AND logic) |
| `time_range` | TimeRange | Filter by creation time |
| `mode` | SearchMode | SEMANTIC / KEYWORD / HYBRID / GRAPH |
| `rerank` | bool | Apply LLM reranking |

**Response:** List of `SearchResult` (record + score + explanation).

---

### `Update(UpdateRequest) → UpdateResponse`

Update an existing memory. Previous version is saved in history.

| Field | Type | Description |
|-------|------|-------------|
| `id` | string | **Required.** Record ID |
| `content` | string | New content (empty = unchanged) |
| `tags` | string[] | New tags (empty = unchanged) |
| `kind` | MemoryKind | New kind (0 = unchanged) |
| `confidence` | float | New confidence (0 = unchanged) |

---

### `Forget(ForgetRequest) → ForgetResponse`

Delete memory records.

| Field | Type | Description |
|-------|------|-------------|
| `id` | string | Delete specific record by ID |
| `scope` | Scope | Delete all records matching scope |
| `before` | Timestamp | Only delete records created before this time |
| `reason` | string | Audit trail reason |

Provide either `id` or `scope` (not both).

**Response:** Number of records deleted.

---

### `List(ListRequest) → ListResponse`

List records with filters and pagination.

| Field | Type | Description |
|-------|------|-------------|
| `scope` | Scope | Scope filter |
| `limit` | uint32 | Page size (default: 50) |
| `offset` | uint32 | Pagination offset |
| `kinds` | MemoryKind[] | Filter by kind |
| `tags` | string[] | Filter by tags |

---

### `History(HistoryRequest) → HistoryResponse`

Get version history of a record (all previous versions).

---

### `Subscribe(SubscribeRequest) → stream MemoryEvent`

Subscribe to real-time memory events (server-streaming RPC).

| Field | Type | Description |
|-------|------|-------------|
| `scope` | Scope | Filter events by scope visibility |
| `event_types` | EventType[] | Filter event types (empty = all) |

**Event types:** `MEMORY_ADDED`, `MEMORY_UPDATED`, `MEMORY_FORGOTTEN`, `CONFLICT_DETECTED`, `CONFLICT_RESOLVED`, `MEMORY_EXPIRED`

---

### `BatchAdd(BatchAddRequest) → BatchAddResponse`

Add multiple records in one call.

---

### `Export(ExportRequest) → stream MemoryRecord`

Export all records matching a scope (server-streaming).

---

### `Import(stream MemoryRecord) → ImportResponse`

Import records (client-streaming). Returns count of imported/skipped.

---

## ConflictService

### `ListConflicts(ListConflictsRequest) → ListConflictsResponse`

List detected conflicts, optionally filtered by status.

### `ResolveConflict(ResolveConflictRequest) → ResolveConflictResponse`

Resolve a conflict using a specified strategy.

| Field | Type | Description |
|-------|------|-------------|
| `conflict_id` | string | **Required.** Conflict ID |
| `strategy` | ResolutionStrategy | Resolution strategy to apply |
| `manual_content` | string | For MANUAL strategy: human-provided resolution |

**Strategies:** `LAST_WRITER_WINS`, `FIRST_WRITER_WINS`, `LLM_MERGE`, `KEEP_BOTH`, `CONFIDENCE_WINS`, `MANUAL`, `CUSTOM`

### `SetPolicy(SetPolicyRequest) → SetPolicyResponse`

Configure default conflict resolution policy for a scope.

---

## ClusterService

### `Join(JoinRequest) → JoinResponse`

Register a new node in the cluster.

### `Leave(LeaveRequest) → LeaveResponse`

Remove a node from the cluster.

### `Status(StatusRequest) → ClusterStatus`

Get current cluster status (nodes, consistency level, record count, pending syncs).

### `Sync(SyncRequest) → stream SyncEvent`

Request sync events from a given vector clock position (server-streaming).

---

## REST API

All gRPC methods are also available via REST (JSON-over-HTTP):

| Method | Endpoint | Maps to |
|--------|----------|---------|
| POST | `/v1/memory` | Add |
| GET | `/v1/memory/:id` | Get |
| POST | `/v1/memory/search` | Search |
| PATCH | `/v1/memory/:id` | Update |
| DELETE | `/v1/memory/:id` | Forget |
| GET | `/v1/memory` | List |
| GET | `/v1/memory/:id/history` | History |
| POST | `/v1/memory/batch` | BatchAdd |
| GET | `/v1/conflicts` | ListConflicts |
| POST | `/v1/conflicts/:id/resolve` | ResolveConflict |
| GET | `/v1/cluster/status` | Status |

---

## MCP Tools

When running as an MCP server, Synapse exposes:

| Tool | Description |
|------|-------------|
| `memory_store` | Store a memory (content, kind, tags, scope) |
| `memory_recall` | Search for relevant memories (query, top_k, scope) |
| `memory_update` | Update an existing memory (id, content) |
| `memory_forget` | Remove a memory (id, reason) |

See [examples/mcp_integration.py](../examples/mcp_integration.py) for usage.

---

## Enums

### MemoryKind
`FACT` | `PREFERENCE` | `EPISODE` | `RULE` | `RELATION` | `CORRECTION` | `SUMMARY`

### Visibility
`PRIVATE` | `SCOPE_UP` | `SCOPE_DOWN` | `SHARED` | `PUBLIC`

### SearchMode
`SEMANTIC` | `KEYWORD` | `HYBRID` | `GRAPH`

### ConsistencyLevel
`EVENTUAL` | `BOUNDED_STALENESS` | `STRONG`

### ConflictPolicy (on write)
`DETECT_AND_QUEUE` | `AUTO_MERGE` | `REJECT` | `FORCE`

### ResolutionStrategy (on resolve)
`LAST_WRITER_WINS` | `FIRST_WRITER_WINS` | `LLM_MERGE` | `KEEP_BOTH` | `MANUAL` | `CONFIDENCE_WINS` | `CUSTOM`
