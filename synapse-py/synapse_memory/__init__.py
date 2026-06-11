"""Synapse Memory — Persistent memory for AI agents.

Two ways to use:

1. As MCP server (recommended for most users):
   $ synapse-mcp

2. As Python library:
   >>> from synapse_memory import LocalStore, MemoryKind, Scope
   >>> store = LocalStore()
   >>> store.add("User prefers concise answers", kind=MemoryKind.PREFERENCE)
   >>> results = store.search("user preferences")

3. As distributed client (connects to Synapse server):
   >>> from synapse_memory import SynapseClient
   >>> async with SynapseClient("localhost:9090") as client:
   ...     await client.add("User prefers concise answers")
"""

from .client import (
    CircuitBreaker,
    CircuitState,
    ConflictError,
    ConnectionError,
    NotFoundError,
    SynapseClient,
    SynapseError,
)
from .local_store import LocalStore
from .models import (
    Conflict,
    ConflictPolicy,
    ConflictStatus,
    EventType,
    MemoryEvent,
    MemoryKind,
    MemoryRecord,
    Resolution,
    ResolutionStrategy,
    Scope,
    SearchMode,
    SearchResult,
    Source,
    VectorClock,
    Visibility,
)
from .embeddings import BatchEmbeddingFn, EmbeddingFn
from .scope import ScopeParseError, is_visible, parse_scope, serialize_scope

from .utils import to_context

__all__ = [
    # Local store (zero-config)
    "LocalStore",
    # Remote client (distributed)
    "SynapseClient",
    "SynapseError",
    "ConnectionError",
    "NotFoundError",
    "ConflictError",
    # Circuit breaker
    "CircuitBreaker",
    "CircuitState",
    # Embedding types
    "EmbeddingFn",
    "BatchEmbeddingFn",
    # Models
    "MemoryRecord",
    "Scope",
    "Source",
    "VectorClock",
    "SearchResult",
    "MemoryEvent",
    "Conflict",
    "Resolution",
    # Enums
    "MemoryKind",
    "Visibility",
    "ConflictPolicy",
    "SearchMode",
    "EventType",
    "ConflictStatus",
    "ResolutionStrategy",
    # Scope utilities
    "parse_scope",
    "serialize_scope",
    "is_visible",
    "ScopeParseError",
    # Utilities
    "to_context",
]

__version__ = "0.1.0"
