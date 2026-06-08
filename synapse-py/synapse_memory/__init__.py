"""Synapse Memory Protocol SDK — Python client for distributed AI agent memory.

Example usage:
    >>> from synapse_memory import SynapseClient, MemoryKind, Scope
    >>> async with SynapseClient("localhost:9090") as client:
    ...     record = await client.add("User prefers concise answers", kind=MemoryKind.PREFERENCE)
    ...     results = await client.search("user preferences")
    ...     context = client.to_context(results)
"""

from .client import (
    ConflictError,
    ConnectionError,
    NotFoundError,
    SynapseClient,
    SynapseError,
)
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
from .scope import ScopeParseError, is_visible, parse_scope, serialize_scope
from .utils import to_context

__all__ = [
    # Client
    "SynapseClient",
    "SynapseError",
    "ConnectionError",
    "NotFoundError",
    "ConflictError",
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
