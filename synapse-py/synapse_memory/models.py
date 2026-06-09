"""Pydantic v2 models for the Synapse Memory Protocol."""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


# === Enums ===


class MemoryKind(str, Enum):
    """Classification of memory type."""

    FACT = "fact"
    PREFERENCE = "preference"
    EPISODE = "episode"
    RULE = "rule"
    RELATION = "relation"
    CORRECTION = "correction"
    SUMMARY = "summary"


class Visibility(str, Enum):
    """Access visibility level for a memory record."""

    PRIVATE = "private"
    SCOPE_UP = "scope_up"
    SCOPE_DOWN = "scope_down"
    SHARED = "shared"
    PUBLIC = "public"


class ConflictPolicy(str, Enum):
    """Policy for handling conflicts on write."""

    DETECT_AND_QUEUE = "detect_and_queue"
    AUTO_MERGE = "auto_merge"
    REJECT = "reject"
    FORCE = "force"


class SearchMode(str, Enum):
    """Search strategy.

    Note: GRAPH mode is reserved for future implementation.
    Currently only SEMANTIC, KEYWORD, and HYBRID are supported.
    """

    SEMANTIC = "semantic"
    KEYWORD = "keyword"
    HYBRID = "hybrid"
    GRAPH = "graph"  # NOT IMPLEMENTED — reserved for future graph-based retrieval


class EventType(str, Enum):
    """Types of memory events."""

    ALL = "all"
    MEMORY_ADDED = "memory_added"
    MEMORY_UPDATED = "memory_updated"
    MEMORY_FORGOTTEN = "memory_forgotten"
    CONFLICT_DETECTED = "conflict_detected"
    CONFLICT_RESOLVED = "conflict_resolved"
    MEMORY_EXPIRED = "memory_expired"


class ConflictStatus(str, Enum):
    """Status of a conflict."""

    PENDING = "pending"
    AUTO_RESOLVED = "auto_resolved"
    MANUAL = "manual"
    DISCARDED = "discarded"


class ResolutionStrategy(str, Enum):
    """Strategy for resolving conflicts."""

    LAST_WRITER_WINS = "last_writer_wins"
    FIRST_WRITER_WINS = "first_writer_wins"
    LLM_MERGE = "llm_merge"
    KEEP_BOTH = "keep_both"
    MANUAL = "manual"
    CONFIDENCE_WINS = "confidence_wins"
    CUSTOM = "custom"


# === Models ===


class VectorClock(BaseModel):
    """Distributed vector clock for causality tracking."""

    clock: dict[str, int] = Field(default_factory=dict)

    def increment(self, node_id: str) -> None:
        """Increment the clock for a given node."""
        self.clock[node_id] = self.clock.get(node_id, 0) + 1

    def merge(self, other: VectorClock) -> VectorClock:
        """Merge two vector clocks, taking the max of each component."""
        merged = dict(self.clock)
        for node_id, ts in other.clock.items():
            merged[node_id] = max(merged.get(node_id, 0), ts)
        return VectorClock(clock=merged)

    def dominates(self, other: VectorClock) -> bool:
        """Check if this clock strictly dominates another (happened-after)."""
        dominated = False
        for node_id in set(self.clock.keys()) | set(other.clock.keys()):
            self_ts = self.clock.get(node_id, 0)
            other_ts = other.clock.get(node_id, 0)
            if self_ts < other_ts:
                return False
            if self_ts > other_ts:
                dominated = True
        return dominated

    def is_concurrent(self, other: VectorClock) -> bool:
        """Check if two clocks are concurrent (conflict potential)."""
        return not self.dominates(other) and not other.dominates(self)


class Source(BaseModel):
    """Origin information for a memory record."""

    agent_id: str = ""
    session_id: str = ""
    input_hash: str = ""
    tool_call_id: str = ""


class Scope(BaseModel):
    """Ownership and visibility scope for a memory record."""

    org: Optional[str] = None
    team: Optional[str] = None
    agent: Optional[str] = None
    user: Optional[str] = None
    session: Optional[str] = None
    visibility: Visibility = Visibility.PRIVATE

    def to_path(self) -> str:
        """Serialize scope to path string: 'org:acme/team:support/user:wang'."""
        parts: list[str] = []
        if self.org:
            parts.append(f"org:{self.org}")
        if self.team:
            parts.append(f"team:{self.team}")
        if self.agent:
            parts.append(f"agent:{self.agent}")
        if self.user:
            parts.append(f"user:{self.user}")
        if self.session:
            parts.append(f"session:{self.session}")
        return "/".join(parts)

    def matches(self, other: Scope) -> bool:
        """Check if this scope matches another (considering wildcards/None)."""
        if self.org and other.org and self.org != other.org:
            return False
        if self.team and other.team and self.team != other.team:
            return False
        if self.agent and other.agent and self.agent != other.agent:
            return False
        if self.user and other.user and self.user != other.user:
            return False
        if self.session and other.session and self.session != other.session:
            return False
        return True

    def is_parent_of(self, other: Scope) -> bool:
        """Check if this scope is a parent (more general) scope of another."""
        levels = ["org", "team", "agent", "user", "session"]
        self_depth = 0
        other_depth = 0
        for level in levels:
            if getattr(self, level):
                self_depth += 1
            if getattr(other, level):
                other_depth += 1

        if self_depth >= other_depth:
            return False

        # Verify all our set fields match the other
        for level in levels:
            self_val = getattr(self, level)
            other_val = getattr(other, level)
            if self_val and self_val != other_val:
                return False
        return True


class MemoryRecord(BaseModel):
    """The atomic unit of knowledge stored in Synapse."""

    id: str = ""
    content: str
    embedding: list[float] = Field(default_factory=list)

    # Metadata
    scope: Scope = Field(default_factory=Scope)
    tags: list[str] = Field(default_factory=list)
    kind: MemoryKind = MemoryKind.FACT
    confidence: float = Field(default=1.0, ge=0.0, le=1.0)
    source: Source = Field(default_factory=Source)

    # Temporal
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    accessed_at: Optional[datetime] = None
    expires_at: Optional[datetime] = None

    # Versioning
    version: int = Field(default=0, ge=0)
    vector_clock: VectorClock = Field(default_factory=VectorClock)
    lineage: list[str] = Field(default_factory=list)


class SearchResult(BaseModel):
    """A single result from a memory search."""

    record: MemoryRecord
    score: float = Field(default=0.0, ge=0.0, le=1.0)
    explanation: str = ""


class Resolution(BaseModel):
    """Resolution of a conflict."""

    strategy: ResolutionStrategy
    result: MemoryRecord
    reasoning: Optional[str] = None
    resolved_by: str = "system"
    resolved_at: Optional[datetime] = None


class Conflict(BaseModel):
    """A conflict between concurrent writes."""

    id: str = ""
    records: list[MemoryRecord] = Field(default_factory=list)
    detected_at: Optional[datetime] = None
    status: ConflictStatus = ConflictStatus.PENDING
    resolution: Optional[Resolution] = None


class MemoryEvent(BaseModel):
    """An event emitted by the memory system."""

    type: EventType
    record: Optional[MemoryRecord] = None
    conflict: Optional[Conflict] = None
    timestamp: Optional[datetime] = None
    source_node: str = ""
