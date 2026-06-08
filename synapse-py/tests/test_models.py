"""Tests for Pydantic models."""

import pytest
from datetime import datetime, timezone

from synapse_memory.models import (
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


class TestMemoryRecord:
    """Test MemoryRecord model."""

    def test_minimal_creation(self) -> None:
        record = MemoryRecord(content="Hello world")
        assert record.content == "Hello world"
        assert record.id == ""
        assert record.kind == MemoryKind.FACT
        assert record.confidence == 1.0
        assert record.tags == []
        assert record.embedding == []

    def test_full_creation(self) -> None:
        now = datetime.now(tz=timezone.utc)
        record = MemoryRecord(
            id="01HYX2N8P4X",
            content="Project deadline is July 15th",
            scope=Scope(org="acme", team="engineering"),
            tags=["project", "deadline"],
            kind=MemoryKind.FACT,
            confidence=0.95,
            source=Source(agent_id="planner", session_id="sess-001"),
            created_at=now,
            version=3,
            lineage=["parent-001"],
        )
        assert record.id == "01HYX2N8P4X"
        assert record.scope.org == "acme"
        assert record.tags == ["project", "deadline"]
        assert record.confidence == 0.95
        assert record.source.agent_id == "planner"
        assert record.version == 3

    def test_confidence_bounds(self) -> None:
        # Valid
        record = MemoryRecord(content="test", confidence=0.0)
        assert record.confidence == 0.0
        record = MemoryRecord(content="test", confidence=1.0)
        assert record.confidence == 1.0

        # Invalid
        with pytest.raises(Exception):
            MemoryRecord(content="test", confidence=1.5)
        with pytest.raises(Exception):
            MemoryRecord(content="test", confidence=-0.1)

    def test_serialization_roundtrip(self) -> None:
        record = MemoryRecord(
            id="test-id",
            content="Some memory",
            scope=Scope(org="acme", visibility=Visibility.SHARED),
            kind=MemoryKind.EPISODE,
            tags=["tag1"],
        )
        data = record.model_dump()
        restored = MemoryRecord.model_validate(data)
        assert restored.id == record.id
        assert restored.content == record.content
        assert restored.scope.org == "acme"
        assert restored.scope.visibility == Visibility.SHARED
        assert restored.kind == MemoryKind.EPISODE


class TestVectorClock:
    """Test VectorClock model."""

    def test_increment(self) -> None:
        vc = VectorClock()
        vc.increment("node-a")
        assert vc.clock["node-a"] == 1
        vc.increment("node-a")
        assert vc.clock["node-a"] == 2

    def test_merge(self) -> None:
        vc1 = VectorClock(clock={"a": 3, "b": 2})
        vc2 = VectorClock(clock={"a": 1, "b": 4, "c": 1})
        merged = vc1.merge(vc2)
        assert merged.clock == {"a": 3, "b": 4, "c": 1}

    def test_dominates(self) -> None:
        vc1 = VectorClock(clock={"a": 3, "b": 2})
        vc2 = VectorClock(clock={"a": 2, "b": 1})
        assert vc1.dominates(vc2) is True
        assert vc2.dominates(vc1) is False

    def test_dominates_equal_not_dominant(self) -> None:
        vc1 = VectorClock(clock={"a": 3, "b": 2})
        vc2 = VectorClock(clock={"a": 3, "b": 2})
        assert vc1.dominates(vc2) is False
        assert vc2.dominates(vc1) is False

    def test_concurrent(self) -> None:
        vc1 = VectorClock(clock={"a": 3, "b": 2})
        vc2 = VectorClock(clock={"a": 2, "b": 4})
        assert vc1.is_concurrent(vc2) is True
        assert vc2.is_concurrent(vc1) is True

    def test_not_concurrent_when_dominated(self) -> None:
        vc1 = VectorClock(clock={"a": 3, "b": 2})
        vc2 = VectorClock(clock={"a": 1, "b": 1})
        assert vc1.is_concurrent(vc2) is False


class TestScope:
    """Test Scope model."""

    def test_to_path(self) -> None:
        scope = Scope(org="acme", team="support", user="wang")
        assert scope.to_path() == "org:acme/team:support/user:wang"

    def test_to_path_empty(self) -> None:
        scope = Scope()
        assert scope.to_path() == ""

    def test_matches_same(self) -> None:
        scope1 = Scope(org="acme", team="support")
        scope2 = Scope(org="acme", team="support")
        assert scope1.matches(scope2) is True

    def test_matches_subset(self) -> None:
        """A more general scope matches a more specific one."""
        general = Scope(org="acme")
        specific = Scope(org="acme", team="support")
        assert general.matches(specific) is True

    def test_not_matches_different_org(self) -> None:
        scope1 = Scope(org="acme")
        scope2 = Scope(org="beta")
        assert scope1.matches(scope2) is False

    def test_is_parent_of(self) -> None:
        parent = Scope(org="acme")
        child = Scope(org="acme", team="support")
        assert parent.is_parent_of(child) is True
        assert child.is_parent_of(parent) is False


class TestSearchResult:
    """Test SearchResult model."""

    def test_creation(self) -> None:
        result = SearchResult(
            record=MemoryRecord(content="test"),
            score=0.85,
            explanation="High semantic similarity",
        )
        assert result.score == 0.85
        assert result.record.content == "test"
        assert result.explanation == "High semantic similarity"


class TestConflict:
    """Test Conflict model."""

    def test_creation(self) -> None:
        conflict = Conflict(
            id="conflict-1",
            records=[
                MemoryRecord(content="Deadline is June 30"),
                MemoryRecord(content="Deadline is July 15"),
            ],
            status=ConflictStatus.PENDING,
        )
        assert len(conflict.records) == 2
        assert conflict.status == ConflictStatus.PENDING
        assert conflict.resolution is None

    def test_with_resolution(self) -> None:
        conflict = Conflict(
            id="conflict-1",
            records=[
                MemoryRecord(content="old"),
                MemoryRecord(content="new"),
            ],
            status=ConflictStatus.AUTO_RESOLVED,
            resolution=Resolution(
                strategy=ResolutionStrategy.LLM_MERGE,
                result=MemoryRecord(content="merged"),
                reasoning="Combined both versions",
                resolved_by="llm",
            ),
        )
        assert conflict.resolution is not None
        assert conflict.resolution.strategy == ResolutionStrategy.LLM_MERGE
        assert conflict.resolution.result.content == "merged"


class TestMemoryEvent:
    """Test MemoryEvent model."""

    def test_creation(self) -> None:
        event = MemoryEvent(
            type=EventType.MEMORY_ADDED,
            record=MemoryRecord(content="new memory"),
            source_node="node-1",
        )
        assert event.type == EventType.MEMORY_ADDED
        assert event.record is not None
        assert event.record.content == "new memory"


class TestEnums:
    """Test enum values."""

    def test_memory_kind_values(self) -> None:
        assert MemoryKind.FACT.value == "fact"
        assert MemoryKind.PREFERENCE.value == "preference"
        assert MemoryKind.CORRECTION.value == "correction"

    def test_search_mode_values(self) -> None:
        assert SearchMode.SEMANTIC.value == "semantic"
        assert SearchMode.HYBRID.value == "hybrid"
        assert SearchMode.GRAPH.value == "graph"

    def test_conflict_policy_values(self) -> None:
        assert ConflictPolicy.AUTO_MERGE.value == "auto_merge"
        assert ConflictPolicy.REJECT.value == "reject"
        assert ConflictPolicy.FORCE.value == "force"
