"""Tests for LocalStore."""

import tempfile
from pathlib import Path

import pytest

from synapse_memory.local_store import LocalStore
from synapse_memory.models import MemoryKind, Scope, Visibility


@pytest.fixture
def store(tmp_path: Path) -> LocalStore:
    """Create a LocalStore with a temporary database."""
    db_path = tmp_path / "test.db"
    return LocalStore(db_path=db_path)


class TestLocalStoreBasic:
    """Test basic CRUD operations."""

    def test_add_and_get(self, store: LocalStore) -> None:
        record = store.add("Hello world", kind=MemoryKind.FACT)
        assert record.id
        assert record.content == "Hello world"
        assert record.kind == MemoryKind.FACT
        assert record.version == 1

        fetched = store.get(record.id)
        assert fetched is not None
        assert fetched.content == "Hello world"

    def test_add_with_scope(self, store: LocalStore) -> None:
        scope = Scope(org="acme", team="eng", visibility=Visibility.SHARED)
        record = store.add("Team fact", scope=scope, kind=MemoryKind.FACT)
        assert record.scope.org == "acme"
        assert record.scope.team == "eng"

    def test_add_with_tags(self, store: LocalStore) -> None:
        record = store.add("Tagged memory", tags=["project", "deadline"])
        assert record.tags == ["project", "deadline"]

    def test_get_nonexistent(self, store: LocalStore) -> None:
        result = store.get("nonexistent-id")
        assert result is None

    def test_update(self, store: LocalStore) -> None:
        record = store.add("Original content")
        updated = store.update(record.id, content="Updated content")
        assert updated is not None
        assert updated.content == "Updated content"
        assert updated.version == 2

    def test_update_nonexistent(self, store: LocalStore) -> None:
        result = store.update("nonexistent", content="foo")
        assert result is None

    def test_forget(self, store: LocalStore) -> None:
        record = store.add("To forget")
        assert store.forget(record.id) is True
        assert store.get(record.id) is None

    def test_forget_nonexistent(self, store: LocalStore) -> None:
        assert store.forget("nonexistent") is False

    def test_count(self, store: LocalStore) -> None:
        assert store.count() == 0
        store.add("One")
        store.add("Two")
        assert store.count() == 2

    def test_list_memories(self, store: LocalStore) -> None:
        store.add("First", kind=MemoryKind.FACT)
        store.add("Second", kind=MemoryKind.PREFERENCE)
        store.add("Third", kind=MemoryKind.FACT)

        all_records = store.list_memories()
        assert len(all_records) == 3

        facts = store.list_memories(kinds=[MemoryKind.FACT])
        assert len(facts) == 2


class TestLocalStoreSearch:
    """Test search functionality."""

    def test_keyword_search(self, store: LocalStore) -> None:
        store.add("Python is a programming language")
        store.add("Rust is fast and safe")
        store.add("Python can do web development")

        results = store.search("Python")
        assert len(results) >= 2
        assert all("python" in r.record.content.lower() for r in results)

    def test_keyword_search_no_results(self, store: LocalStore) -> None:
        store.add("Hello world")
        results = store.search("nonexistent_term_xyz")
        assert len(results) == 0

    def test_search_with_tag_filter(self, store: LocalStore) -> None:
        store.add("Tagged item", tags=["important"])
        store.add("Other item", tags=["misc"])

        results = store.search("item", tags=["important"])
        assert len(results) == 1
        assert results[0].record.tags == ["important"]

    def test_search_with_kind_filter(self, store: LocalStore) -> None:
        store.add("A preference", kind=MemoryKind.PREFERENCE)
        store.add("A fact", kind=MemoryKind.FACT)

        results = store.search("a", kinds=[MemoryKind.PREFERENCE])
        assert len(results) == 1
        assert results[0].record.kind == MemoryKind.PREFERENCE


class TestLocalStoreExpiry:
    """Test TTL/expiry cleanup."""

    def test_cleanup_expired(self, tmp_path: Path) -> None:
        import time as _time

        db_path = tmp_path / "test.db"
        store = LocalStore(db_path=db_path)

        # Add a record with expires_at in the past
        store.add("This will expire")
        # Manually set expires_at to the past
        store._conn.execute(
            "UPDATE memories SET expires_at = ? WHERE 1=1",
            (_time.time() - 100,),
        )
        store._conn.commit()

        assert store.count() == 1
        deleted = store.cleanup_expired()
        assert deleted == 1
        assert store.count() == 0
