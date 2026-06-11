"""Tests for MCP server security measures."""

import uuid
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from synapse_memory.mcp_server import (
    MAX_CONTENT_LENGTH,
    MAX_RESULT_TOKENS,
    MAX_TOP_K,
    SynapseMCPServer,
)
from synapse_memory.models import MemoryKind, MemoryRecord, Scope, SearchResult


@pytest.fixture
def mcp_server(tmp_path: Path) -> SynapseMCPServer:
    """Create an MCP server in local mode with a temp database."""
    return SynapseMCPServer(
        db_path=tmp_path / "test.db",
        embedding="none",
    )


class TestContentLengthValidation:
    """Test content length validation in store/update."""

    def test_store_rejects_oversized_content(self, mcp_server: SynapseMCPServer) -> None:
        """Content exceeding MAX_CONTENT_LENGTH should be rejected."""
        store = mcp_server._get_store()
        oversized_content = "x" * (MAX_CONTENT_LENGTH + 1)

        result = mcp_server._local_store(store, {"content": oversized_content})
        assert "✗" in result
        assert "too large" in result.lower()
        assert str(MAX_CONTENT_LENGTH) in result

    def test_store_accepts_valid_content(self, mcp_server: SynapseMCPServer) -> None:
        """Content within limit should be accepted."""
        store = mcp_server._get_store()
        valid_content = "x" * 1000

        result = mcp_server._local_store(store, {"content": valid_content})
        assert "✓" in result
        assert "Memory stored" in result

    def test_store_accepts_exactly_max_content(self, mcp_server: SynapseMCPServer) -> None:
        """Content at exactly MAX_CONTENT_LENGTH should be accepted."""
        store = mcp_server._get_store()
        content = "x" * MAX_CONTENT_LENGTH

        result = mcp_server._local_store(store, {"content": content})
        assert "✓" in result

    def test_update_rejects_oversized_content(self, mcp_server: SynapseMCPServer) -> None:
        """Update with oversized content should be rejected."""
        store = mcp_server._get_store()

        # First create a valid record
        record = store.add("initial content")
        oversized_content = "x" * (MAX_CONTENT_LENGTH + 1)

        result = mcp_server._local_update(store, {
            "id": record.id,
            "content": oversized_content,
        })
        assert "✗" in result
        assert "too large" in result.lower()


class TestTopKCapping:
    """Test that top_k is capped at MAX_TOP_K."""

    def test_local_recall_caps_top_k(self, mcp_server: SynapseMCPServer) -> None:
        """top_k should be capped at MAX_TOP_K."""
        store = mcp_server._get_store()

        # Add some records
        for i in range(10):
            store.add(f"Record number {i}")

        # Request more than MAX_TOP_K
        result = mcp_server._local_recall(store, {
            "query": "Record",
            "top_k": 100,
        })
        # Should not error — the cap is applied silently
        assert "memories" in result.lower() or "No memories" in result

    def test_top_k_at_limit(self, mcp_server: SynapseMCPServer) -> None:
        """top_k at exactly MAX_TOP_K should work."""
        store = mcp_server._get_store()
        store.add("Test record")

        result = mcp_server._local_recall(store, {
            "query": "Test",
            "top_k": MAX_TOP_K,
        })
        # Should work without issues
        assert "Test record" in result or "No memories" in result


class TestResultTruncation:
    """Test token-based result truncation."""

    def test_truncation_with_many_large_results(self) -> None:
        """Results should be truncated when exceeding MAX_RESULT_TOKENS."""
        # Create many large results
        # Each record with 1000 chars content -> ~270 tokens per block
        # 50 records * 270 tokens = ~13500, exceeding 8000 limit
        results = []
        for i in range(50):
            record = MemoryRecord(
                id=str(uuid.uuid4()),
                content="A" * 1000,
                kind=MemoryKind.FACT,
                tags=["tag1", "tag2"],
            )
            results.append(SearchResult(record=record, score=0.9))

        formatted = SynapseMCPServer._format_results(results)
        assert "[..." in formatted
        assert "truncated" in formatted.lower()

    def test_no_truncation_with_small_results(self) -> None:
        """Small result sets should not be truncated."""
        results = []
        for i in range(3):
            record = MemoryRecord(
                id=str(uuid.uuid4()),
                content=f"Short content {i}",
                kind=MemoryKind.FACT,
                tags=[],
            )
            results.append(SearchResult(record=record, score=0.8))

        formatted = SynapseMCPServer._format_results(results)
        assert "truncated" not in formatted.lower()
        # All 3 results should be included
        assert 'index="3"' in formatted

    def test_truncation_includes_partial_results(self) -> None:
        """Truncation should include as many results as fit."""
        results = []
        for i in range(100):
            record = MemoryRecord(
                id=str(uuid.uuid4()),
                content="x" * 200,
                kind=MemoryKind.FACT,
                tags=[],
            )
            results.append(SearchResult(record=record, score=0.5))

        formatted = SynapseMCPServer._format_results(results)
        # Should include some but not all
        assert 'index="1"' in formatted
        assert "truncated" in formatted.lower()


class TestUUIDValidation:
    """Test UUID4 validation in forget/update."""

    def test_forget_rejects_invalid_uuid(self, mcp_server: SynapseMCPServer) -> None:
        """Forget should reject non-UUID4 IDs."""
        store = mcp_server._get_store()

        result = mcp_server._local_forget(store, {"id": "not-a-uuid"})
        assert "✗" in result
        assert "Invalid record ID" in result

    def test_forget_rejects_empty_id(self, mcp_server: SynapseMCPServer) -> None:
        """Forget should reject empty IDs."""
        store = mcp_server._get_store()

        result = mcp_server._local_forget(store, {"id": ""})
        assert "✗" in result
        assert "Invalid record ID" in result

    def test_forget_rejects_sql_injection_attempt(self, mcp_server: SynapseMCPServer) -> None:
        """Forget should reject SQL injection attempts."""
        store = mcp_server._get_store()

        result = mcp_server._local_forget(store, {"id": "'; DROP TABLE memories; --"})
        assert "✗" in result
        assert "Invalid record ID" in result

    def test_forget_accepts_valid_uuid4(self, mcp_server: SynapseMCPServer) -> None:
        """Forget should accept valid UUID4 format."""
        store = mcp_server._get_store()
        record = store.add("To forget")

        result = mcp_server._local_forget(store, {"id": record.id})
        assert "✓" in result
        assert "forgotten" in result

    def test_update_rejects_invalid_uuid(self, mcp_server: SynapseMCPServer) -> None:
        """Update should reject non-UUID4 IDs."""
        store = mcp_server._get_store()

        result = mcp_server._local_update(store, {
            "id": "invalid-id",
            "content": "new content",
        })
        assert "✗" in result
        assert "Invalid record ID" in result

    def test_update_accepts_valid_uuid4(self, mcp_server: SynapseMCPServer) -> None:
        """Update should accept valid UUID4 format."""
        store = mcp_server._get_store()
        record = store.add("Original")

        result = mcp_server._local_update(store, {
            "id": record.id,
            "content": "Updated content",
        })
        assert "✓" in result
        assert "updated" in result.lower()

    def test_uuid4_validation_rejects_uuid1(self, mcp_server: SynapseMCPServer) -> None:
        """Should reject UUID1 (not UUID4)."""
        store = mcp_server._get_store()
        uuid1 = str(uuid.uuid1())

        result = mcp_server._local_forget(store, {"id": uuid1})
        assert "✗" in result
        assert "Invalid record ID" in result

    def test_uuid4_validation_accepts_proper_uuid4(self) -> None:
        """Verify _is_valid_uuid4 works correctly."""
        valid = str(uuid.uuid4())
        assert SynapseMCPServer._is_valid_uuid4(valid) is True

        assert SynapseMCPServer._is_valid_uuid4("not-a-uuid") is False
        assert SynapseMCPServer._is_valid_uuid4("") is False
        assert SynapseMCPServer._is_valid_uuid4("12345678-1234-1234-1234-123456789012") is False
