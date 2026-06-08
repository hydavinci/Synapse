"""Local SQLite storage backend for Synapse.

Zero-dependency on external services. Stores memories in a local SQLite database
with optional numpy-based vector search. This is the default backend when no
remote endpoint is configured.
"""

from __future__ import annotations

import json
import sqlite3
import time
import uuid
from pathlib import Path
from typing import Optional

from .models import MemoryKind, MemoryRecord, Scope, SearchResult, Visibility


def _default_db_path() -> Path:
    """Default database path: ~/.synapse/memories.db"""
    p = Path.home() / ".synapse"
    p.mkdir(parents=True, exist_ok=True)
    return p / "memories.db"


class LocalStore:
    """SQLite-based local memory store with embedded vector search.

    Args:
        db_path: Path to SQLite database file. Defaults to ~/.synapse/memories.db
        embedding_fn: Optional function that takes text and returns a float list (embedding).
                      If None, vector search is disabled and only keyword/tag search works.
    """

    def __init__(
        self,
        db_path: Optional[Path] = None,
        embedding_fn=None,
    ) -> None:
        self._db_path = db_path or _default_db_path()
        self._embedding_fn = embedding_fn
        self._conn = sqlite3.connect(str(self._db_path), check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._init_schema()

    def _init_schema(self) -> None:
        self._conn.executescript("""
            CREATE TABLE IF NOT EXISTS memories (
                id TEXT PRIMARY KEY,
                content TEXT NOT NULL,
                embedding BLOB,
                scope_org TEXT DEFAULT '',
                scope_team TEXT DEFAULT '',
                scope_user TEXT DEFAULT '',
                scope_agent TEXT DEFAULT '',
                scope_visibility INTEGER DEFAULT 1,
                kind TEXT DEFAULT 'fact',
                confidence REAL DEFAULT 1.0,
                tags TEXT DEFAULT '[]',
                source TEXT DEFAULT '',
                version INTEGER DEFAULT 1,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                accessed_at REAL NOT NULL,
                expires_at REAL,
                metadata TEXT DEFAULT '{}'
            );

            CREATE TABLE IF NOT EXISTS memory_history (
                id TEXT,
                version INTEGER,
                content TEXT NOT NULL,
                updated_at REAL NOT NULL,
                PRIMARY KEY (id, version)
            );

            CREATE INDEX IF NOT EXISTS idx_memories_scope
                ON memories(scope_org, scope_team, scope_user, scope_agent);
            CREATE INDEX IF NOT EXISTS idx_memories_kind ON memories(kind);
            CREATE INDEX IF NOT EXISTS idx_memories_created ON memories(created_at);
        """)
        self._conn.commit()

    def add(
        self,
        content: str,
        scope: Optional[Scope] = None,
        kind: Optional[MemoryKind] = None,
        tags: Optional[list[str]] = None,
        confidence: float = 1.0,
        metadata: Optional[dict] = None,
    ) -> MemoryRecord:
        """Store a new memory record."""
        record_id = str(uuid.uuid4())
        now = time.time()
        scope = scope or Scope()
        kind = kind or MemoryKind.FACT
        tags = tags or []

        # Compute embedding if function provided
        embedding: Optional[list[float]] = None
        embedding_blob: Optional[bytes] = None
        if self._embedding_fn:
            embedding = self._embedding_fn(content)
            embedding_blob = self._encode_embedding(embedding)

        self._conn.execute(
            """INSERT INTO memories
               (id, content, embedding, scope_org, scope_team, scope_user, scope_agent,
                scope_visibility, kind, confidence, tags, source, version,
                created_at, updated_at, accessed_at, expires_at, metadata)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                record_id,
                content,
                embedding_blob,
                scope.org or "",
                scope.team or "",
                scope.user or "",
                scope.agent or "",
                scope.visibility.value if scope.visibility else Visibility.PRIVATE.value,
                kind.value,
                confidence,
                json.dumps(tags),
                "",
                1,
                now,
                now,
                now,
                None,
                json.dumps(metadata or {}),
            ),
        )
        self._conn.commit()

        return MemoryRecord(
            id=record_id,
            content=content,
            scope=scope,
            kind=kind,
            confidence=confidence,
            tags=tags,
            version=1,
            created_at=now,
            updated_at=now,
        )

    def get(self, record_id: str) -> Optional[MemoryRecord]:
        """Get a single record by ID."""
        row = self._conn.execute(
            "SELECT * FROM memories WHERE id = ?", (record_id,)
        ).fetchone()
        if not row:
            return None

        # Update accessed_at
        self._conn.execute(
            "UPDATE memories SET accessed_at = ? WHERE id = ?",
            (time.time(), record_id),
        )
        self._conn.commit()
        return self._row_to_record(row)

    def update(self, record_id: str, content: Optional[str] = None,
               tags: Optional[list[str]] = None, kind: Optional[MemoryKind] = None,
               confidence: Optional[float] = None) -> Optional[MemoryRecord]:
        """Update an existing record. Saves previous version to history."""
        existing = self.get(record_id)
        if not existing:
            return None

        # Save to history
        self._conn.execute(
            "INSERT OR REPLACE INTO memory_history (id, version, content, updated_at) VALUES (?, ?, ?, ?)",
            (record_id, existing.version, existing.content, existing.updated_at),
        )

        now = time.time()
        new_content = content if content is not None else existing.content
        new_tags = tags if tags is not None else existing.tags
        new_kind = kind if kind is not None else existing.kind
        new_confidence = confidence if confidence is not None else existing.confidence
        new_version = existing.version + 1

        # Recompute embedding if content changed
        embedding_blob = None
        if content is not None and self._embedding_fn:
            embedding = self._embedding_fn(new_content)
            embedding_blob = self._encode_embedding(embedding)

        if embedding_blob:
            self._conn.execute(
                """UPDATE memories SET content=?, embedding=?, tags=?, kind=?,
                   confidence=?, version=?, updated_at=? WHERE id=?""",
                (new_content, embedding_blob, json.dumps(new_tags), new_kind.value,
                 new_confidence, new_version, now, record_id),
            )
        else:
            self._conn.execute(
                """UPDATE memories SET content=?, tags=?, kind=?,
                   confidence=?, version=?, updated_at=? WHERE id=?""",
                (new_content, json.dumps(new_tags), new_kind.value,
                 new_confidence, new_version, now, record_id),
            )
        self._conn.commit()

        return MemoryRecord(
            id=record_id,
            content=new_content,
            scope=existing.scope,
            kind=new_kind,
            confidence=new_confidence,
            tags=new_tags,
            version=new_version,
            created_at=existing.created_at,
            updated_at=now,
        )

    def forget(self, record_id: str) -> bool:
        """Delete a record by ID."""
        cursor = self._conn.execute("DELETE FROM memories WHERE id = ?", (record_id,))
        self._conn.commit()
        return cursor.rowcount > 0

    def search(
        self,
        query: str,
        scope: Optional[Scope] = None,
        top_k: int = 5,
        min_score: float = 0.0,
        kinds: Optional[list[MemoryKind]] = None,
        tags: Optional[list[str]] = None,
    ) -> list[SearchResult]:
        """Search memories by semantic similarity (if embeddings available) or keyword match."""

        # Build base query with filters
        conditions = ["1=1"]
        params: list = []

        if scope:
            if scope.org:
                conditions.append("scope_org = ?")
                params.append(scope.org)
            if scope.team:
                conditions.append("scope_team = ?")
                params.append(scope.team)
            if scope.user:
                conditions.append("scope_user = ?")
                params.append(scope.user)
            if scope.agent:
                conditions.append("scope_agent = ?")
                params.append(scope.agent)

        if kinds:
            placeholders = ",".join("?" * len(kinds))
            conditions.append(f"kind IN ({placeholders})")
            params.extend(k.value for k in kinds)

        where = " AND ".join(conditions)

        # If we have an embedding function, do vector search
        if self._embedding_fn:
            query_embedding = self._embedding_fn(query)
            return self._vector_search(query_embedding, where, params, top_k, min_score, tags)

        # Fallback: keyword search
        return self._keyword_search(query, where, params, top_k, tags)

    def list_memories(
        self,
        scope: Optional[Scope] = None,
        limit: int = 50,
        offset: int = 0,
        kinds: Optional[list[MemoryKind]] = None,
    ) -> list[MemoryRecord]:
        """List memories with optional scope/kind filtering."""
        conditions = ["1=1"]
        params: list = []

        if scope:
            if scope.org:
                conditions.append("scope_org = ?")
                params.append(scope.org)
            if scope.team:
                conditions.append("scope_team = ?")
                params.append(scope.team)
            if scope.user:
                conditions.append("scope_user = ?")
                params.append(scope.user)

        if kinds:
            placeholders = ",".join("?" * len(kinds))
            conditions.append(f"kind IN ({placeholders})")
            params.extend(k.value for k in kinds)

        where = " AND ".join(conditions)
        params.extend([limit, offset])

        rows = self._conn.execute(
            f"SELECT * FROM memories WHERE {where} ORDER BY updated_at DESC LIMIT ? OFFSET ?",
            params,
        ).fetchall()

        return [self._row_to_record(row) for row in rows]

    def count(self, scope: Optional[Scope] = None) -> int:
        """Count total records, optionally filtered by scope."""
        if scope:
            conditions = []
            params = []
            if scope.org:
                conditions.append("scope_org = ?")
                params.append(scope.org)
            if scope.team:
                conditions.append("scope_team = ?")
                params.append(scope.team)
            if scope.user:
                conditions.append("scope_user = ?")
                params.append(scope.user)
            where = " AND ".join(conditions) if conditions else "1=1"
            row = self._conn.execute(
                f"SELECT COUNT(*) FROM memories WHERE {where}", params
            ).fetchone()
        else:
            row = self._conn.execute("SELECT COUNT(*) FROM memories").fetchone()
        return row[0] if row else 0

    def _vector_search(
        self,
        query_embedding: list[float],
        where: str,
        params: list,
        top_k: int,
        min_score: float,
        tags: Optional[list[str]],
    ) -> list[SearchResult]:
        """Cosine similarity search over stored embeddings."""
        import numpy as np

        query_vec = np.array(query_embedding, dtype=np.float32)
        query_norm = np.linalg.norm(query_vec)
        if query_norm == 0:
            return []
        query_vec = query_vec / query_norm

        rows = self._conn.execute(
            f"SELECT * FROM memories WHERE {where} AND embedding IS NOT NULL",
            params,
        ).fetchall()

        scored: list[tuple[float, sqlite3.Row]] = []
        for row in rows:
            embedding_blob = row[2]  # embedding column
            if not embedding_blob:
                continue

            stored_vec = np.frombuffer(embedding_blob, dtype=np.float32)
            stored_norm = np.linalg.norm(stored_vec)
            if stored_norm == 0:
                continue

            score = float(np.dot(query_vec, stored_vec / stored_norm))

            if score >= min_score:
                # Tag filter
                if tags:
                    record_tags = json.loads(row[10])  # tags column
                    if not all(t in record_tags for t in tags):
                        continue
                scored.append((score, row))

        # Sort by score descending
        scored.sort(key=lambda x: x[0], reverse=True)

        results = []
        for score, row in scored[:top_k]:
            record = self._row_to_record(row)
            results.append(SearchResult(record=record, score=score))

        return results

    def _keyword_search(
        self,
        query: str,
        where: str,
        params: list,
        top_k: int,
        tags: Optional[list[str]],
    ) -> list[SearchResult]:
        """Simple keyword-based search fallback."""
        rows = self._conn.execute(
            f"SELECT * FROM memories WHERE {where} ORDER BY updated_at DESC LIMIT 200",
            params,
        ).fetchall()

        query_lower = query.lower()
        query_terms = query_lower.split()

        scored: list[tuple[float, sqlite3.Row]] = []
        for row in rows:
            content: str = row[1]  # content column
            content_lower = content.lower()

            # Tag filter
            if tags:
                record_tags = json.loads(row[10])
                if not all(t in record_tags for t in tags):
                    continue

            # Simple scoring: fraction of query terms found in content
            matches = sum(1 for term in query_terms if term in content_lower)
            if matches > 0:
                score = matches / len(query_terms)
                scored.append((score, row))

        scored.sort(key=lambda x: x[0], reverse=True)

        results = []
        for score, row in scored[:top_k]:
            record = self._row_to_record(row)
            results.append(SearchResult(record=record, score=score))

        return results

    def _row_to_record(self, row) -> MemoryRecord:
        """Convert a database row to a MemoryRecord."""
        return MemoryRecord(
            id=row[0],
            content=row[1],
            scope=Scope(
                org=row[3] or None,
                team=row[4] or None,
                user=row[5] or None,
                agent=row[6] or None,
                visibility=Visibility(row[7]) if row[7] else None,
            ),
            kind=MemoryKind(row[8]),
            confidence=row[9],
            tags=json.loads(row[10]),
            version=row[12],
            created_at=row[13],
            updated_at=row[14],
        )

    @staticmethod
    def _encode_embedding(embedding: list[float]) -> bytes:
        """Encode embedding list to bytes for storage."""
        import numpy as np
        return np.array(embedding, dtype=np.float32).tobytes()

    def close(self) -> None:
        """Close the database connection."""
        self._conn.close()
