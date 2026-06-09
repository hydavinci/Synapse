"""Local SQLite storage backend for Synapse.

Zero-dependency on external services. Stores memories in a local SQLite database
with optional numpy-based vector search. This is the default backend when no
remote endpoint is configured.
"""

from __future__ import annotations

import json
import sqlite3
import threading
import time
import uuid
from datetime import datetime, timezone
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
        self._lock = threading.RLock()  # CVE-14: RLock for read+write consistency
        self._conn = sqlite3.connect(str(self._db_path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA busy_timeout=5000")  # Wait up to 5s on contention
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
                scope_visibility TEXT DEFAULT 'private',
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
                tags TEXT DEFAULT '[]',
                kind TEXT DEFAULT 'fact',
                updated_at REAL NOT NULL,
                PRIMARY KEY (id, version)
            );

            CREATE INDEX IF NOT EXISTS idx_memories_scope
                ON memories(scope_org, scope_team, scope_user, scope_agent);
            CREATE INDEX IF NOT EXISTS idx_memories_kind ON memories(kind);
            CREATE INDEX IF NOT EXISTS idx_memories_created ON memories(created_at);

            -- FTS5 full-text search index (content + tags)
            CREATE VIRTUAL TABLE IF NOT EXISTS memories_fts USING fts5(
                content,
                tags,
                content='memories',
                content_rowid='rowid'
            );

            -- Triggers to keep FTS index in sync
            CREATE TRIGGER IF NOT EXISTS memories_ai AFTER INSERT ON memories BEGIN
                INSERT INTO memories_fts(rowid, content, tags)
                VALUES (new.rowid, new.content, new.tags);
            END;
            CREATE TRIGGER IF NOT EXISTS memories_ad AFTER DELETE ON memories BEGIN
                INSERT INTO memories_fts(memories_fts, rowid, content, tags)
                VALUES ('delete', old.rowid, old.content, old.tags);
            END;
            CREATE TRIGGER IF NOT EXISTS memories_au AFTER UPDATE ON memories BEGIN
                INSERT INTO memories_fts(memories_fts, rowid, content, tags)
                VALUES ('delete', old.rowid, old.content, old.tags);
                INSERT INTO memories_fts(rowid, content, tags)
                VALUES (new.rowid, new.content, new.tags);
            END;
        """)
        self._conn.commit()

        # Rebuild FTS index if table was created before FTS was added
        self._rebuild_fts_if_needed()

    def _rebuild_fts_if_needed(self) -> None:
        """Rebuild FTS index on first run after upgrade."""
        row = self._conn.execute(
            "SELECT COUNT(*) as cnt FROM memories_fts"
        ).fetchone()
        fts_count = row["cnt"] if row else 0
        row2 = self._conn.execute(
            "SELECT COUNT(*) as cnt FROM memories"
        ).fetchone()
        mem_count = row2["cnt"] if row2 else 0
        if mem_count > 0 and fts_count == 0:
            self._conn.execute(
                "INSERT INTO memories_fts(memories_fts) VALUES('rebuild')"
            )
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
        now_ts = time.time()
        now = datetime.fromtimestamp(now_ts, tz=timezone.utc)
        scope = scope or Scope()
        kind = kind or MemoryKind.FACT
        tags = tags or []

        # Compute embedding if function provided
        embedding: Optional[list[float]] = None
        embedding_blob: Optional[bytes] = None
        if self._embedding_fn:
            embedding = self._embedding_fn(content)
            embedding_blob = self._encode_embedding(embedding)

        with self._lock:
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
                    now_ts,
                    now_ts,
                    now_ts,
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
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO memory_history (id, version, content, tags, kind, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
                (record_id, existing.version, existing.content,
                 json.dumps(existing.tags), existing.kind.value,
                 existing.updated_at.timestamp() if existing.updated_at else time.time()),
            )

            now_ts = time.time()
            now = datetime.fromtimestamp(now_ts, tz=timezone.utc)
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
                     new_confidence, new_version, now_ts, record_id),
                )
            else:
                self._conn.execute(
                    """UPDATE memories SET content=?, tags=?, kind=?,
                       confidence=?, version=?, updated_at=? WHERE id=?""",
                    (new_content, json.dumps(new_tags), new_kind.value,
                     new_confidence, new_version, now_ts, record_id),
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
        with self._lock:
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
        """Cosine similarity search over stored embeddings.

        Optimized: only loads id+embedding for scoring, then batch-fetches full records
        for top-K hits. Processes in batches to avoid OOM on large corpora.
        """
        import numpy as np

        query_vec = np.array(query_embedding, dtype=np.float32)
        query_norm = np.linalg.norm(query_vec)
        if query_norm == 0:
            return []
        query_vec = query_vec / query_norm

        # Phase 1: Score only id + embedding (lightweight)
        BATCH_SIZE = 1000
        cursor = self._conn.execute(
            f"SELECT id, embedding FROM memories WHERE {where} AND embedding IS NOT NULL",
            params,
        )

        scored: list[tuple[float, str]] = []
        while True:
            rows = cursor.fetchmany(BATCH_SIZE)
            if not rows:
                break

            for row in rows:
                embedding_blob = row["embedding"]
                if not embedding_blob:
                    continue

                stored_vec = np.frombuffer(embedding_blob, dtype=np.float32)
                stored_norm = np.linalg.norm(stored_vec)
                if stored_norm == 0:
                    continue

                score = float(np.dot(query_vec, stored_vec / stored_norm))

                if score >= min_score:
                    if len(scored) < top_k * 3:  # Over-fetch for tag filtering
                        scored.append((score, row["id"]))
                    elif score > scored[-1][0]:
                        scored.append((score, row["id"]))
                        scored.sort(key=lambda x: x[0], reverse=True)
                        scored = scored[:top_k * 3]

        # Sort candidates by score
        scored.sort(key=lambda x: x[0], reverse=True)

        # Phase 2: Batch-fetch full records for candidates
        candidate_ids = [rid for _, rid in scored]
        if not candidate_ids:
            return []

        placeholders = ",".join("?" * len(candidate_ids))
        full_rows = self._conn.execute(
            f"SELECT * FROM memories WHERE id IN ({placeholders})",
            candidate_ids,
        ).fetchall()

        # Build id->row map
        row_map = {row["id"]: row for row in full_rows}

        # Phase 3: Apply tag filter and build results
        results = []
        for score, rid in scored:
            if rid not in row_map:
                continue
            row = row_map[rid]

            # Tag filter
            if tags:
                record_tags = json.loads(row["tags"])
                if not all(t in record_tags for t in tags):
                    continue

            record = self._row_to_record(row)
            results.append(SearchResult(record=record, score=score))
            if len(results) >= top_k:
                break

        return results

    def _keyword_search(
        self,
        query: str,
        where: str,
        params: list,
        top_k: int,
        tags: Optional[list[str]],
    ) -> list[SearchResult]:
        """FTS5-accelerated keyword search with fallback to LIKE scan."""
        # Try FTS5 first (much faster for large datasets)
        try:
            return self._fts5_search(query, where, params, top_k, tags)
        except Exception:
            # Fall back to brute-force scan if FTS5 unavailable
            return self._fallback_keyword_search(query, where, params, top_k, tags)

    def _fts5_search(
        self,
        query: str,
        where: str,
        params: list,
        top_k: int,
        tags: Optional[list[str]],
    ) -> list[SearchResult]:
        """Use FTS5 index for fast full-text keyword search."""
        # Escape FTS5 special characters and build OR query
        terms = query.split()
        if not terms:
            return []
        # Quote each term to avoid FTS5 syntax errors
        fts_query = " OR ".join(f'"{t}"' for t in terms[:20])  # cap at 20 terms

        # Join FTS results with the memories table for scope filtering
        sql = f"""SELECT m.*, rank AS fts_rank
                  FROM memories_fts fts
                  JOIN memories m ON m.rowid = fts.rowid
                  WHERE memories_fts MATCH ?
                    AND {where}
                  ORDER BY fts_rank
                  LIMIT ?"""
        rows = self._conn.execute(sql, [fts_query] + params + [top_k * 3]).fetchall()

        results: list[SearchResult] = []
        for row in rows:
            # Tag filter
            if tags:
                record_tags = json.loads(row["tags"])
                if not all(t in record_tags for t in tags):
                    continue

            record = self._row_to_record(row)
            # FTS5 rank is negative (lower = better); normalize to 0..1
            fts_rank = abs(row["fts_rank"]) if row["fts_rank"] else 0
            score = 1.0 / (1.0 + fts_rank)  # sigmoid-like normalization
            results.append(SearchResult(record=record, score=score))
            if len(results) >= top_k:
                break

        return results

    def _fallback_keyword_search(
        self,
        query: str,
        where: str,
        params: list,
        top_k: int,
        tags: Optional[list[str]],
    ) -> list[SearchResult]:
        """Brute-force keyword matching (fallback when FTS5 unavailable)."""
        rows = self._conn.execute(
            f"SELECT * FROM memories WHERE {where} ORDER BY updated_at DESC LIMIT 200",
            params,
        ).fetchall()

        query_lower = query.lower()
        query_terms = query_lower.split()

        scored: list[tuple[float, sqlite3.Row]] = []
        for row in rows:
            content: str = row["content"]
            content_lower = content.lower()

            # Tag filter
            if tags:
                record_tags = json.loads(row["tags"])
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
            id=row["id"],
            content=row["content"],
            scope=Scope(
                org=row["scope_org"] or None,
                team=row["scope_team"] or None,
                user=row["scope_user"] or None,
                agent=row["scope_agent"] or None,
                visibility=Visibility(row["scope_visibility"]) if row["scope_visibility"] else Visibility.PRIVATE,
            ),
            kind=MemoryKind(row["kind"]),
            confidence=row["confidence"],
            tags=json.loads(row["tags"]),
            version=row["version"],
            created_at=datetime.fromtimestamp(row["created_at"], tz=timezone.utc) if row["created_at"] else None,
            updated_at=datetime.fromtimestamp(row["updated_at"], tz=timezone.utc) if row["updated_at"] else None,
        )

    @staticmethod
    def _encode_embedding(embedding: list[float]) -> bytes:
        """Encode embedding list to bytes for storage."""
        import numpy as np
        return np.array(embedding, dtype=np.float32).tobytes()

    def start_cleanup_scheduler(self, interval_seconds: int = 3600) -> None:
        """Start a background thread that periodically cleans up expired memories.

        Args:
            interval_seconds: How often to run cleanup (default: 1 hour)
        """
        import logging
        logger = logging.getLogger(__name__)

        def _cleanup_loop():
            while not self._cleanup_stop_event.is_set():
                self._cleanup_stop_event.wait(interval_seconds)
                if self._cleanup_stop_event.is_set():
                    break
                try:
                    deleted = self.cleanup_expired()
                    if deleted > 0:
                        logger.info("Cleaned up %d expired memories", deleted)
                except Exception as e:
                    logger.warning("Cleanup failed: %s", e)

        self._cleanup_stop_event = threading.Event()
        self._cleanup_thread = threading.Thread(
            target=_cleanup_loop, daemon=True, name="synapse-cleanup"
        )
        self._cleanup_thread.start()

    def close(self) -> None:
        """Close the database connection and stop background tasks."""
        if hasattr(self, '_cleanup_stop_event'):
            self._cleanup_stop_event.set()
            self._cleanup_thread.join(timeout=2)
        self._conn.close()

    def cleanup_expired(self) -> int:
        """Remove expired memories (where expires_at < now). Returns count deleted."""
        now = time.time()
        with self._lock:
            cursor = self._conn.execute(
                "DELETE FROM memories WHERE expires_at IS NOT NULL AND expires_at < ?",
                (now,),
            )
            self._conn.commit()
        return cursor.rowcount
