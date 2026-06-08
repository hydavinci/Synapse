"""SynapseClient — async client for the Synapse Memory Protocol.

Supports gRPC (primary) and REST/HTTP (fallback) transports.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, Optional

import httpx

from .models import (
    ConflictPolicy,
    EventType,
    MemoryEvent,
    MemoryKind,
    MemoryRecord,
    Scope,
    SearchMode,
    SearchResult,
)
from .scope import parse_scope
from .utils import to_context as _to_context

logger = logging.getLogger(__name__)


class SynapseError(Exception):
    """Base exception for Synapse client errors."""

    pass


class ConnectionError(SynapseError):
    """Raised when the client cannot connect to the server."""

    pass


class NotFoundError(SynapseError):
    """Raised when a record is not found."""

    pass


class ConflictError(SynapseError):
    """Raised when a conflict is detected and policy is REJECT."""

    pass


class TransportMode:
    """Transport mode constants."""

    GRPC = "grpc"
    REST = "rest"
    AUTO = "auto"


class SynapseClient:
    """Async client for the Synapse Memory Protocol.

    Supports gRPC (primary) and REST/HTTP (fallback) transports.
    Uses asyncio for all operations.

    Args:
        endpoint: Server endpoint (host:port for gRPC, URL for REST)
        token: Authentication token (optional)
        default_scope: Default scope for operations when not specified
        transport: Transport mode - 'grpc', 'rest', or 'auto' (tries gRPC, falls back to REST)
        max_retries: Maximum number of retries for transient failures
        retry_delay: Base delay between retries in seconds (exponential backoff)
        timeout: Request timeout in seconds

    Example:
        >>> async with SynapseClient("localhost:9090") as client:
        ...     record = await client.add("Project deadline is July 15th", kind=MemoryKind.FACT)
        ...     results = await client.search("deadline")
    """

    def __init__(
        self,
        endpoint: str,
        *,
        token: Optional[str] = None,
        default_scope: Optional[Scope] = None,
        transport: str = TransportMode.AUTO,
        max_retries: int = 3,
        retry_delay: float = 0.5,
        timeout: float = 30.0,
    ) -> None:
        self._endpoint = endpoint
        self._token = token
        self._default_scope = default_scope or Scope()
        self._transport = transport
        self._max_retries = max_retries
        self._retry_delay = retry_delay
        self._timeout = timeout

        # HTTP client (lazy-initialized)
        self._http_client: Optional[httpx.AsyncClient] = None

        # gRPC channel (lazy-initialized)
        self._grpc_channel: Any = None
        self._grpc_stub: Any = None

        # Active transport mode
        self._active_transport: Optional[str] = None

        # Connection state
        self._connected = False

    async def connect(self) -> None:
        """Establish connection to the Synapse server.

        Tries gRPC first (if transport is 'grpc' or 'auto'), falls back to REST.
        """
        if self._connected:
            return

        if self._transport in (TransportMode.GRPC, TransportMode.AUTO):
            try:
                await self._connect_grpc()
                self._active_transport = TransportMode.GRPC
                self._connected = True
                logger.info("Connected via gRPC to %s", self._endpoint)
                return
            except Exception as e:
                if self._transport == TransportMode.GRPC:
                    raise ConnectionError(f"gRPC connection failed: {e}") from e
                logger.warning("gRPC unavailable, falling back to REST: %s", e)

        # REST fallback
        await self._connect_rest()
        self._active_transport = TransportMode.REST
        self._connected = True
        logger.info("Connected via REST to %s", self._endpoint)

    async def _connect_grpc(self) -> None:
        """Initialize gRPC connection."""
        try:
            import grpc
            import grpc.aio

            # Parse endpoint
            target = self._endpoint
            if not target.startswith("dns:") and "://" not in target:
                # Plain host:port
                pass

            channel_options = [
                ("grpc.max_receive_message_length", 50 * 1024 * 1024),
                ("grpc.keepalive_time_ms", 30000),
                ("grpc.keepalive_timeout_ms", 10000),
            ]

            self._grpc_channel = grpc.aio.insecure_channel(target, options=channel_options)

            # Test connectivity with a short deadline
            await asyncio.wait_for(
                self._grpc_channel.channel_ready(),
                timeout=5.0,
            )
        except ImportError as e:
            raise ConnectionError("grpcio not installed") from e
        except asyncio.TimeoutError as e:
            if self._grpc_channel:
                await self._grpc_channel.close()
                self._grpc_channel = None
            raise ConnectionError("gRPC connection timed out") from e

    async def _connect_rest(self) -> None:
        """Initialize REST/HTTP connection."""
        base_url = self._endpoint
        if not base_url.startswith("http"):
            base_url = f"http://{base_url}"

        headers: dict[str, str] = {}
        if self._token:
            headers["Authorization"] = f"Bearer {self._token}"

        self._http_client = httpx.AsyncClient(
            base_url=base_url,
            headers=headers,
            timeout=self._timeout,
        )

    async def close(self) -> None:
        """Close all connections and release resources."""
        if self._http_client:
            await self._http_client.aclose()
            self._http_client = None
        if self._grpc_channel:
            await self._grpc_channel.close()
            self._grpc_channel = None
        self._connected = False
        self._active_transport = None

    async def __aenter__(self) -> SynapseClient:
        await self.connect()
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.close()

    # === Core Operations ===

    async def add(
        self,
        content: str,
        *,
        scope: Optional[Scope] = None,
        kind: Optional[MemoryKind] = None,
        tags: Optional[list[str]] = None,
        confidence: float = 1.0,
        deduplicate: bool = True,
        on_conflict: Optional[ConflictPolicy] = None,
        expires_at: Optional[datetime] = None,
    ) -> MemoryRecord:
        """Store a new memory record.

        Args:
            content: The memory text content
            scope: Ownership scope (uses default if not provided)
            kind: Memory classification (auto-classified if not provided)
            tags: Optional labels
            confidence: Confidence score (0-1, default: 1.0)
            deduplicate: Check for near-duplicates before inserting
            on_conflict: Policy when conflicts are detected
            expires_at: Optional TTL timestamp

        Returns:
            The stored MemoryRecord

        Raises:
            ConflictError: If on_conflict is REJECT and a conflict is detected
        """
        effective_scope = scope or self._default_scope
        payload = {
            "content": content,
            "scope": effective_scope.model_dump(exclude_none=True),
            "tags": tags or [],
            "confidence": confidence,
            "deduplicate": deduplicate,
        }
        if kind:
            payload["kind"] = kind.value
        if on_conflict:
            payload["on_conflict"] = on_conflict.value
        if expires_at:
            payload["expires_at"] = expires_at.isoformat()

        response = await self._request("POST", "/v1/memories", json=payload)
        return MemoryRecord.model_validate(response)

    async def search(
        self,
        query: str,
        *,
        scope: Optional[Scope] = None,
        top_k: int = 10,
        min_score: float = 0.0,
        kinds: Optional[list[MemoryKind]] = None,
        tags: Optional[list[str]] = None,
        mode: SearchMode = SearchMode.HYBRID,
        time_range: Optional[tuple[datetime, datetime]] = None,
        agent_id: Optional[str] = None,
        include_expired: bool = False,
        rerank: bool = False,
    ) -> list[SearchResult]:
        """Search memories by semantic similarity.

        Args:
            query: Search query text
            scope: Scope filter (uses default if not provided)
            top_k: Maximum number of results
            min_score: Minimum similarity score threshold
            kinds: Filter by memory kinds
            tags: Filter by tags (AND logic)
            mode: Search strategy (semantic, keyword, hybrid, graph)
            time_range: Filter by time range (start, end)
            agent_id: Filter by source agent
            include_expired: Include expired records
            rerank: Apply LLM reranking to results

        Returns:
            List of SearchResult objects sorted by relevance
        """
        effective_scope = scope or self._default_scope
        payload: dict[str, Any] = {
            "query": query,
            "scope": effective_scope.model_dump(exclude_none=True),
            "top_k": top_k,
            "min_score": min_score,
            "mode": mode.value,
            "include_expired": include_expired,
            "rerank": rerank,
        }
        if kinds:
            payload["kinds"] = [k.value for k in kinds]
        if tags:
            payload["tags"] = tags
        if time_range:
            payload["time_range"] = {
                "start": time_range[0].isoformat(),
                "end": time_range[1].isoformat(),
            }
        if agent_id:
            payload["agent_id"] = agent_id

        response = await self._request("POST", "/v1/memories/search", json=payload)
        results_data = response.get("results", [])
        return [SearchResult.model_validate(r) for r in results_data]

    async def get(self, id: str) -> MemoryRecord:
        """Retrieve a specific memory record by ID.

        Args:
            id: The memory record ID (ULID)

        Returns:
            The MemoryRecord

        Raises:
            NotFoundError: If the record doesn't exist
        """
        response = await self._request("GET", f"/v1/memories/{id}")
        return MemoryRecord.model_validate(response)

    async def update(self, id: str, content: str, **kwargs: Any) -> MemoryRecord:
        """Update an existing memory record.

        Args:
            id: The memory record ID to update
            content: New content for the memory
            **kwargs: Additional fields to update (tags, kind, confidence, etc.)

        Returns:
            The updated MemoryRecord

        Raises:
            NotFoundError: If the record doesn't exist
        """
        payload: dict[str, Any] = {"content": content}
        payload.update(kwargs)
        response = await self._request("PATCH", f"/v1/memories/{id}", json=payload)
        return MemoryRecord.model_validate(response)

    async def forget(
        self,
        id: Optional[str] = None,
        *,
        scope: Optional[Scope] = None,
        before: Optional[datetime] = None,
    ) -> int:
        """Remove memory records.

        Can delete by specific ID, by scope, or by time range.
        At least one filter must be provided.

        Args:
            id: Specific memory ID to forget
            scope: Delete all records matching this scope
            before: Delete records created before this timestamp

        Returns:
            Number of records deleted

        Raises:
            ValueError: If no filter criteria provided
        """
        if not id and not scope and not before:
            raise ValueError("At least one of id, scope, or before must be provided")

        payload: dict[str, Any] = {}
        if id:
            payload["id"] = id
        if scope:
            payload["scope"] = scope.model_dump(exclude_none=True)
        if before:
            payload["before"] = before.isoformat()

        response = await self._request("POST", "/v1/memories/forget", json=payload)
        return response.get("deleted_count", 0)

    # === Real-time ===

    async def subscribe(
        self,
        scope: Optional[Scope] = None,
        event_types: Optional[list[EventType]] = None,
    ) -> AsyncIterator[MemoryEvent]:
        """Subscribe to real-time memory events.

        Returns an async iterator that yields MemoryEvent objects as they occur.

        Args:
            scope: Scope to watch (uses default if not provided)
            event_types: Filter by event types (all if not specified)

        Yields:
            MemoryEvent objects as they arrive

        Example:
            >>> async for event in client.subscribe(event_types=[EventType.MEMORY_ADDED]):
            ...     print(f"New memory: {event.record.content}")
        """
        effective_scope = scope or self._default_scope
        params: dict[str, Any] = {
            "scope": effective_scope.to_path(),
        }
        if event_types:
            params["event_types"] = [et.value for et in event_types]

        if self._active_transport == TransportMode.GRPC:
            async for event in self._subscribe_grpc(params):
                yield event
        else:
            async for event in self._subscribe_rest(params):
                yield event

    async def _subscribe_grpc(self, params: dict[str, Any]) -> AsyncIterator[MemoryEvent]:
        """gRPC streaming subscription."""
        # In a full implementation, this would use the gRPC streaming stub.
        # For now, raise if gRPC stub is not available.
        if not self._grpc_stub:
            raise SynapseError("gRPC subscription requires a connected stub")

        # Placeholder for gRPC server streaming call
        # response_stream = self._grpc_stub.Subscribe(subscribe_request)
        # async for event_proto in response_stream:
        #     yield MemoryEvent.model_validate(proto_to_dict(event_proto))
        raise NotImplementedError("gRPC subscribe not yet implemented — use REST transport")

    async def _subscribe_rest(self, params: dict[str, Any]) -> AsyncIterator[MemoryEvent]:
        """REST/SSE streaming subscription."""
        if not self._http_client:
            raise SynapseError("REST client not connected")

        import json

        async with self._http_client.stream(
            "GET", "/v1/events/subscribe", params=params
        ) as response:
            response.raise_for_status()
            async for line in response.aiter_lines():
                line = line.strip()
                if not line or line.startswith(":"):
                    continue
                if line.startswith("data:"):
                    data = line[5:].strip()
                    if data:
                        event_data = json.loads(data)
                        yield MemoryEvent.model_validate(event_data)

    # === Convenience ===

    def to_context(self, results: list[SearchResult], max_tokens: int = 2000) -> str:
        """Format search results as an LLM-friendly context string.

        Args:
            results: Search results to format
            max_tokens: Approximate maximum token count for output

        Returns:
            Formatted context string suitable for LLM consumption
        """
        return _to_context(results, max_tokens=max_tokens)

    # === Bulk Operations ===

    async def batch_add(self, records: list[dict[str, Any]]) -> list[MemoryRecord]:
        """Add multiple memory records in a single batch.

        Args:
            records: List of record dictionaries with fields matching AddRequest
                     (content, scope, kind, tags, confidence, etc.)

        Returns:
            List of stored MemoryRecords
        """
        # Ensure scopes are serialized
        processed: list[dict[str, Any]] = []
        for record in records:
            r = dict(record)
            if "scope" not in r:
                r["scope"] = self._default_scope.model_dump(exclude_none=True)
            elif isinstance(r["scope"], Scope):
                r["scope"] = r["scope"].model_dump(exclude_none=True)
            processed.append(r)

        response = await self._request("POST", "/v1/memories/batch", json={"records": processed})
        results_data = response.get("records", [])
        return [MemoryRecord.model_validate(r) for r in results_data]

    async def export_all(self, scope: Optional[Scope] = None) -> AsyncIterator[MemoryRecord]:
        """Export all memory records as an async iterator.

        Args:
            scope: Filter by scope (exports all if not specified)

        Yields:
            MemoryRecord objects
        """
        effective_scope = scope or self._default_scope
        params: dict[str, Any] = {}
        if effective_scope.to_path():
            params["scope"] = effective_scope.to_path()

        if self._active_transport == TransportMode.REST and self._http_client:
            import json

            async with self._http_client.stream(
                "GET", "/v1/memories/export", params=params
            ) as response:
                response.raise_for_status()
                async for line in response.aiter_lines():
                    line = line.strip()
                    if line:
                        record_data = json.loads(line)
                        yield MemoryRecord.model_validate(record_data)
        else:
            # Fallback: paginated GET
            offset = 0
            limit = 100
            while True:
                response = await self._request(
                    "GET",
                    "/v1/memories",
                    params={**params, "offset": offset, "limit": limit},
                )
                records_data = response.get("records", [])
                if not records_data:
                    break
                for r in records_data:
                    yield MemoryRecord.model_validate(r)
                if len(records_data) < limit:
                    break
                offset += limit

    # === Internal Transport ===

    async def _request(
        self,
        method: str,
        path: str,
        *,
        json: Optional[dict[str, Any]] = None,
        params: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        """Make an HTTP request with retry logic.

        Args:
            method: HTTP method
            path: URL path
            json: JSON body
            params: Query parameters

        Returns:
            Response JSON as dict

        Raises:
            SynapseError: On non-retryable errors
            ConnectionError: On connection failures
        """
        if not self._connected:
            await self.connect()

        last_error: Optional[Exception] = None
        for attempt in range(self._max_retries + 1):
            try:
                return await self._do_request(method, path, json=json, params=params)
            except httpx.HTTPStatusError as e:
                status = e.response.status_code
                if status == 404:
                    raise NotFoundError(f"Not found: {path}") from e
                if status == 409:
                    raise ConflictError(f"Conflict: {path}") from e
                if status < 500:
                    raise SynapseError(f"Client error {status}: {e.response.text}") from e
                # Server error — retry
                last_error = e
            except (httpx.ConnectError, httpx.TimeoutException) as e:
                last_error = e
            except Exception as e:
                raise SynapseError(f"Unexpected error: {e}") from e

            # Exponential backoff
            if attempt < self._max_retries:
                delay = self._retry_delay * (2**attempt)
                logger.warning(
                    "Request %s %s failed (attempt %d/%d), retrying in %.1fs: %s",
                    method, path, attempt + 1, self._max_retries + 1, delay, last_error,
                )
                await asyncio.sleep(delay)

        raise ConnectionError(
            f"Failed after {self._max_retries + 1} attempts: {last_error}"
        ) from last_error

    async def _do_request(
        self,
        method: str,
        path: str,
        *,
        json: Optional[dict[str, Any]] = None,
        params: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        """Execute a single HTTP request."""
        if not self._http_client:
            await self._connect_rest()

        assert self._http_client is not None
        response = await self._http_client.request(
            method, path, json=json, params=params
        )
        response.raise_for_status()

        if response.status_code == 204:
            return {}

        return response.json()  # type: ignore[no-any-return]
