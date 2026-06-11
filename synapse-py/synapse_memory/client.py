"""SynapseClient — async client for the Synapse Memory Protocol.

Supports gRPC (primary) and REST/HTTP (fallback) transports.

Usage:
    # gRPC (recommended for production / distributed mode):
    async with SynapseClient("localhost:9090") as client:
        record = await client.add("Project deadline is July 15th")
        results = await client.search("deadline")

    # REST (for environments where gRPC isn't available):
    async with SynapseClient("http://localhost:9091", transport="rest") as client:
        ...
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import AsyncIterator
from datetime import datetime
from enum import Enum
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


class CircuitState(str, Enum):
    """Circuit breaker states."""
    CLOSED = "closed"      # Normal operation, requests flow through
    OPEN = "open"          # Circuit tripped, fail fast
    HALF_OPEN = "half_open"  # Testing if service is back


class CircuitBreaker:
    """Circuit breaker pattern for fault tolerance.

    States:
    - CLOSED: Normal operation. Tracks consecutive failures.
    - OPEN: Fail fast. After timeout, moves to HALF_OPEN.
    - HALF_OPEN: Allows one probe request. Success -> CLOSED, failure -> OPEN.

    Args:
        failure_threshold: Number of consecutive failures before opening circuit.
        recovery_timeout: Seconds to wait before moving from OPEN to HALF_OPEN.
    """

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 30.0,
    ) -> None:
        self._failure_threshold = failure_threshold
        self._recovery_timeout = recovery_timeout
        self._state = CircuitState.CLOSED
        self._consecutive_failures = 0
        self._last_failure_time: float = 0.0
        self._lock = asyncio.Lock()

    @property
    def state(self) -> CircuitState:
        """Get current circuit state, considering time-based transitions."""
        if self._state == CircuitState.OPEN:
            # Check if recovery timeout has elapsed
            if time.time() - self._last_failure_time >= self._recovery_timeout:
                return CircuitState.HALF_OPEN
        return self._state

    @property
    def consecutive_failures(self) -> int:
        return self._consecutive_failures

    async def allow_request(self) -> bool:
        """Check if a request is allowed through the circuit.

        Returns:
            True if the request should proceed, False if it should fail fast.
        """
        async with self._lock:
            current_state = self.state
            if current_state == CircuitState.CLOSED:
                return True
            elif current_state == CircuitState.HALF_OPEN:
                # Allow one probe request
                return True
            else:
                # OPEN — fail fast
                return False

    async def record_success(self) -> None:
        """Record a successful request."""
        async with self._lock:
            self._consecutive_failures = 0
            self._state = CircuitState.CLOSED

    async def record_failure(self) -> None:
        """Record a failed request."""
        async with self._lock:
            self._consecutive_failures += 1
            self._last_failure_time = time.time()
            if self._state == CircuitState.HALF_OPEN:
                # Probe failed, reopen
                self._state = CircuitState.OPEN
            elif self._consecutive_failures >= self._failure_threshold:
                self._state = CircuitState.OPEN

    async def reset(self) -> None:
        """Reset the circuit breaker to CLOSED state."""
        async with self._lock:
            self._state = CircuitState.CLOSED
            self._consecutive_failures = 0
            self._last_failure_time = 0.0


class TransportMode:
    """Transport mode constants."""
    GRPC = "grpc"
    REST = "rest"
    AUTO = "auto"


class SynapseClient:
    """Async client for the Synapse Memory Protocol.

    Supports gRPC (primary) and REST/HTTP (fallback) transports.

    Args:
        endpoint: Server endpoint (host:port for gRPC, URL for REST)
        token: Authentication token (optional)
        default_scope: Default scope for operations when not specified
        transport: Transport mode - 'grpc', 'rest', or 'auto' (tries gRPC, falls back to REST)
        max_retries: Maximum retries for transient failures
        retry_delay: Base delay between retries (exponential backoff)
        timeout: Request timeout in seconds
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

        # HTTP client (lazy)
        self._http_client: Optional[httpx.AsyncClient] = None

        # gRPC (lazy)
        self._grpc_channel: Any = None
        self._grpc_stub: Any = None

        # Active transport mode
        self._active_transport: Optional[str] = None
        self._connected = False

        # Circuit breaker for gRPC transport
        self._circuit_breaker = CircuitBreaker(failure_threshold=5, recovery_timeout=30.0)

        # gRPC health probe task
        self._grpc_probe_task: Optional[asyncio.Task] = None
        self._probe_interval = 60.0  # Probe gRPC health every 60s when using REST fallback

    async def connect(self) -> None:
        """Establish connection to the Synapse server."""
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
        """Initialize gRPC connection and stub."""
        try:
            import grpc
            import grpc.aio
        except ImportError as e:
            raise ConnectionError("grpcio not installed. Install with: pip install grpcio") from e

        target = self._endpoint
        channel_options = [
            ("grpc.max_receive_message_length", 50 * 1024 * 1024),
            ("grpc.keepalive_time_ms", 30000),
            ("grpc.keepalive_timeout_ms", 10000),
        ]

        self._grpc_channel = grpc.aio.insecure_channel(target, options=channel_options)

        try:
            await asyncio.wait_for(
                self._grpc_channel.channel_ready(),
                timeout=5.0,
            )
        except asyncio.TimeoutError as e:
            await self._grpc_channel.close()
            self._grpc_channel = None
            raise ConnectionError("gRPC connection timed out") from e

        # Import the generated stub — this needs proto compilation.
        # For now we use a dynamic unary call approach.
        # In a full build, you'd import from synapse_pb2_grpc.
        self._grpc_stub = self._grpc_channel

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
        """Close all connections."""
        if self._grpc_probe_task and not self._grpc_probe_task.done():
            self._grpc_probe_task.cancel()
            try:
                await self._grpc_probe_task
            except asyncio.CancelledError:
                pass
            self._grpc_probe_task = None
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
        """Store a new memory record."""
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
    ) -> list[SearchResult]:
        """Search memories by semantic similarity."""
        effective_scope = scope or self._default_scope
        payload: dict[str, Any] = {
            "query": query,
            "scope": effective_scope.model_dump(exclude_none=True),
            "top_k": top_k,
            "min_score": min_score,
            "mode": mode.value,
        }
        if kinds:
            payload["kinds"] = [k.value for k in kinds]
        if tags:
            payload["tags"] = tags

        response = await self._request("POST", "/v1/memories/search", json=payload)
        results_data = response.get("results", [])
        return [SearchResult.model_validate(r) for r in results_data]

    async def get(self, id: str) -> MemoryRecord:
        """Retrieve a specific memory record by ID."""
        response = await self._request("GET", f"/v1/memories/{id}")
        return MemoryRecord.model_validate(response)

    async def update(self, id: str, content: str, **kwargs: Any) -> MemoryRecord:
        """Update an existing memory record."""
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
        """Remove memory records."""
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

    async def batch_add(self, records: list[dict[str, Any]]) -> list[MemoryRecord]:
        """Add multiple memory records in a single batch."""
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

    # === Convenience ===

    def to_context(self, results: list[SearchResult], max_tokens: int = 2000) -> str:
        """Format search results as an LLM-friendly context string."""
        return _to_context(results, max_tokens=max_tokens)

    # === Internal Transport ===

    async def _request(
        self,
        method: str,
        path: str,
        *,
        json: Optional[dict[str, Any]] = None,
        params: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        """Route request to the active transport with retry logic and circuit breaker."""
        if not self._connected:
            await self.connect()

        last_error: Optional[Exception] = None
        for attempt in range(self._max_retries + 1):
            try:
                # Determine transport: check circuit breaker for gRPC
                use_transport = self._active_transport
                if use_transport == TransportMode.GRPC:
                    if not await self._circuit_breaker.allow_request():
                        # Circuit is open — fall back to REST
                        logger.warning("Circuit breaker OPEN for gRPC, falling back to REST")
                        await self._ensure_rest_fallback()
                        use_transport = TransportMode.REST

                if use_transport == TransportMode.GRPC:
                    result = await self._grpc_request(method, path, json=json, params=params)
                    await self._circuit_breaker.record_success()
                    return result
                else:
                    return await self._rest_request(method, path, json=json, params=params)
            except (httpx.HTTPStatusError,) as e:
                status = e.response.status_code
                if status == 404:
                    raise NotFoundError(f"Not found: {path}") from e
                if status == 409:
                    raise ConflictError(f"Conflict: {path}") from e
                if status < 500:
                    raise SynapseError(f"Client error {status}: {e.response.text}") from e
                last_error = e
            except (httpx.ConnectError, httpx.TimeoutException) as e:
                last_error = e
                # Record failure for circuit breaker if using gRPC
                if self._active_transport == TransportMode.GRPC:
                    await self._circuit_breaker.record_failure()
                    # If circuit just opened, try REST fallback immediately
                    if self._circuit_breaker.state == CircuitState.OPEN:
                        logger.warning("Circuit breaker tripped, switching to REST fallback")
                        await self._ensure_rest_fallback()
                        # Retry this attempt on REST
                        try:
                            return await self._rest_request(method, path, json=json, params=params)
                        except Exception as rest_e:
                            last_error = rest_e
            except Exception as e:
                if "StatusCode.NOT_FOUND" in str(e):
                    raise NotFoundError(f"Not found: {path}") from e
                # Record gRPC failures
                if self._active_transport == TransportMode.GRPC:
                    await self._circuit_breaker.record_failure()
                raise SynapseError(f"Unexpected error: {e}") from e

            if attempt < self._max_retries:
                delay = self._retry_delay * (2 ** attempt)
                logger.warning(
                    "Request %s %s failed (attempt %d/%d), retrying in %.1fs",
                    method, path, attempt + 1, self._max_retries + 1, delay,
                )
                await asyncio.sleep(delay)

        raise ConnectionError(
            f"Failed after {self._max_retries + 1} attempts: {last_error}"
        ) from last_error

    async def _ensure_rest_fallback(self) -> None:
        """Ensure REST client is available for fallback and start gRPC probe."""
        if not self._http_client:
            await self._connect_rest()
        # Start periodic gRPC probe if not already running
        if self._grpc_probe_task is None or self._grpc_probe_task.done():
            self._grpc_probe_task = asyncio.create_task(self._probe_grpc_health())

    async def _probe_grpc_health(self) -> None:
        """Periodically probe gRPC health. If available, switch back and close circuit."""
        while True:
            await asyncio.sleep(self._probe_interval)
            try:
                # Try to re-establish gRPC
                if self._grpc_channel:
                    await self._grpc_channel.close()
                    self._grpc_channel = None
                await self._connect_grpc()
                # gRPC is back!
                await self._circuit_breaker.reset()
                self._active_transport = TransportMode.GRPC
                logger.info("gRPC health probe succeeded, switching back to gRPC")
                return  # Stop probing
            except Exception:
                logger.debug("gRPC health probe failed, staying on REST")
                continue

    async def _grpc_request(
        self,
        method: str,
        path: str,
        *,
        json: Optional[dict[str, Any]] = None,
        params: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        """Make a gRPC call mapped from the REST-like interface.

        NOTE: gRPC transport requires compiled proto stubs (synapse_pb2/synapse_pb2_grpc).
        If stubs are not available, automatically falls back to REST transport.
        To compile stubs: `python -m grpc_tools.protoc -I proto --python_out=... --grpc_python_out=... proto/synapse/v1/*.proto`
        """
        # Check if compiled gRPC stubs are available
        try:
            from ._grpc_stubs import memory_pb2, memory_pb2_grpc  # type: ignore[attr-defined]
            stubs_available = True
        except ImportError:
            stubs_available = False

        if not stubs_available:
            # Fall back to REST if gRPC stubs not compiled
            logger.info(
                "gRPC stubs not compiled — falling back to REST. "
                "See docs/getting-started.md for proto compilation instructions."
            )
            self._active_transport = TransportMode.REST
            await self._connect_rest()
            return await self._rest_request(method, path, json=json, params=params)

        # Route by path pattern using compiled stubs
        stub = memory_pb2_grpc.MemoryServiceStub(self._grpc_channel)

        if path == "/v1/memories" and method == "POST":
            req = memory_pb2.AddRequest(**self._to_proto_fields(json or {}))
            resp = await stub.Add(req)
            return self._proto_to_dict(resp.record)
        elif path.startswith("/v1/memories/") and method == "GET":
            record_id = path.split("/")[-1]
            req = memory_pb2.GetRequest(id=record_id)
            resp = await stub.Get(req)
            return self._proto_to_dict(resp.record)
        elif path == "/v1/memories/search" and method == "POST":
            req = memory_pb2.SearchRequest(**self._to_proto_fields(json or {}))
            resp = await stub.Search(req)
            return {"results": [self._proto_to_dict(r) for r in resp.results]}
        elif path == "/v1/memories/forget" and method == "POST":
            req = memory_pb2.ForgetRequest(**self._to_proto_fields(json or {}))
            resp = await stub.Forget(req)
            return {"deleted_count": resp.deleted_count}
        elif path.startswith("/v1/memories/") and method == "PATCH":
            record_id = path.split("/")[-1]
            fields = self._to_proto_fields(json or {})
            fields["id"] = record_id
            req = memory_pb2.UpdateRequest(**fields)
            resp = await stub.Update(req)
            return self._proto_to_dict(resp.record)
        elif path == "/v1/memories/batch" and method == "POST":
            records = json.get("records", []) if json else []  # type: ignore[union-attr]
            req = memory_pb2.BatchAddRequest(
                records=[memory_pb2.AddRequest(**self._to_proto_fields(r)) for r in records]
            )
            resp = await stub.BatchAdd(req)
            return {"records": [self._proto_to_dict(r) for r in resp.records]}
        else:
            raise SynapseError(f"Unsupported gRPC route: {method} {path}")

    @staticmethod
    def _to_proto_fields(data: dict[str, Any]) -> dict[str, Any]:
        """Convert REST-style JSON fields to proto-compatible field names."""
        # Proto uses snake_case which matches our REST API, so mostly pass-through
        # Filter out None values and convert enums
        return {k: v for k, v in data.items() if v is not None}

    @staticmethod
    def _proto_to_dict(proto_obj: Any) -> dict[str, Any]:
        """Convert a protobuf message to a dict for model validation."""
        from google.protobuf.json_format import MessageToDict
        return MessageToDict(proto_obj, preserving_proto_field_name=True)

    async def _rest_request(
        self,
        method: str,
        path: str,
        *,
        json: Optional[dict[str, Any]] = None,
        params: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        """Execute a single REST/HTTP request."""
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
