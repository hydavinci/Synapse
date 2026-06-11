"""Tests for CircuitBreaker pattern in client.py."""

import asyncio
import time
from unittest.mock import AsyncMock, patch

import pytest

from synapse_memory.client import CircuitBreaker, CircuitState


@pytest.fixture
def breaker() -> CircuitBreaker:
    """Create a circuit breaker with default settings."""
    return CircuitBreaker(failure_threshold=5, recovery_timeout=30.0)


@pytest.fixture
def fast_breaker() -> CircuitBreaker:
    """Create a circuit breaker with fast recovery for testing."""
    return CircuitBreaker(failure_threshold=3, recovery_timeout=0.1)


class TestCircuitBreakerStates:
    """Test state transitions."""

    @pytest.mark.asyncio
    async def test_initial_state_is_closed(self, breaker: CircuitBreaker) -> None:
        """Circuit starts in CLOSED state."""
        assert breaker.state == CircuitState.CLOSED

    @pytest.mark.asyncio
    async def test_closed_allows_requests(self, breaker: CircuitBreaker) -> None:
        """CLOSED state allows all requests."""
        assert await breaker.allow_request() is True

    @pytest.mark.asyncio
    async def test_closed_to_open_after_threshold_failures(
        self, breaker: CircuitBreaker
    ) -> None:
        """Circuit opens after consecutive failure threshold is reached."""
        for _ in range(5):
            await breaker.record_failure()

        assert breaker.state == CircuitState.OPEN

    @pytest.mark.asyncio
    async def test_below_threshold_stays_closed(self, breaker: CircuitBreaker) -> None:
        """Circuit stays closed if failures are below threshold."""
        for _ in range(4):
            await breaker.record_failure()

        assert breaker.state == CircuitState.CLOSED
        assert await breaker.allow_request() is True

    @pytest.mark.asyncio
    async def test_success_resets_failure_count(self, breaker: CircuitBreaker) -> None:
        """A success resets consecutive failure count."""
        for _ in range(4):
            await breaker.record_failure()

        await breaker.record_success()
        assert breaker.consecutive_failures == 0
        assert breaker.state == CircuitState.CLOSED

    @pytest.mark.asyncio
    async def test_open_to_half_open_after_timeout(
        self, fast_breaker: CircuitBreaker
    ) -> None:
        """Circuit moves to HALF_OPEN after recovery timeout."""
        # Trip the circuit
        for _ in range(3):
            await fast_breaker.record_failure()

        assert fast_breaker.state == CircuitState.OPEN

        # Wait for recovery timeout
        await asyncio.sleep(0.15)

        assert fast_breaker.state == CircuitState.HALF_OPEN

    @pytest.mark.asyncio
    async def test_half_open_allows_probe(self, fast_breaker: CircuitBreaker) -> None:
        """HALF_OPEN state allows one probe request."""
        # Trip the circuit
        for _ in range(3):
            await fast_breaker.record_failure()

        # Wait for recovery
        await asyncio.sleep(0.15)
        assert fast_breaker.state == CircuitState.HALF_OPEN
        assert await fast_breaker.allow_request() is True


class TestCircuitBreakerFailFast:
    """Test fail-fast behavior when OPEN."""

    @pytest.mark.asyncio
    async def test_open_denies_requests(self, breaker: CircuitBreaker) -> None:
        """OPEN state denies requests (fail fast)."""
        # Trip the circuit
        for _ in range(5):
            await breaker.record_failure()

        assert breaker.state == CircuitState.OPEN
        assert await breaker.allow_request() is False

    @pytest.mark.asyncio
    async def test_open_multiple_denials(self, breaker: CircuitBreaker) -> None:
        """Multiple requests are denied when circuit is open."""
        for _ in range(5):
            await breaker.record_failure()

        for _ in range(10):
            assert await breaker.allow_request() is False


class TestCircuitBreakerProbe:
    """Test probe behavior in HALF_OPEN state."""

    @pytest.mark.asyncio
    async def test_probe_success_closes_circuit(
        self, fast_breaker: CircuitBreaker
    ) -> None:
        """Successful probe in HALF_OPEN closes the circuit."""
        # Trip the circuit
        for _ in range(3):
            await fast_breaker.record_failure()

        # Wait for recovery
        await asyncio.sleep(0.15)
        assert fast_breaker.state == CircuitState.HALF_OPEN

        # Successful probe
        await fast_breaker.record_success()
        assert fast_breaker.state == CircuitState.CLOSED
        assert fast_breaker.consecutive_failures == 0

    @pytest.mark.asyncio
    async def test_probe_failure_reopens_circuit(
        self, fast_breaker: CircuitBreaker
    ) -> None:
        """Failed probe in HALF_OPEN reopens the circuit."""
        # Trip the circuit
        for _ in range(3):
            await fast_breaker.record_failure()

        # Wait for recovery
        await asyncio.sleep(0.15)
        assert fast_breaker.state == CircuitState.HALF_OPEN

        # Failed probe
        await fast_breaker.record_failure()
        assert fast_breaker.state == CircuitState.OPEN


class TestCircuitBreakerRecovery:
    """Test timeout-based recovery."""

    @pytest.mark.asyncio
    async def test_full_recovery_cycle(self, fast_breaker: CircuitBreaker) -> None:
        """Test full cycle: CLOSED -> OPEN -> HALF_OPEN -> CLOSED."""
        # Start CLOSED
        assert fast_breaker.state == CircuitState.CLOSED

        # Trip to OPEN
        for _ in range(3):
            await fast_breaker.record_failure()
        assert fast_breaker.state == CircuitState.OPEN

        # Wait for HALF_OPEN
        await asyncio.sleep(0.15)
        assert fast_breaker.state == CircuitState.HALF_OPEN

        # Recover to CLOSED
        await fast_breaker.record_success()
        assert fast_breaker.state == CircuitState.CLOSED

    @pytest.mark.asyncio
    async def test_reset_returns_to_closed(self, breaker: CircuitBreaker) -> None:
        """Manual reset returns circuit to CLOSED."""
        for _ in range(5):
            await breaker.record_failure()
        assert breaker.state == CircuitState.OPEN

        await breaker.reset()
        assert breaker.state == CircuitState.CLOSED
        assert breaker.consecutive_failures == 0

    @pytest.mark.asyncio
    async def test_not_open_before_timeout(self) -> None:
        """Circuit stays OPEN before recovery timeout elapses."""
        breaker = CircuitBreaker(failure_threshold=2, recovery_timeout=10.0)

        for _ in range(2):
            await breaker.record_failure()

        assert breaker.state == CircuitState.OPEN
        # Should still be OPEN (10s timeout hasn't elapsed)
        assert await breaker.allow_request() is False
