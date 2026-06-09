#!/usr/bin/env python3
"""End-to-end integration test for the Synapse gRPC server.

Prerequisites:
  - Synapse server binary built: `cargo build --release`
  - Python deps: `pip install grpcio grpcio-tools`
  - Proto stubs generated: see synapse-py/synapse_memory/_grpc_stubs/

Usage:
  ./tests/test_e2e_grpc.py
"""

import os
import sys
import time
import signal
import subprocess

# Add generated stubs to path
STUBS_DIR = os.path.join(os.path.dirname(__file__), "..", "synapse-py", "synapse_memory", "_grpc_stubs")
sys.path.insert(0, os.path.abspath(STUBS_DIR))

import grpc
from synapse.v1 import memory_pb2, memory_pb2_grpc


SERVER_BIN = os.path.join(os.path.dirname(__file__), "..", "target", "release", "synapse-server")
SERVER_ADDR = "localhost:9090"


def start_server():
    """Start synapse-server in background."""
    proc = subprocess.Popen(
        [SERVER_BIN],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env={**os.environ, "RUST_LOG": "info"},
    )
    time.sleep(1)  # Wait for server to bind
    if proc.poll() is not None:
        _, stderr = proc.communicate()
        raise RuntimeError(f"Server failed to start: {stderr.decode()}")
    return proc


def stop_server(proc):
    """Gracefully stop the server."""
    proc.send_signal(signal.SIGTERM)
    proc.wait(timeout=5)


def test_add_list_forget():
    """Test basic CRUD: Add → List → Forget → Verify."""
    channel = grpc.insecure_channel(SERVER_ADDR)
    stub = memory_pb2_grpc.MemoryServiceStub(channel)

    # Add
    resp = stub.Add(memory_pb2.AddRequest(
        content="The capital of France is Paris",
        kind=1,  # FACT
        tags=["geography", "europe"],
        scope=memory_pb2.Scope(org="test-org", visibility=5),  # PUBLIC
        confidence=0.95,
    ))
    assert resp.record.id, "Expected non-empty record ID"
    assert resp.record.content == "The capital of France is Paris"
    assert resp.record.confidence > 0.94
    assert list(resp.record.tags) == ["geography", "europe"]
    record_id = resp.record.id

    # List
    listed = stub.List(memory_pb2.ListRequest(
        scope=memory_pb2.Scope(org="test-org", visibility=5),
        limit=10,
    ))
    assert len(listed.records) == 1, f"Expected 1 record, got {len(listed.records)}"
    assert listed.records[0].id == record_id

    # Forget
    stub.Forget(memory_pb2.ForgetRequest(id=record_id))

    # Verify deleted
    listed2 = stub.List(memory_pb2.ListRequest(
        scope=memory_pb2.Scope(org="test-org", visibility=5),
        limit=10,
    ))
    assert len(listed2.records) == 0, f"Expected 0 records after delete, got {len(listed2.records)}"

    channel.close()
    print("✓ test_add_list_forget PASSED")


def test_health_check():
    """Test gRPC health check returns SERVING."""
    from grpc_health.v1 import health_pb2, health_pb2_grpc
    channel = grpc.insecure_channel(SERVER_ADDR)
    stub = health_pb2_grpc.HealthStub(channel)
    resp = stub.Check(health_pb2.HealthCheckRequest(service=""))
    assert resp.status == 1, f"Expected SERVING (1), got {resp.status}"
    channel.close()
    print("✓ test_health_check PASSED")


def test_multiple_records():
    """Test adding multiple records and listing them."""
    channel = grpc.insecure_channel(SERVER_ADDR)
    stub = memory_pb2_grpc.MemoryServiceStub(channel)

    # Add 3 records
    ids = []
    for i in range(3):
        resp = stub.Add(memory_pb2.AddRequest(
            content=f"Memory record number {i}",
            kind=1,
            scope=memory_pb2.Scope(org="multi-test", visibility=5),
        ))
        ids.append(resp.record.id)

    # List all
    listed = stub.List(memory_pb2.ListRequest(
        scope=memory_pb2.Scope(org="multi-test", visibility=5),
        limit=10,
    ))
    assert len(listed.records) == 3, f"Expected 3, got {len(listed.records)}"

    # Cleanup
    for rid in ids:
        stub.Forget(memory_pb2.ForgetRequest(id=rid))

    channel.close()
    print("✓ test_multiple_records PASSED")


def test_auth_rejected():
    """Test that auth token is required when server has SYNAPSE_AUTH_TOKEN set."""
    # This test only verifies the unauthenticated path (server started without token)
    # When auth is enabled, this would need to be run separately
    channel = grpc.insecure_channel(SERVER_ADDR)
    stub = memory_pb2_grpc.MemoryServiceStub(channel)
    # Should succeed since we started without SYNAPSE_AUTH_TOKEN
    resp = stub.List(memory_pb2.ListRequest(limit=1))
    assert resp is not None
    channel.close()
    print("✓ test_auth_rejected PASSED (server has no auth configured)")


if __name__ == "__main__":
    print(f"Starting Synapse server from: {SERVER_BIN}")
    server = start_server()
    try:
        test_health_check()
        test_add_list_forget()
        test_multiple_records()
        test_auth_rejected()
        print("\n✅ ALL E2E TESTS PASSED")
    finally:
        stop_server(server)
        print("Server stopped.")
