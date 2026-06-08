"""Basic usage example: Add, search, and list memories."""

import asyncio
from synapse_memory import SynapseClient, Scope, MemoryKind


async def main():
    # Connect to Synapse server
    client = SynapseClient(
        endpoint="localhost:9090",
        transport="rest",  # Use REST for simplicity; 'grpc' for production
    )

    # Define scope for this user/agent
    scope = Scope(org="myapp", user="davinci", agent="assistant")

    # --- Add memories ---
    print("=== Adding memories ===")

    record1 = await client.add(
        content="用户偏好简洁专业的回答，不要废话",
        scope=scope,
        kind=MemoryKind.PREFERENCE,
        tags=["communication", "style"],
    )
    print(f"Added: {record1.id} → {record1.content}")

    record2 = await client.add(
        content="项目Alpha的截止日期是2026年7月15日",
        scope=scope,
        kind=MemoryKind.FACT,
        tags=["project", "deadline"],
    )
    print(f"Added: {record2.id} → {record2.content}")

    record3 = await client.add(
        content="2026-06-01 与客户开了需求评审会议，确认了MVP范围",
        scope=scope,
        kind=MemoryKind.EPISODE,
        tags=["meeting", "project"],
    )
    print(f"Added: {record3.id} → {record3.content}")

    # --- Search memories ---
    print("\n=== Searching memories ===")

    results = await client.search(
        query="项目截止日期是什么时候",
        scope=scope,
        top_k=3,
    )

    for r in results:
        print(f"  [{r.score:.2f}] {r.record.content}")

    # --- Format as context for LLM ---
    print("\n=== Context for LLM ===")
    context = client.to_context(results, max_tokens=1000)
    print(context)

    # --- List all memories ---
    print("\n=== Listing all memories ===")
    all_records = await client.list(scope=scope, limit=10)
    for record in all_records:
        print(f"  [{record.kind.value}] {record.content[:50]}...")

    # --- Update a memory ---
    print("\n=== Updating memory ===")
    updated = await client.update(
        id=record2.id,
        content="项目Alpha的截止日期从7月15日延期到8月1日",
    )
    print(f"Updated: {updated.content}")

    # --- Forget a memory ---
    print("\n=== Forgetting memory ===")
    count = await client.forget(id=record3.id)
    print(f"Forgotten: {count} record(s)")

    print("\nDone!")


if __name__ == "__main__":
    asyncio.run(main())
