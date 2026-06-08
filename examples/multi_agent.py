"""Multi-agent memory sharing example with conflict resolution."""

import asyncio
from synapse_memory import SynapseClient, Scope, MemoryKind, EventType


async def main():
    # Both agents connect to the same Synapse server
    agent_a = SynapseClient(endpoint="localhost:9090", transport="rest")
    agent_b = SynapseClient(endpoint="localhost:9090", transport="rest")

    # Shared scope: both agents are in the "support" team
    team_scope = Scope(org="acme", team="support")
    scope_a = Scope(org="acme", team="support", agent="billing-bot")
    scope_b = Scope(org="acme", team="support", agent="logistics-bot")

    print("=== Agent A: Adding customer info ===")
    await agent_a.add(
        content="客户王先生的VIP等级是金牌，折扣率15%",
        scope=scope_a,
        kind=MemoryKind.FACT,
        tags=["customer", "vip"],
    )

    await agent_a.add(
        content="王先生偏好顺丰快递，地址：北京市朝阳区xx路100号",
        scope=scope_a,
        kind=MemoryKind.PREFERENCE,
        tags=["customer", "shipping"],
    )

    print("=== Agent B: Querying shared team memories ===")
    # Agent B can see Agent A's memories because they share the same team
    results = await agent_b.search(
        query="王先生的配送偏好",
        scope=team_scope,
        top_k=5,
    )
    for r in results:
        print(f"  Found: {r.record.content}")

    print("\n=== Simulating conflict ===")
    # Agent A writes one version
    await agent_a.add(
        content="王先生的订单#9001状态：已发货",
        scope=scope_a,
        kind=MemoryKind.FACT,
        tags=["order", "status"],
    )

    # Agent B writes a conflicting version
    await agent_b.add(
        content="王先生的订单#9001状态：已签收",
        scope=scope_b,
        kind=MemoryKind.FACT,
        tags=["order", "status"],
    )

    # In a real scenario, Synapse would detect the conflict via vector clocks
    # and apply the configured resolution strategy (e.g., LLM merge or last-writer-wins)
    print("  → Conflict detected! Both agents wrote different order status.")
    print("  → Synapse would resolve using configured strategy (LLM_MERGE / LAST_WRITER_WINS)")

    print("\n=== Subscribe to memory events ===")
    # Agent B subscribes to changes in the team scope
    event_count = 0

    async def watch_events():
        nonlocal event_count
        async for event in agent_b.subscribe(
            scope=team_scope,
            event_types=[EventType.MEMORY_ADDED, EventType.MEMORY_UPDATED],
        ):
            event_count += 1
            print(f"  Event: {event.type.value} → {event.record.content[:40]}...")
            if event_count >= 2:
                break

    # Start watcher and trigger some writes
    watcher = asyncio.create_task(watch_events())

    # Give subscriber time to connect
    await asyncio.sleep(0.1)

    await agent_a.add(
        content="王先生新增了一个收货地址：上海市浦东新区xx路200号",
        scope=scope_a,
        kind=MemoryKind.FACT,
        tags=["customer", "address"],
    )

    await agent_a.add(
        content="王先生升级为钻石VIP，折扣率提升到20%",
        scope=scope_a,
        kind=MemoryKind.FACT,
        tags=["customer", "vip"],
    )

    # Wait for events or timeout
    try:
        await asyncio.wait_for(watcher, timeout=5.0)
    except asyncio.TimeoutError:
        print("  (Event subscription timed out)")

    print(f"\n=== Total events received: {event_count} ===")
    print("\nDone!")


if __name__ == "__main__":
    asyncio.run(main())
