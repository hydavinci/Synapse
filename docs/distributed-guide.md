# Synapse 分布式部署指南

## 架构总览

```
┌─────────────┐  ┌─────────────┐  ┌─────────────┐
│  Agent A    │  │  Agent B    │  │  Agent C    │
│ (Claude)    │  │ (GPT)       │  │ (自研Agent)  │
└──────┬──────┘  └──────┬──────┘  └──────┬──────┘
       │                │                │
       │ MCP/gRPC       │ MCP/gRPC       │ gRPC/REST
       ▼                ▼                ▼
┌────────────────────────────────────────────────┐
│              Synapse Server (Rust)              │
│  ┌──────┐  ┌──────────┐  ┌──────────────────┐ │
│  │Vector│  │ Conflict │  │   Replication    │ │
│  │Search│  │ Resolver │  │   (Gossip)       │ │
│  └──────┘  └──────────┘  └──────────────────┘ │
│  ┌─────────────────────────────────────────┐   │
│  │         Storage (Memory / Future: DB)   │   │
│  └─────────────────────────────────────────┘   │
└──────────────────────┬─────────────────────────┘
                       │ Cluster Sync
                       ▼
┌──────────────────────────────────────────────────┐
│           Other Synapse Nodes (Peers)            │
└──────────────────────────────────────────────────┘
```

## 核心概念

### 为什么需要分布式？

本地模式 (`synapse-mcp` 裸跑) 适合单人单 agent。但当你有：

- **多个 Agent 协作**：客服 agent、销售 agent、分析 agent 需要共享客户记忆
- **多台设备**：家里电脑的 Claude 和办公室 Cursor 需要同步记忆
- **团队使用**：团队成员各自的 agent 需要共享知识库
- **高可用**：单机挂了记忆不能丢

这时需要分布式。

---

## 快速部署

### 方式一：Docker (推荐)

```bash
# 单节点
docker run -d \
  --name synapse \
  -p 9090:9090 \
  -v synapse-data:/data \
  ghcr.io/hydavinci/synapse:latest

# 客户端连接
synapse-mcp --endpoint localhost:9090
```

### 方式二：Docker Compose (多节点集群)

```yaml
# docker-compose.yml
version: "3.9"

services:
  synapse-node1:
    image: ghcr.io/hydavinci/synapse:latest
    environment:
      SYNAPSE_NODE_ID: node-1
      SYNAPSE_PORT: 9090
      SYNAPSE_PEERS: "synapse-node2:9090,synapse-node3:9090"
      SYNAPSE_CONSISTENCY: eventual
    ports:
      - "9090:9090"
    volumes:
      - node1-data:/data

  synapse-node2:
    image: ghcr.io/hydavinci/synapse:latest
    environment:
      SYNAPSE_NODE_ID: node-2
      SYNAPSE_PORT: 9090
      SYNAPSE_PEERS: "synapse-node1:9090,synapse-node3:9090"
    ports:
      - "9091:9090"
    volumes:
      - node2-data:/data

  synapse-node3:
    image: ghcr.io/hydavinci/synapse:latest
    environment:
      SYNAPSE_NODE_ID: node-3
      SYNAPSE_PORT: 9090
      SYNAPSE_PEERS: "synapse-node1:9090,synapse-node2:9090"
    ports:
      - "9092:9090"
    volumes:
      - node3-data:/data

volumes:
  node1-data:
  node2-data:
  node3-data:
```

```bash
docker compose up -d

# 任意节点都能接收请求
synapse-mcp --endpoint localhost:9090
```

### 方式三：从源码编译

```bash
git clone https://github.com/hydavinci/Synapse.git
cd Synapse/synapse-server

# 编译
cargo build --release

# 运行
./target/release/synapse-server
# 或带配置文件
./target/release/synapse-server --config synapse.toml
```

---

## 配置文件

`synapse.toml`:

```toml
[server]
host = "0.0.0.0"
port = 9090
log_level = "info"

[cluster]
node_id = "node-1"         # 唯一标识，自动生成则无需设置
peers = [                   # 其他节点地址
  "192.168.1.101:9090",
  "192.168.1.102:9090",
]
consistency = "eventual"    # eventual | bounded_staleness | strong

[storage]
backend = "memory"          # v1: memory; 未来: rocksdb, postgres
max_records = 1000000

[conflict]
similarity_threshold = 0.85 # 向量相似度超过此阈值视为"同一记忆"
default_strategy = "last_writer_wins"  # 默认冲突策略
```

所有配置项都可用环境变量覆盖：`SYNAPSE_HOST`、`SYNAPSE_PORT`、`SYNAPSE_NODE_ID`、`SYNAPSE_PEERS` 等。

---

## 分布式原理详解

### 1. Vector Clock（向量时钟）

Synapse 用 Vector Clock 追踪因果关系，这是分布式冲突检测的核心。

```
Vector Clock = { node_id → logical_timestamp }
```

每个节点维护自己的逻辑时钟。写操作时递增：

```
初始状态：
  Node-1 clock: {node-1: 0}
  Node-2 clock: {node-2: 0}

Node-1 写入 "deadline is July 15":
  Node-1 clock: {node-1: 1}
  记录的 vector_clock: {node-1: 1}

Node-2 写入 "deadline is June 30":
  Node-2 clock: {node-2: 1}
  记录的 vector_clock: {node-2: 1}
```

**判断因果关系**：

```
Clock A dominates Clock B ⟺ ∀ key: A[key] ≥ B[key] 且至少一个 >

{node-1: 2, node-2: 1} dominates {node-1: 1, node-2: 1}
→ A 是 B 的后续版本，不冲突

{node-1: 2, node-2: 1} vs {node-1: 1, node-2: 2}
→ 互不 dominate → 并发写入 → 冲突！
```

### 2. 冲突检测

两次写入冲突需同时满足：
1. **目标相同记忆**：通过向量相似度 > threshold（默认 0.85）判断，或相同 record ID
2. **Vector Clock 并发**：两个时钟互不 dominate

```
Agent A (node-1): 写入 "项目截止日期是6月30日"    clock: {A:3, B:2}
Agent B (node-2): 写入 "项目截止日期已延期到7月15日" clock: {A:2, B:4}

Step 1: 语义相似度 = 0.91 > 0.85 → 视为同一记忆
Step 2: {A:3,B:2} vs {A:2,B:4} → 并发！
→ 触发冲突解决
```

### 3. 冲突解决策略

| 策略 | 原理 | 适用场景 |
|------|------|---------|
| `last_writer_wins` | 物理时间戳最新者胜 | 简单场景，能接受偶尔丢失 |
| `first_writer_wins` | 保留最早版本 | 不可变事实 |
| `llm_merge` | 用 LLM 理解两条记忆语义后合并 | 复杂信息，需要推理 |
| `keep_both` | 保留所有版本为独立记录 | 不确定哪个对 |
| `confidence_wins` | confidence 分数高者胜 | 有可信度差异时 |
| `manual` | 标记待人工处理 | 关键决策 |

**LLM Merge 示例**：

```
冲突记忆 A: "项目截止日期是6月30日"
冲突记忆 B: "项目截止日期已延期到7月15日"

LLM 分析:
  - B 提到"延期"，说明知道之前是6月30日
  - B 是对 A 的更新，不是矛盾
  
合并结果: "项目截止日期最初为6月30日，后延期至7月15日"
```

### 4. 数据同步

节点间通过 **Anti-Entropy Gossip** 同步数据：

```
┌────────┐         ┌────────┐
│ Node 1 │──Sync──▶│ Node 2 │
│        │◀──Sync──│        │
└────────┘         └────────┘
     │                  │
     └───────┬──────────┘
             ▼
        ┌────────┐
        │ Node 3 │
        └────────┘

同步流程：
1. Node 1 发送自己的 Merkle Tree 摘要给 Node 2
2. Node 2 对比，发现差异分支
3. 只同步差异的记录（增量传输）
4. 接收方 merge_clock 合并向量时钟
5. 如果检测到冲突，触发冲突解决管道
```

### 5. 一致性级别

你可以按场景选择不同一致性：

```toml
# 全局默认
[cluster]
consistency = "eventual"
```

或通过 Scope 细粒度控制（协议支持，server 后续实现）：

| 级别 | 行为 | 延迟 | 场景 |
|------|------|------|------|
| `eventual` | 写入本地立即返回，异步复制 | ~1ms | 偏好、非关键事实 |
| `bounded_staleness` | 读保证 N ms 内的新鲜度 | ~10-50ms | 多数业务场景 |
| `strong` | 线性一致，写后所有节点可读 | ~50-200ms | 规则、关键决策 |

---

## 客户端连接

### Python SDK (直接调用)

```python
from synapse_memory import SynapseClient, MemoryKind, Scope

async with SynapseClient("your-server:9090", token="your-token") as client:
    # 带 scope 写入
    await client.add(
        "客户VIP等级: 金卡",
        scope=Scope(org="acme", team="support", agent="billing-bot"),
        kind=MemoryKind.FACT,
    )

    # 搜索（自动受 scope 可见性限制）
    results = await client.search(
        "VIP客户",
        scope=Scope(org="acme", team="support"),
    )

    # 订阅实时事件
    async for event in client.subscribe(scope=Scope(org="acme")):
        print(f"New event: {event.type} - {event.record.content}")
```

### MCP 模式 (通过 AI 客户端)

```json
{
  "mcpServers": {
    "synapse": {
      "command": "synapse-mcp",
      "args": ["--endpoint", "your-server:9090", "--token", "your-token"]
    }
  }
}
```

### Scope 示例

```
# 个人 agent 的私有记忆
--scope "user:davinci/agent:my-assistant"

# 团队共享
--scope "org:acme/team:engineering"

# 特定会话（临时记忆）
--scope "user:davinci/session:abc123"
```

---

## 多 Agent 协作示例

```
场景：客服系统，多个专业 Agent 协作

┌───────────────────────────────────────────────┐
│ Scope: org:company/team:support               │
│                                               │
│  billing-agent ──┐                            │
│                  │ 共享记忆                     │
│  tech-agent ─────┼──▶ Synapse Server          │
│                  │    (冲突解决 + 同步)          │
│  sales-agent ────┘                            │
│                                               │
│  记忆可见性:                                    │
│  - billing-agent 写的记忆 → SCOPE_UP          │
│  - 整个 support team 可见                      │
│  - 其他 team 不可见 (除非 SHARED/PUBLIC)        │
└───────────────────────────────────────────────┘
```

Agent A (billing) 存入：
```
memory_store("客户#1234 VIP金卡, 月消费5万+", scope="org:company/team:support/agent:billing", kind="fact")
```

Agent B (tech-support) 搜索：
```
memory_recall("客户#1234 什么等级", scope="org:company/team:support/agent:tech")
→ 找到! (因为 billing-agent 设置了 visibility=SCOPE_UP, team 内可见)
```

---

## 生产部署建议

| 配置项 | 开发 | 生产 |
|--------|------|------|
| 节点数 | 1 | 3+ (奇数) |
| 存储 | memory | 未来: RocksDB/Postgres |
| 一致性 | eventual | bounded_staleness |
| 冲突策略 | last_writer_wins | llm_merge |
| 认证 | 无 | Bearer token |
| 网络 | localhost | 内网/VPN + TLS |

### 安全

```toml
# 未来版本支持
[auth]
enabled = true
tokens = ["token-agent-a", "token-agent-b"]
# 或接入 OIDC
oidc_issuer = "https://auth.company.com"
```

### 监控

Server 暴露 Prometheus metrics（规划中）：
- `synapse_records_total` — 总记录数
- `synapse_search_latency_ms` — 搜索延迟
- `synapse_conflicts_active` — 活跃冲突数
- `synapse_sync_lag_ms` — 节点间同步延迟

---

## 当前状态 & Roadmap

### v0.1 (当前)
- ✅ 单节点 gRPC server
- ✅ 内存存储
- ✅ Vector clock 基础实现
- ✅ 冲突检测框架
- ✅ Cluster join/leave/status API

### v0.2 (计划)
- [ ] 持久化存储 (RocksDB)
- [ ] 实际 Gossip 同步实现
- [ ] Token 认证
- [ ] LLM merge 策略对接

### v0.3 (计划)
- [ ] TLS 加密
- [ ] Prometheus metrics
- [ ] Web UI (集群状态、冲突管理)
- [ ] 自动 compaction (合并/摘要/过期)

### v1.0
- [ ] 生产级多节点测试
- [ ] 持久化 + WAL
- [ ] OIDC / mTLS 认证
- [ ] Kubernetes Helm Chart
