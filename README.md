![alt text](image.png)

# goim-cs

> 亿级用户即时通讯（IM）服务端的**最小可运行参考实现**，含通用 IM 与**在线客服（Customer Service）编排**两大能力。
>
> 本文档基于 [goim-cs 架构设计 v2](https://icntj9t3t224.feishu.cn/docx/PkmPdLxwuoRFCux35K4cBLpGnIh)，把设计文档中 12 章的关键决策（长连接网关、消息路由扩散、三段式消息体、三类消息 ID、客服会话状态机、坐席调度）落成**可编译、可测试**的 Go 代码。
>
> 架构参考 [goim](https://github.com/Terry-Mao/goim) 的 Comet / Logic / Job 分层思想，并在协议、ID 体系、客服编排上做自研实现。

---

## 一、设计目标与非目标

### 1.1 设计目标

| 维度 | 目标 | 量化指标 |
|---|---|---|
| **海量连接** | 单网关承载大规模长连接，集群支撑亿级在线 | 单 Comet ≥ 100 万长连接；集群水平扩展 |
| **低延迟** | 在线消息端到端实时投递 | 同城在线投递 P99 < 200ms |
| **不丢不重** | 消息可靠送达，幂等去重 | store-first + 三类 ID 保证最终一致 |
| **严格时序** | 会话内消息顺序唯一确定 | 服务端分配 conv_seq 严格递增 |
| **多端同步** | 同一账号多设备一致视图 | 按 max_seq 补洞 + 踢人策略 |
| **客服编排** | 支持排队、机器人、人工、工单 | 会话状态机 + 最小连接调度 |

### 1.2 非目标

- 不覆盖音视频实时通话（RTC）的编解码与媒体传输，仅预留信令通道。
- 不展开客户端（iOS/Android/Web）的本地存储与 UI 细节。
- 安全合规（端到端加密、风控）仅给出接入点，不展开密码学方案。

> **相对经典架构的三点强化**：① 逻辑层无状态化 + 一致性哈希分片；② 会话 seq 由 Redis INCR 下沉到分片，避免全局单点；③ Job 投递层经 Kafka 异步削峰，解耦逻辑层与网关。

---

## 二、整体架构：五层分层模型

### 2.1 五层分层模型

每层职责单一、可独立水平扩展：

| 层级 | 职责 | goim-cs 对应 |
|---|---|---|
| **用户端** | iOS/Android/Web/客服坐席端，TCP SDK + WebSocket 接入 | cmd/demo 客户端模拟 |
| **接入层 Comet** | 保持海量连接、鉴权、心跳、攻击防护，将海量连接整流为少量内部连接 | [internal/comet](file:///Users/bytedance/trae/goim-cs/internal/comet) |
| **逻辑层 Logic** | 寻址、时序分配、落库、扩散决策，核心业务逻辑（c2c/c2g/s2c/c2s） | [internal/logic](file:///Users/bytedance/trae/goim-cs/internal/logic) |
| **投递层 Job** | 消费 Kafka，把消息推给目标用户所在 Comet（削峰、解耦） | [internal/job](file:///Users/bytedance/trae/goim-cs/internal/job) |
| **存储层 Store** | 消息库（MySQL 分库分表/HBase）、状态路由（Redis）、文件（对象存储） | [internal/store](file:///Users/bytedance/trae/goim-cs/internal/store) + [db/migrations](file:///Users/bytedance/trae/goim-cs/db/migrations) |

### 2.2 整流思想

接入层把海量客户端长连接**整流**为与逻辑层之间的少量内部 RPC/长连接，逻辑层因此无需感知具体 TCP 连接，可做到完全无状态、随意扩缩容。这是支撑亿级在线的关键解耦。

---

## 三、连接管理与接入层（Comet）

### 3.1 连接保持与分桶

Comet 网关用**分桶（Bucket）**管理连接，降低锁竞争：Bucket 数取 2 的幂，按 uid 位运算取模分散。单 uid 可挂多个连接，天然支持多端在线。

- **Conn**（[internal/comet/conn.go](file:///Users/bytedance/trae/goim-cs/internal/comet/conn.go)）：每连接一个带缓冲 send channel，Push 非阻塞、队列满即丢弃，做慢消费者保护；Close 经 `sync.Once` 幂等。
- **Bucket**（[internal/comet/bucket.go](file:///Users/bytedance/trae/goim-cs/internal/comet/bucket.go)）：`conns map[connID]*Conn + users map[uid][]*Conn`，`PushToUser` 返回投递成功数，支持多端广播。
- **I/O 模型**：基于 Go netpoll（epoll 等价、事件驱动），避免「2 goroutine/连接」的内存与调度开销。

### 3.2 心跳与超时

客户端定期发送心跳帧（`CmdHeartbeat`），服务端每次读到帧刷新 read deadline（默认 90s）。超时未活跃的连接将被回收，释放 Bucket 槽位。

### 3.3 踢人（Kickout）

沿用经典架构的多端互踢策略：同类型设备重复登录时，旧连接被踢下线。

---

## 四、通信协议设计

### 4.1 帧协议（接入层 ↔ 客户端）

自定义二进制帧，length-prefix 解决 TCP 粘包/拆包（[internal/protocol/frame.go](file:///Users/bytedance/trae/goim-cs/internal/protocol/frame.go)）：

| 字段 | 长度 | 说明 |
|---|---|---|
| magic | 2B | 0xC5C5，协议探测 |
| version | 1B | 协议版本 = 1 |
| cmd | 2B | 命令字 |
| seq | 4B | 请求序号，用于 ACK 配对 |
| body_len | 4B | 消息体长度（≤ 4MB） |
| body | 变长 | protobuf / json 消息体 |

头部固定 13 字节。对比经典架构的 20 字节头，精简了冗余字段，扩展信息下放到 body 的 protobuf，兼顾紧凑与可演进。

### 4.2 命令字（Cmd）

| 命令 | 含义 |
|---|---|
| CmdAuth / CmdAuthReply | 鉴权请求 / 响应 |
| CmdHeartbeat / CmdHeartbeatReply | 心跳保活 |
| CmdSend / CmdSendAck | 上行发消息 / 服务端 ACK（携带 server_msg_id、conv_seq） |
| CmdPush / CmdPushAck | 下行推送 / 客户端确认收到 |
| CmdSync | 补洞拉取（sinceSeq 之后的消息） |

### 4.3 消息体协议

消息体采用 protobuf（[api/proto/message.proto](file:///Users/bytedance/trae/goim-cs/api/proto/message.proto)），序列化效率、压缩率、可扩展性俱佳。

---

## 五、消息模型与三类 ID

### 5.1 三段式消息体

消息模型（[internal/protocol/model/message.go](file:///Users/bytedance/trae/goim-cs/internal/protocol/model/message.go)）分三层，职责清晰、便于扩展：

| 分层 | 字段 | 作用 |
|---|---|---|
| **Envelope 信封** | ConversationID, FromUID, ConvType, SenderRole | 路由寻址：发给谁、谁发的、什么会话类型、什么角色 |
| **Identity 标识** | ClientMsgID, ServerMsgID, ConvSeq, SendTimeMs | 身份与时序：去重、全局唯一、会话内排序 |
| **Content 内容** | Type, Text, URL, Schema, Data | 消息体：文本/图片/自定义 |

### 5.2 三类相互独立的消息 ID

这是 IM 可靠性的基石。经典架构用单一 msg_id + msg_seq，goim-cs 拆为三类各司其职的 ID，彻底分离「去重」「全局唯一」「排序」三个正交诉求。

| ID | 生成方 | 作用 | 实现 |
|---|---|---|---|
| **client_msg_id** | 客户端 UUID | 幂等去重（重发不重复落库） | Dedup 查重 |
| **server_msg_id** | 服务端 Snowflake | 全局唯一、趋势递增 | [internal/idgen/snowflake.go](file:///Users/bytedance/trae/goim-cs/internal/idgen/snowflake.go) |
| **conv_seq** | 服务端会话级 | 会话内严格递增，排序与补洞依据 | [internal/idgen/seq.go](file:///Users/bytedance/trae/goim-cs/internal/idgen/seq.go) |

### 5.3 Snowflake 与时钟回拨

Snowflake 结构：1 符号位 + 41 时间戳 + 10 机器位 + 12 序列位。检测到时钟回拨时**直接拒发 ID**（`ErrClockBackwards`），绝不静默生成可能重复的 ID。

会话 seq 生产环境用 `Redis INCR key=conv:seq:{id}`，按会话分片避免全局单点（对比经典架构的单点 Redis 瓶颈）。

---

## 六、消息路由与分发机制

### 6.1 四层路由框架

逻辑层 `HandleSend`（[internal/logic/logic.go](file:///Users/bytedance/trae/goim-cs/internal/logic/logic.go)）按**「寻址 → 时序 → 落库 → 扩散」**四步处理每条上行消息。

### 6.2 读写扩散混合

沿用经典架构「扩散写」思想，但引入阈值做混合，规避大群写放大（消息风暴扩散系数）：

| 策略 | 适用 | 代价 |
|---|---|---|
| **写扩散** | 单聊 / 小群（成员 ≤ FanoutThreshold=500） | 写每个收件箱，写放大；读取快 |
| **读扩散** | 大群（成员 > 500） | 只通知一次，客户端按 conv_seq 拉取；读放大 |

---

## 七、核心业务流程

### 7.1 鉴权（auth）

客户端用 uid+token 发起鉴权，Comet 调 Logic 校验 token 合法性后设置 session 状态，返回结果。goim-cs demo 中以 body 前 8 字节为 uid 模拟。

### 7.2 单聊（c2c）

`Alice → CmdSend → Comet → Logic（去重 + 分配 ID/seq + store-first 落库）→ Job/Kafka → Comet → Bob（CmdPush → PushAck）`

### 7.3 群聊（c2g）

小群走写扩散：落发送方消息 → 取成员列表 → 并发批量写各成员收件箱 → 在线成员直推、离线成员等补洞。大群走读扩散仅广播通知。

### 7.4 推送与上报（s2c / c2s）

- **s2c（服务端推送）**：业务线调 sendMsg → Logic 查 Redis 目标在线状态与所在 Comet → 经 Job 投递 → Comet 下推。不在线则进离线队列。
- **c2s（客户端上报）**：客户端上行 → Comet 回 ACK → 投递到对应业务 MQ 队列 → 业务服务器消费。

### 7.5 离线消息拉取（pull / 补洞）

`HandleSync(conversationID, sinceSeq, limit)` 返回 sinceSeq 之后的消息。客户端重连后按本地 max_seq 分页拉取直至返回 0 条，保证不丢消息。

---

## 八、消息可靠性与时序一致性

### 8.1 store-first 可靠投递

**「先落库、再扩散投递」**是可靠性的核心约束：即使投递瞬间失败（离线/弱网），消息已持久化，客户端重连后必能通过补洞补齐，实现最终一致。

### 8.2 ACK 链路与送达确认

- 发送 ACK（`CmdSendAck`）回带 server_msg_id/conv_seq；
- 推送 ACK（`CmdPushAck`）由接收端回，用于将消息标记为「已送达」。
- 未达消息可触发离线推送（APNS/厂商通道）。

### 8.3 幂等去重

同一 `client_msg_id` 重发时，Dedup 直接返回已存在结果（`Duplicated=true`），不二次落库、不二次扩散，保证「不重」。

> **不丢不重的闭环**：store-first（不丢） + client_msg_id 去重（不重） + conv_seq 严格递增（不乱） + 补洞拉取（弱网兜底）。四者共同构成可靠时序保证。

---

## 九、存储设计

### 9.1 扩散写表模型

核心表见 [db/migrations/001_core.sql](file:///Users/bytedance/trae/goim-cs/db/migrations/001_core.sql) 与 [002_cs.sql](file:///Users/bytedance/trae/goim-cs/db/migrations/002_cs.sql)：

| 表 | 作用 | 关键索引/分片 |
|---|---|---|
| t_message | 消息主体 | `uk(conversation_id, conv_seq)`、`uk(conversation_id, client_msg_id)` 幂等；按 conversation_id 哈希分库 |
| t_conversation | 会话元信息 | `max_seq` 维护当前最大序号 |
| t_conversation_member | 会话成员 + 已读位点 | `idx(uid)` 反查用户会话；`read_seq` 已读 |
| t_user_inbox | 用户收件箱（写扩散） | `pk(uid, inbox_seq)`；按 uid 哈希分库 |
| t_offline_push | 离线推送队列 | `idx(uid, status)` |

### 9.2 水平分库分表

- `t_message` 按 conversation_id 哈希分库分表（同会话同库，便于按 conv_seq 范围拉取）。
- `t_user_inbox` 按 uid 哈希分库分表（同用户同库，便于多端同步与补洞）。

### 9.3 Redis 缓存与状态路由

| key | value | 用途 |
|---|---|---|
| uid → {comet, conn, last_active} | 在线状态/路由 | s2c 投递寻址、踢人 |
| conv:seq:{id} | INCR 计数器 | 会话内 conv_seq 分配（分片去单点） |
| conn → uid | 反向映射 | Comet 层 session |

### 9.4 文件存储与冷数据归档

图片/文件走对象存储（商用云存储），消息体只存 URL/token。冷数据下沉 HBase，`rowkey = conversation_id + reverse(conv_seq)`，避免热点、支持按会话范围扫描。

---

## 十、在线客服系统编排

### 10.1 会话状态机

客服会话（[internal/cs/session.go](file:///Users/bytedance/trae/goim-cs/internal/cs/session.go)）以状态机驱动，非法跃迁被 `Transition` 守卫拒绝：

```
访客发起 → Queuing
    ├── 机器人接待 → 直接分配坐席 → Serving
    └── 转人工 → Serving
        Serving → 会话结束 → Closed
```

### 10.2 坐席调度

调度器（[internal/cs/dispatcher.go](file:///Users/bytedance/trae/goim-cs/internal/cs/dispatcher.go)）采用**最小连接优先**：线性扫描在线坐席，选 `cur_sessions` 最小者；分配时原子自增防超分。配合 `container/heap` 优先级排队队列，高优先级访客优先出队。

### 10.3 工单沉淀

会话关闭后沉淀为工单（`t_ticket`），记录分类、满意度评分、处理状态，支撑售后追溯与质检。坐席与会话表（`t_cs_session`、`t_agent`）见 [002_cs.sql](file:///Users/bytedance/trae/goim-cs/db/migrations/002_cs.sql)。

---

## 十一、容量评估与高可用

### 11.1 容量评估

| 指标 | 单机 | 集群（亿级） |
|---|---|---|
| 长连接 | ≥ 100 万 / Comet | 100+ Comet 实例横向扩展 |
| 内存/连接 | netpoll 模型，KB 级 | 分桶降低锁竞争 |
| 消息吞吐 | Logic 无状态可平扩 | Kafka 削峰 + Job 并行消费 |

### 11.2 高可用与容灾

- **无状态逻辑层**：Logic 不持有连接状态，任意实例可处理任意请求，故障即摘除。
- **接入层冗余**：Comet 多实例 + IPList/域名负载均衡，单点故障不影响整体在线。
- **异步削峰**：突发流量经 Kafka 缓冲，避免逻辑层/存储层被打垮。
- **多机房**：会话按 conversation_id 路由就近机房，跨机房经消息总线同步。

---

## 十二、与参考架构的差异及演进路线

### 12.1 相对经典架构的关键改进

| 维度 | 经典海量 IM 架构 | goim-cs v2 |
|---|---|---|
| 逻辑层 | msg_logic 关键路径，易成单点 | 无状态 + 一致性哈希分片，随意扩缩 |
| 会话 seq | 易依赖单点全局计数 | Redis INCR 按会话分片，去单点 |
| 投递解耦 | Logic 直推 Gate | Kafka + Job 异步削峰 |
| 消息 ID | 单一 msg_id + seq | 三类正交 ID（去重/唯一/排序） |
| 大群 | 纯扩散写，写放大严重 | 读写扩散混合（阈值 500） |
| 协议头 | 20 字节 | 13 字节 + protobuf 可扩展 |

### 12.2 后续演进路线

1. 微服务拆分：`cmd/comet`、`cmd/logic`、`cmd/job` 独立进程，接入 Kafka 与服务发现。
2. 存储替换：`MemoryStore → MySQL 分库分表 + Redis + HBase 冷热分级`。
3. 多端已读同步、消息撤回/编辑、@提醒、会话置顶等业务能力。
4. 端到端加密、风控反垃圾、全链路压测与混沌工程。

---

## 目录结构

```
goim-cs/
├── api/proto/            # protobuf 消息定义
├── cmd/demo/             # 端到端串联 demo
├── db/migrations/        # 建表 DDL
└── internal/
    ├── protocol/         # 帧协议 + 消息模型
    ├── comet/            # 长连接网关（连接/分桶/server）
    ├── logic/            # 消息路由分发核心
    ├── job/              # 投递层
    ├── idgen/            # Snowflake + 会话 seq
    ├── store/            # 存储抽象 + 内存实现
    └── cs/               # 客服编排（状态机 + 调度）
```

---

## 构建与测试

```bash
go build ./...
go vet ./...
go test ./...
```

## 运行 demo

[cmd/demo](file:///Users/bytedance/trae/goim-cs/cmd/demo) 在本机用真实 TCP 跑通「单聊收发 + 服务端分配 ID/seq + 在线推送」全链路：

```bash
go run ./cmd/demo
```

预期输出：
```
[demo] gateway listening on 127.0.0.1:xxxxx
[demo] alice got ACK: server_msg_id=... conv_seq=1
[demo] bob received: "Hello Bob!" (conv_seq=1, from=1)
[demo] END-TO-END OK ✓
```

---

## 从参考实现到生产

本仓库用内存实现（`store.MemoryStore`、`idgen.SeqAllocator`）保证零依赖、可直接 `go test`。生产环境需替换为：

- **存储**：MySQL 分库分表（`t_message`/`t_user_inbox` 按 `conversation_id`/`uid` 哈希），冷数据下沉 HBase（rowkey = `conversation_id + reverse(conv_seq)`）。
- **会话 seq**：Redis `INCR key=conv:seq:{id}`。
- **Job 投递**：经 Kafka 异步削峰，Comet 多实例水平扩展。
- **网关 I/O**：Go netpoll（epoll 等价、事件驱动），避免 2 goroutine/连接 的开销。

---

**小结**：goim-cs v2 在经过海量在线验证的五层架构骨架上，用「无状态逻辑层 + 分片 seq + Kafka 削峰 + 三类消息 ID + 读写扩散混合」补齐了经典架构的单点与写放大短板，并把客服编排作为一等公民纳入设计，形成可落地、可扩展的亿级 IM 服务端方案。
# goim-cs
