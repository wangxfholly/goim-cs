# goim-cs

亿级用户即时通讯（IM）服务端的**最小可运行参考实现**，含通用 IM 与**在线客服（Customer Service）编排**两大能力。

代码与配套《架构设计文档》(12 章) 一一对应：本仓库不是玩具 demo，而是把设计文档里每一个关键决策（长连接网关、消息路由扩散、三段式消息体、三类消息 ID、客服会话状态机、坐席调度）落成**可编译、可测试**的 Go 代码。

> 架构参考 [goim](https://github.com/Terry-Mao/goim) 的 Comet / Logic / Job 分层思想，但协议、ID 体系、客服编排为本项目自研实现。

## 设计文档 ↔ 代码映射

| 设计章节 | 主题 | 代码位置 |
|---|---|---|
| 二、长连接网关 | 自定义二进制帧协议 / 粘包拆包 | `internal/protocol/frame.go` |
| 二、长连接网关 | 连接管理（分桶 Bucket、多端在线） | `internal/comet/{conn,bucket,server}.go` |
| 三、消息模型 | 三段式消息体（Envelope/Identity/Content） | `internal/protocol/model/message.go`、`api/proto/message.proto` |
| 四、消息路由分发 | 寻址→时序→落库→扩散；读/写扩散混合 | `internal/logic/logic.go` |
| 四、消息路由分发 | Job 投递层（Comet 推送适配） | `internal/job/pusher.go` |
| 五、消息可靠性 | 三类 ID：client_msg_id 去重 / Snowflake / conv_seq | `internal/idgen/{snowflake,seq}.go`、`internal/logic` |
| 六、客服系统编排 | 会话状态机（排队/机器人/人工/关闭） | `internal/cs/session.go` |
| 六、客服系统编排 | 坐席最小连接调度 + 优先级排队 | `internal/cs/dispatcher.go` |
| 七、核心表结构 | 消息/会话/收件箱/客服/工单 DDL | `db/migrations/*.sql` |
| 端到端验证 | 单聊收发全链路 demo | `cmd/demo/main.go` |

## 核心设计要点

- **自定义二进制帧协议**：`magic(2) + version(1) + cmd(2) + seq(4) + body_len(4) + body`。length-prefix 解决 TCP 粘包/拆包；magic 做协议探测。
- **三类相互独立的消息 ID**：
  - `client_msg_id`（客户端 UUID）—— 幂等去重；
  - `server_msg_id`（Snowflake）—— 全局唯一、趋势递增；
  - `conv_seq`（会话内严格递增）—— 排序与补洞依据。
- **store-first 可靠投递**：先落库再扩散投递，离线/弱网客户端重连后按 `max_seq` 补洞。
- **读写扩散混合**：成员数 ≤ `FanoutThreshold(500)` 走写扩散（写每个收件箱 + 在线直推）；大群走读扩散（只通知，按需拉取），规避群聊写放大。
- **分桶连接管理**：Bucket 数为 2 的幂，按 uid 位运算分散锁竞争；单 uid 多连接支持多端在线。
- **Snowflake 时钟回拨保护**：检测到回拨直接拒发 ID（`ErrClockBackwards`），不静默生成可能重复的 ID。
- **客服会话状态机**：`Queuing → Bot/Serving → Closed`，非法跃迁被 `Transition` 守卫拒绝。
- **坐席调度**：最小连接优先 + `container/heap` 优先级排队，分配时原子自增防超分。

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

## 构建与测试

```bash
go build ./...
go vet ./...
go test ./...
```

## 运行 demo

`cmd/demo` 在本机用真实 TCP 跑通「单聊收发 + 服务端分配 ID/seq + 在线推送」全链路：

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

## 从参考实现到生产

本仓库用内存实现（`store.MemoryStore`、`idgen.SeqAllocator`）保证零依赖、可直接 `go test`。生产环境需替换为：

- **存储**：MySQL 分库分表（`t_message`/`t_user_inbox` 按 `conversation_id`/`uid` 哈希），冷数据下沉 HBase（rowkey = `conversation_id + reverse(conv_seq)`）。
- **会话 seq**：Redis `INCR key=conv:seq:{id}`。
- **Job 投递**：经 Kafka 异步削峰，Comet 多实例水平扩展。
- **网关 I/O**：Go netpoll（epoll 等价、事件驱动），避免 2 goroutine/连接 的开销。
# goim-cs
