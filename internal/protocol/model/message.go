// Package model 是设计文档「五、消息体与消息 ID 设计」的纯 Go 落地。
//
// 为了让仓库在没有 protoc 的环境下也能直接编译运行，这里用 Go struct + JSON
// 复刻 api/proto/message.proto 的三层结构；生产环境可改用 protobuf 编码。
package model

// ConvType 会话类型。
type ConvType int

const (
	ConvUnknown ConvType = iota
	ConvSingle           // 单聊
	ConvGroup            // 群聊
	ConvCS               // 客服
)

// SenderRole 发送方角色（客服场景核心字段，设计文档 6.3）。
type SenderRole int

const (
	RoleUnknown SenderRole = iota
	RoleUser
	RoleAgent  // 人工坐席
	RoleBot    // 机器人
	RoleSystem // 系统消息
)

// MsgType 内容类型。
type MsgType int

const (
	MsgUnknown MsgType = iota
	MsgText
	MsgImage
	MsgCustom
)

// Message 消息体三层结构（设计文档 5.1）。
type Message struct {
	Envelope Envelope `json:"envelope"`
	Identity Identity `json:"identity"`
	Content  Content  `json:"content"`
}

// Envelope 路由层：谁发给谁、哪个会话、怎么投递。
type Envelope struct {
	ConversationID string     `json:"conversation_id"`
	FromUID        int64      `json:"from_uid"`
	ConvType       ConvType   `json:"conv_type"`
	SenderRole     SenderRole `json:"sender_role"`
}

// Identity 标识层：三个独立职责的消息 ID（设计文档 5.2）。
//   - ClientMsgID: 客户端 UUID，幂等去重
//   - ServerMsgID: Snowflake，全局唯一
//   - ConvSeq    : 会话内严格递增，时序/补洞
type Identity struct {
	ClientMsgID string `json:"client_msg_id"`
	ServerMsgID int64  `json:"server_msg_id"`
	ConvSeq     int64  `json:"conv_seq"`
	SendTimeMs  int64  `json:"send_time_ms"`
}

// Content 内容层，按 Type 多态。
type Content struct {
	Type   MsgType `json:"type"`
	Text   string  `json:"text,omitempty"`
	URL    string  `json:"url,omitempty"`
	Schema string  `json:"schema,omitempty"` // 客服卡片/工单等自定义 schema
	Data   []byte  `json:"data,omitempty"`
}
