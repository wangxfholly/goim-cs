// Package store 是设计文档「七、核心表结构设计」的存储抽象层。
//
// 接口面向 8 张表建模；本包提供内存实现用于 demo/单测，生产可替换为
// MySQL 分表 + Redis + HBase（见 db/migrations 下的 DDL）。
package store

import "github.com/wangxfholly/goim-cs/internal/protocol/model"

// MessageStore 对应 t_message（设计文档 7.1）。
type MessageStore interface {
	// Save 落库消息（store-first，设计文档 10.3）。
	Save(msg *model.Message) error
	// ListByConv 按会话拉取 [sinceSeq+1, sinceSeq+limit] 的消息，用于补洞（设计文档 4.3）。
	ListByConv(conversationID string, sinceSeq int64, limit int) ([]*model.Message, error)
	// Dedup 基于 client_msg_id 幂等去重（设计文档 5.2），返回是否首次出现。
	Dedup(clientMsgID string) (firstSeen bool)
}

// InboxStore 对应 t_user_inbox（写扩散收件箱，设计文档 7.4）。
type InboxStore interface {
	Append(uid int64, conversationID string, convSeq int64, serverMsgID int64) error
	List(uid int64, sinceSeq int64, limit int) ([]InboxEntry, error)
}

// InboxEntry 收件箱条目。
type InboxEntry struct {
	UID            int64
	ConversationID string
	ConvSeq        int64
	ServerMsgID    int64
}

// ConversationStore 对应 t_conversation / t_conversation_member（设计文档 7.2 / 7.3）。
type ConversationStore interface {
	Members(conversationID string) ([]int64, error)
	AddMember(conversationID string, uid int64) error
	MemberCount(conversationID string) int
}
