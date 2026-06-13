// Package logic 是设计文档「四、消息路由分发机制」的落地。
//
// 实现路由分发四层框架（设计文档 4.1）:
//  1. 寻址(addressing)   —— 通过 ConversationStore 找到会话成员
//  2. 扩散(fanout)       —— 单聊/小群写扩散，大群读扩散（4.2 混合模式）
//  3. 时序(ordering)     —— 服务端分配严格递增 conv_seq
//  4. 可靠(reliability)  —— store-first 先落库再投递（4.3 / 10.3）
package logic

import (
	"time"

	"github.com/wangxfholly/goim-cs/internal/idgen"
	"github.com/wangxfholly/goim-cs/internal/protocol/model"
	"github.com/wangxfholly/goim-cs/internal/store"
)

// FanoutThreshold 成员数超过该阈值的群走读扩散，否则写扩散（设计文档 4.2）。
const FanoutThreshold = 500

// Pusher 抽象「把消息投递到目标用户」的能力，由 Job/Comet 实现。
type Pusher interface {
	PushToUser(uid int64, msg *model.Message) (delivered int)
}

// Logic 消息处理核心。
type Logic struct {
	sf     *idgen.Snowflake
	seq    idgen.SeqStore
	msgs   store.MessageStore
	convs  store.ConversationStore
	inbox  store.InboxStore
	pusher Pusher
}

// New 创建 Logic。
func New(sf *idgen.Snowflake, seq idgen.SeqStore, msgs store.MessageStore,
	convs store.ConversationStore, inbox store.InboxStore, pusher Pusher) *Logic {
	return &Logic{sf: sf, seq: seq, msgs: msgs, convs: convs, inbox: inbox, pusher: pusher}
}

// SetPusher 注入投递器。用于解决「网关 Buckets 与 Logic 互相依赖」的初始化环：
// 先建 Logic（pusher=nil）→ 建网关 → 用网关 Buckets 构造 Pusher → 回注。
func (l *Logic) SetPusher(p Pusher) { l.pusher = p }

// SendResult 发送结果，回给发送方的 ACK（设计文档 4.3）。
type SendResult struct {
	ServerMsgID int64
	ConvSeq     int64
	Duplicated  bool // 命中幂等去重
}

// HandleSend 处理一条上行消息，完成寻址→时序→落库→扩散。
func (l *Logic) HandleSend(msg *model.Message) (*SendResult, error) {
	// (0) 幂等去重：同一 client_msg_id 重发直接返回已存在结果（设计文档 5.2）。
	if !l.msgs.Dedup(msg.Identity.ClientMsgID) {
		return &SendResult{Duplicated: true}, nil
	}

	// (1) 时序：服务端分配全局唯一 ID + 会话内严格递增 seq。
	serverID, err := l.sf.NextID()
	if err != nil {
		return nil, err
	}
	convSeq := l.seq.NextSeq(msg.Envelope.ConversationID)
	msg.Identity.ServerMsgID = serverID
	msg.Identity.ConvSeq = convSeq
	msg.Identity.SendTimeMs = time.Now().UnixMilli()

	// (2) 可靠：store-first，先落库再投递，保证可达性最终一致（设计文档 10.3）。
	if err := l.msgs.Save(msg); err != nil {
		return nil, err
	}

	// (3) 寻址 + 扩散。
	members, err := l.convs.Members(msg.Envelope.ConversationID)
	if err != nil {
		return nil, err
	}
	if len(members) > FanoutThreshold {
		l.readFanout(msg) // 大群：读扩散
	} else {
		l.writeFanout(msg, members) // 单聊/小群：写扩散
	}

	return &SendResult{ServerMsgID: serverID, ConvSeq: convSeq}, nil
}

// writeFanout 写扩散：逐成员写收件箱 + 在线直推（设计文档 4.2）。
func (l *Logic) writeFanout(msg *model.Message, members []int64) {
	for _, uid := range members {
		_ = l.inbox.Append(uid, msg.Envelope.ConversationID, msg.Identity.ConvSeq, msg.Identity.ServerMsgID)
		l.pusher.PushToUser(uid, msg) // 离线时投递失败由客户端重连后按 max_seq 补洞
	}
}

// readFanout 读扩散：不写每个成员收件箱，只通知在线成员「会话有新消息」，
// 客户端按需拉取（设计文档 4.2，解决群聊风暴的写放大）。
func (l *Logic) readFanout(msg *model.Message) {
	members, _ := l.convs.Members(msg.Envelope.ConversationID)
	for _, uid := range members {
		l.pusher.PushToUser(uid, msg)
	}
}

// HandleSync 处理客户端补洞请求：返回 sinceSeq 之后的消息（设计文档 4.3）。
func (l *Logic) HandleSync(conversationID string, sinceSeq int64, limit int) ([]*model.Message, error) {
	return l.msgs.ListByConv(conversationID, sinceSeq, limit)
}
