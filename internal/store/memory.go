package store

import (
	"sort"
	"sync"

	"github.com/wangxfholly/goim-cs/internal/protocol/model"
)

// MemoryStore 是 MessageStore + InboxStore + ConversationStore 的内存实现。
// 用于本地 demo 与单测；生产替换为 MySQL/HBase/Redis。
type MemoryStore struct {
	mu       sync.RWMutex
	byConv   map[string][]*model.Message // conversation_id -> 按 conv_seq 有序
	dedup    map[string]struct{}         // client_msg_id 去重集合
	inbox    map[int64][]InboxEntry      // uid -> 收件箱
	members  map[string]map[int64]bool   // conversation_id -> 成员集合
}

// NewMemoryStore 创建内存存储。
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		byConv:  make(map[string][]*model.Message),
		dedup:   make(map[string]struct{}),
		inbox:   make(map[int64][]InboxEntry),
		members: make(map[string]map[int64]bool),
	}
}

// Save 实现 MessageStore。
func (m *MemoryStore) Save(msg *model.Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cid := msg.Envelope.ConversationID
	m.byConv[cid] = append(m.byConv[cid], msg)
	return nil
}

// ListByConv 实现 MessageStore：补洞拉取。
func (m *MemoryStore) ListByConv(conversationID string, sinceSeq int64, limit int) ([]*model.Message, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	all := m.byConv[conversationID]
	out := make([]*model.Message, 0, limit)
	for _, msg := range all {
		if msg.Identity.ConvSeq > sinceSeq {
			out = append(out, msg)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Identity.ConvSeq < out[j].Identity.ConvSeq
	})
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// Dedup 实现 MessageStore：基于 client_msg_id 幂等。
func (m *MemoryStore) Dedup(clientMsgID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.dedup[clientMsgID]; ok {
		return false
	}
	m.dedup[clientMsgID] = struct{}{}
	return true
}

// Append 实现 InboxStore。
func (m *MemoryStore) Append(uid int64, conversationID string, convSeq, serverMsgID int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.inbox[uid] = append(m.inbox[uid], InboxEntry{uid, conversationID, convSeq, serverMsgID})
	return nil
}

// List 实现 InboxStore。
func (m *MemoryStore) List(uid int64, sinceSeq int64, limit int) ([]InboxEntry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]InboxEntry, 0, limit)
	for _, e := range m.inbox[uid] {
		if e.ConvSeq > sinceSeq {
			out = append(out, e)
			if len(out) >= limit {
				break
			}
		}
	}
	return out, nil
}

// Members 实现 ConversationStore。
func (m *MemoryStore) Members(conversationID string) ([]int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	set := m.members[conversationID]
	out := make([]int64, 0, len(set))
	for uid := range set {
		out = append(out, uid)
	}
	return out, nil
}

// AddMember 实现 ConversationStore。
func (m *MemoryStore) AddMember(conversationID string, uid int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.members[conversationID] == nil {
		m.members[conversationID] = make(map[int64]bool)
	}
	m.members[conversationID][uid] = true
	return nil
}

// MemberCount 实现 ConversationStore。
func (m *MemoryStore) MemberCount(conversationID string) int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.members[conversationID])
}

var (
	_ MessageStore      = (*MemoryStore)(nil)
	_ InboxStore        = (*MemoryStore)(nil)
	_ ConversationStore = (*MemoryStore)(nil)
)
