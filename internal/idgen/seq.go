package idgen

import "sync"

// SeqAllocator 为每个会话分配严格递增的 conv_seq（设计文档 5.2）。
//
// 这是内存版实现，用于本地 demo / 单测；生产环境应替换为 Redis INCR
// （key = conv:seq:{conversation_id}），保证跨进程严格递增。SeqStore 接口
// 让上层逻辑无需感知底层是内存还是 Redis。
type SeqStore interface {
	// NextSeq 返回指定会话的下一个严格递增序号（从 1 开始）。
	NextSeq(conversationID string) int64
	// MaxSeq 返回当前已分配的最大序号，用于客户端补洞比对。
	MaxSeq(conversationID string) int64
}

// SeqAllocator 内存实现。
type SeqAllocator struct {
	mu   sync.Mutex
	seqs map[string]int64
}

// NewSeqAllocator 创建内存序号分配器。
func NewSeqAllocator() *SeqAllocator {
	return &SeqAllocator{seqs: make(map[string]int64)}
}

// NextSeq 实现 SeqStore。
func (a *SeqAllocator) NextSeq(conversationID string) int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.seqs[conversationID]++
	return a.seqs[conversationID]
}

// MaxSeq 实现 SeqStore。
func (a *SeqAllocator) MaxSeq(conversationID string) int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.seqs[conversationID]
}

var _ SeqStore = (*SeqAllocator)(nil)
