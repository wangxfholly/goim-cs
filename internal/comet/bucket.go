package comet

import (
	"sync"

	"github.com/wangxfholly/goim-cs/internal/protocol"
)

// Bucket 是连接的分片容器（设计文档 9.2）。每个 Bucket 独立持锁，
// 把全局大锁的竞争打散到 N 个分片，是单机百万连接的关键。
type Bucket struct {
	mu    sync.RWMutex
	conns map[int64]*Conn // connID -> Conn
	users map[int64][]*Conn // uid -> 多端连接（一个用户可多设备在线）
}

func newBucket() *Bucket {
	return &Bucket{
		conns: make(map[int64]*Conn),
		users: make(map[int64][]*Conn),
	}
}

// BucketManager 管理 N 个分片。N 必须是 2 的幂，便于用位与取模。
type BucketManager struct {
	buckets []*Bucket
	mask    int64
}

// NewBucketManager 创建分片管理器，n 向上取到 2 的幂。
func NewBucketManager(n int) *BucketManager {
	size := 1
	for size < n {
		size <<= 1
	}
	bm := &BucketManager{
		buckets: make([]*Bucket, size),
		mask:    int64(size - 1),
	}
	for i := range bm.buckets {
		bm.buckets[i] = newBucket()
	}
	return bm
}

func (bm *BucketManager) bucket(connID int64) *Bucket {
	return bm.buckets[connID&bm.mask]
}

// Register 登记一条连接。
func (bm *BucketManager) Register(c *Conn) {
	b := bm.bucket(c.ConnID)
	b.mu.Lock()
	b.conns[c.ConnID] = c
	b.users[c.UID] = append(b.users[c.UID], c)
	b.mu.Unlock()
}

// Unregister 注销一条连接。
func (bm *BucketManager) Unregister(c *Conn) {
	b := bm.bucket(c.ConnID)
	b.mu.Lock()
	delete(b.conns, c.ConnID)
	conns := b.users[c.UID]
	for i, x := range conns {
		if x == c {
			b.users[c.UID] = append(conns[:i], conns[i+1:]...)
			break
		}
	}
	if len(b.users[c.UID]) == 0 {
		delete(b.users, c.UID)
	}
	b.mu.Unlock()
}

// PushToUser 把一帧推给某用户的全部在线端。返回成功投递的连接数。
func (bm *BucketManager) PushToUser(uid int64, f *protocol.Frame) int {
	delivered := 0
	for _, b := range bm.buckets {
		b.mu.RLock()
		for _, c := range b.users[uid] {
			if c.Push(f) {
				delivered++
			}
		}
		b.mu.RUnlock()
	}
	return delivered
}

// Broadcast 向所有连接广播（仅锁单分片，逐片遍历）。
func (bm *BucketManager) Broadcast(f *protocol.Frame) {
	for _, b := range bm.buckets {
		b.mu.RLock()
		for _, c := range b.conns {
			c.Push(f)
		}
		b.mu.RUnlock()
	}
}

// Count 返回当前连接总数（用于负载上报）。
func (bm *BucketManager) Count() int {
	total := 0
	for _, b := range bm.buckets {
		b.mu.RLock()
		total += len(b.conns)
		b.mu.RUnlock()
	}
	return total
}
