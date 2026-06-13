// Package comet 是设计文档「二、长连接网关设计」与「九、长连接网关核心实现」的落地。
//
// 设计取舍:
//   - Go 的 runtime netpoll 底层就是 epoll(Linux)，每个 goroutine 阻塞读时
//     不占用 OS 线程，等价于事件驱动；因此用「每连接一个读 goroutine」即可，
//     无需手写 epoll。海量连接的真正瓶颈是 FD + 内存 + 锁竞争（设计文档 2.1）。
//   - 连接打散到分片 Bucket（设计文档 9.2），广播/查找/踢连接只锁单分片。
package comet

import (
	"sync"

	"github.com/wangxfholly/goim-cs/internal/protocol"
)

// Conn 表示一条客户端长连接。
type Conn struct {
	UID    int64
	ConnID int64
	send   chan *protocol.Frame // 下行写队列，与读 goroutine 解耦
	closed chan struct{}
	once   sync.Once
}

// NewConn 创建连接，sendBuf 为下行缓冲队列长度。
func NewConn(uid, connID int64, sendBuf int) *Conn {
	return &Conn{
		UID:    uid,
		ConnID: connID,
		send:   make(chan *protocol.Frame, sendBuf),
		closed: make(chan struct{}),
	}
}

// Push 向连接投递一帧（非阻塞）。队列满则丢弃并返回 false，
// 由上层决定是否走「store-first + 离线补洞」兜底（设计文档 10.3 / 9.3）。
func (c *Conn) Push(f *protocol.Frame) bool {
	select {
	case <-c.closed:
		return false
	case c.send <- f:
		return true
	default:
		return false // 慢消费者保护：不阻塞整个 Bucket
	}
}

// Recv 返回下行通道，供写 goroutine 消费。
func (c *Conn) Recv() <-chan *protocol.Frame { return c.send }

// Close 幂等关闭连接。
func (c *Conn) Close() {
	c.once.Do(func() { close(c.closed) })
}

// Closed 返回关闭信号通道。
func (c *Conn) Closed() <-chan struct{} { return c.closed }
