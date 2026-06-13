package comet

import (
	"encoding/binary"
	"log"
	"net"
	"sync/atomic"
	"time"

	"github.com/wangxfholly/goim-cs/internal/protocol"
)

// Handler 由逻辑层注入，处理上行业务帧（如 CmdSend / CmdSync）。
// 返回的帧（如发送 ACK）会被异步推回该连接。
type Handler interface {
	OnAuth(connID int64, body []byte) (uid int64, reply []byte, err error)
	OnMessage(c *Conn, f *protocol.Frame)
}

// Server 是长连接网关 TCP 服务（设计文档 9.1：netpoll 等价事件驱动）。
type Server struct {
	bm           *BucketManager
	handler      Handler
	connIDSeq    int64
	heartbeat    time.Duration // 心跳超时，超过则判死连接（设计文档 9.3）
	sendBuf      int
}

// Config 网关配置。
type Config struct {
	Buckets          int
	SendBuffer       int
	HeartbeatTimeout time.Duration
}

// NewServer 创建网关服务。
func NewServer(cfg Config, h Handler) *Server {
	if cfg.Buckets == 0 {
		cfg.Buckets = 256
	}
	if cfg.SendBuffer == 0 {
		cfg.SendBuffer = 64
	}
	if cfg.HeartbeatTimeout == 0 {
		cfg.HeartbeatTimeout = 90 * time.Second
	}
	return &Server{
		bm:        NewBucketManager(cfg.Buckets),
		handler:   h,
		heartbeat: cfg.HeartbeatTimeout,
		sendBuf:   cfg.SendBuffer,
	}
}

// Buckets 暴露分片管理器，供 Job 层推送使用。
func (s *Server) Buckets() *BucketManager { return s.bm }

// Serve 在 ln 上接受连接，直到 ln 关闭。
func (s *Server) Serve(ln net.Listener) error {
	for {
		nc, err := ln.Accept()
		if err != nil {
			return err
		}
		go s.handleConn(nc)
	}
}

func (s *Server) handleConn(nc net.Conn) {
	defer nc.Close()

	// 1. 鉴权握手：第一帧必须是 CmdAuth（设计文档 9.4 接入路由）。
	_ = nc.SetReadDeadline(time.Now().Add(10 * time.Second))
	first, err := protocol.ReadFrame(nc)
	if err != nil || first.Cmd != protocol.CmdAuth {
		return
	}
	uid, reply, err := s.handler.OnAuth(0, first.Body)
	if err != nil {
		return
	}
	connID := atomic.AddInt64(&s.connIDSeq, 1)
	c := NewConn(uid, connID, s.sendBuf)
	s.bm.Register(c)
	defer func() {
		s.bm.Unregister(c)
		c.Close()
	}()

	// 回发鉴权结果
	ack := &protocol.Frame{Version: protocol.Version, Cmd: protocol.CmdAuthReply, Seq: first.Seq, Body: reply}
	_, _ = nc.Write(ack.Encode())

	// 2. 启动写 goroutine（读写分离）。
	go s.writeLoop(nc, c)

	// 3. 读循环 + 心跳超时（设计文档 9.3）。
	for {
		_ = nc.SetReadDeadline(time.Now().Add(s.heartbeat))
		f, err := protocol.ReadFrame(nc)
		if err != nil {
			return // 含心跳超时：判定死连接，清理
		}
		switch f.Cmd {
		case protocol.CmdHeartbeat:
			c.Push(&protocol.Frame{Version: protocol.Version, Cmd: protocol.CmdHeartbeatReply, Seq: f.Seq})
		default:
			s.handler.OnMessage(c, f)
		}
	}
}

func (s *Server) writeLoop(nc net.Conn, c *Conn) {
	for {
		select {
		case <-c.Closed():
			return
		case f := <-c.Recv():
			_ = nc.SetWriteDeadline(time.Now().Add(5 * time.Second))
			if _, err := nc.Write(f.Encode()); err != nil {
				c.Close()
				return
			}
		}
	}
}

// helper: 把 int64 编进 body（demo 用）
func putInt64(v int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(v))
	return b
}

var _ = putInt64
var _ = log.Println
