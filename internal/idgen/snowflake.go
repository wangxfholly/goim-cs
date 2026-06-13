// Package idgen 实现设计文档「5.2 消息 ID 设计（三个独立职责）」。
//
// 三个 ID 各司其职，互不替代:
//   - server_msg_id: Snowflake，64bit 全局唯一（本包 Snowflake）
//   - conv_seq     : 会话内严格递增，用于时序与补洞（本包 SeqAllocator，生产用 Redis INCR）
//   - client_msg_id: 客户端生成的 UUID，用于幂等去重（不在服务端生成）
package idgen

import (
	"errors"
	"sync"
	"time"
)

// Snowflake 64bit 结构: 1 符号位 + 41 时间戳(ms) + 10 机器号 + 12 序列号。
//
// 处理时钟回拨（设计文档 5.2）: 若检测到当前时间小于上次时间戳，直接报错拒绝发号，
// 避免生成重复或乱序 ID；调用方可重试或告警。
type Snowflake struct {
	mu        sync.Mutex
	epoch     int64 // 自定义纪元(ms)
	machineID int64 // 0..1023
	lastTs    int64
	seq       int64
}

const (
	machineBits = 10
	seqBits     = 12
	maxMachine  = -1 ^ (-1 << machineBits) // 1023
	maxSeq      = -1 ^ (-1 << seqBits)     // 4095

	machineShift = seqBits
	tsShift      = seqBits + machineBits
)

// DefaultEpoch 2024-01-01 00:00:00 UTC 对应的毫秒时间戳。
const DefaultEpoch int64 = 1704067200000

// ErrClockBackwards 检测到时钟回拨。
var ErrClockBackwards = errors.New("idgen: clock moved backwards, refuse to generate id")

// NewSnowflake 创建发号器，machineID 必须在 [0,1023]。
func NewSnowflake(machineID int64) (*Snowflake, error) {
	if machineID < 0 || machineID > maxMachine {
		return nil, errors.New("idgen: machineID out of range [0,1023]")
	}
	return &Snowflake{epoch: DefaultEpoch, machineID: machineID, lastTs: -1}, nil
}

// NextID 生成下一个全局唯一 ID。
func (s *Snowflake) NextID() (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now().UnixMilli()
	if now < s.lastTs {
		// 时钟回拨：拒绝发号（设计文档要求的处理策略）。
		return 0, ErrClockBackwards
	}
	if now == s.lastTs {
		s.seq = (s.seq + 1) & maxSeq
		if s.seq == 0 {
			// 当前毫秒序列耗尽，自旋到下一毫秒。
			for now <= s.lastTs {
				now = time.Now().UnixMilli()
			}
		}
	} else {
		s.seq = 0
	}
	s.lastTs = now

	id := ((now - s.epoch) << tsShift) |
		(s.machineID << machineShift) |
		s.seq
	return id, nil
}
