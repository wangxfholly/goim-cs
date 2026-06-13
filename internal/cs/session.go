// Package cs 是设计文档「六、客服 IM 场景扩展」与「十二、坐席分配与高并发排队」的落地。
//
// 客服 IM = 通用消息通道 + 业务编排层。本包实现:
//   - 会话生命周期状态机（设计文档 6.2）
//   - 坐席分配算法：最少会话优先（设计文档 12.2）
//   - 高并发排队队列（设计文档 12.3）
package cs

import "errors"

// SessionState 客服会话状态（设计文档 6.2 状态机）。
type SessionState int

const (
	StateQueuing SessionState = iota // 排队中
	StateBot                         // 机器人接待
	StateServing                     // 人工服务中
	StateClosed                      // 已结束
)

func (s SessionState) String() string {
	switch s {
	case StateQueuing:
		return "queuing"
	case StateBot:
		return "bot"
	case StateServing:
		return "serving"
	case StateClosed:
		return "closed"
	default:
		return "unknown"
	}
}

// allowedTransitions 定义合法状态流转（设计文档 6.2）。
var allowedTransitions = map[SessionState][]SessionState{
	StateQueuing: {StateBot, StateServing, StateClosed},
	StateBot:     {StateServing, StateClosed}, // 机器人转人工 / 结束
	StateServing: {StateClosed},               // 人工结束
	StateClosed:  {},                          // 终态
}

// ErrIllegalTransition 非法状态流转。
var ErrIllegalTransition = errors.New("cs: illegal session state transition")

// Session 一次客服会话。
type Session struct {
	ID      string
	UserUID int64
	AgentID int64 // 0 表示尚未分配人工
	State   SessionState
}

// Transition 执行状态流转，非法流转返回错误（状态机守卫）。
func (s *Session) Transition(to SessionState) error {
	for _, allowed := range allowedTransitions[s.State] {
		if allowed == to {
			s.State = to
			return nil
		}
	}
	return ErrIllegalTransition
}
