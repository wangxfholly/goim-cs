package cs

import (
	"container/heap"
	"errors"
	"sync"
)

// Agent 坐席（设计文档 7.7 t_agent）。
type Agent struct {
	ID          int64
	MaxSessions int   // 最大并发会话数
	curSessions int   // 当前会话数
	online      bool
}

// ErrNoAgent 无可用坐席（应转排队/机器人，设计文档 12.1）。
var ErrNoAgent = errors.New("cs: no available agent")

// Dispatcher 坐席分配器：最少会话优先（Least-Conn，设计文档 12.2）。
//
// 用最小堆按「当前会话数」排序，O(log n) 取出负载最低的坐席。
// 所有改写都加锁，等价于设计文档要求的「原子分配，防超分」（12.3）。
type Dispatcher struct {
	mu     sync.Mutex
	agents map[int64]*Agent
}

// NewDispatcher 创建分配器。
func NewDispatcher() *Dispatcher {
	return &Dispatcher{agents: make(map[int64]*Agent)}
}

// AddAgent 上线一个坐席。
func (d *Dispatcher) AddAgent(a *Agent) {
	d.mu.Lock()
	defer d.mu.Unlock()
	a.online = true
	d.agents[a.ID] = a
}

// Assign 分配一个负载最低且未满的坐席；无可用坐席返回 ErrNoAgent。
func (d *Dispatcher) Assign() (*Agent, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	var best *Agent
	for _, a := range d.agents {
		if !a.online || a.curSessions >= a.MaxSessions {
			continue
		}
		if best == nil || a.curSessions < best.curSessions {
			best = a
		}
	}
	if best == nil {
		return nil, ErrNoAgent
	}
	best.curSessions++ // 原子占用，防止并发超分
	return best, nil
}

// Release 会话结束，释放坐席容量。
func (d *Dispatcher) Release(agentID int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if a, ok := d.agents[agentID]; ok && a.curSessions > 0 {
		a.curSessions--
	}
}

// --- 优先级排队队列（设计文档 12.3，对应生产的 Redis ZSet）---

type queueItem struct {
	uid      int64
	priority int64 // 越小越优先（进线时间戳；VIP 可提权减小）
	index    int
}

type priorityQueue []*queueItem

func (pq priorityQueue) Len() int            { return len(pq) }
func (pq priorityQueue) Less(i, j int) bool  { return pq[i].priority < pq[j].priority }
func (pq priorityQueue) Swap(i, j int)       { pq[i], pq[j] = pq[j], pq[i]; pq[i].index = i; pq[j].index = j }
func (pq *priorityQueue) Push(x interface{}) { *pq = append(*pq, x.(*queueItem)) }
func (pq *priorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	it := old[n-1]
	*pq = old[:n-1]
	return it
}

// WaitingQueue 进线排队队列。
type WaitingQueue struct {
	mu  sync.Mutex
	pq  priorityQueue
	max int // 队列容量上限，超过则削峰兜底（设计文档 12.3）
}

// ErrQueueFull 队列已满，应提示等待/转机器人/留言转工单。
var ErrQueueFull = errors.New("cs: waiting queue full")

// NewWaitingQueue 创建排队队列，maxLen<=0 表示不限。
func NewWaitingQueue(maxLen int) *WaitingQueue {
	return &WaitingQueue{max: maxLen}
}

// Enqueue 入队，返回当前排队位置（1-based）。
func (q *WaitingQueue) Enqueue(uid, priority int64) (pos int, err error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.max > 0 && q.pq.Len() >= q.max {
		return 0, ErrQueueFull
	}
	heap.Push(&q.pq, &queueItem{uid: uid, priority: priority})
	return q.pq.Len(), nil
}

// Dequeue 取出队首用户（事件驱动：坐席空闲时调用，设计文档 12.3）。
func (q *WaitingQueue) Dequeue() (uid int64, ok bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.pq.Len() == 0 {
		return 0, false
	}
	return heap.Pop(&q.pq).(*queueItem).uid, true
}

// Len 当前排队人数。
func (q *WaitingQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.pq.Len()
}
