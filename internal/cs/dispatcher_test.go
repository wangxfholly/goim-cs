package cs

import "testing"

func TestSessionStateMachine(t *testing.T) {
	s := &Session{ID: "s1", State: StateQueuing}
	// 合法：排队 -> 人工
	if err := s.Transition(StateServing); err != nil {
		t.Fatalf("queuing->serving should be allowed: %v", err)
	}
	// 合法：人工 -> 结束
	if err := s.Transition(StateClosed); err != nil {
		t.Fatalf("serving->closed should be allowed: %v", err)
	}
	// 非法：终态不可再流转
	if err := s.Transition(StateServing); err != ErrIllegalTransition {
		t.Fatalf("closed->serving should be illegal, got %v", err)
	}
}

func TestDispatcherLeastConn(t *testing.T) {
	d := NewDispatcher()
	d.AddAgent(&Agent{ID: 1, MaxSessions: 2})
	d.AddAgent(&Agent{ID: 2, MaxSessions: 2})

	// 第一次分配：两人都空，任取其一
	a1, err := d.Assign()
	if err != nil {
		t.Fatal(err)
	}
	// 第二次：应分给另一个（负载更低）
	a2, _ := d.Assign()
	if a1.ID == a2.ID {
		t.Fatal("least-conn should pick the other idle agent")
	}
	// 第三、四次填满
	_, _ = d.Assign()
	_, _ = d.Assign()
	// 第五次：全满，应无可用坐席
	if _, err := d.Assign(); err != ErrNoAgent {
		t.Fatalf("want ErrNoAgent, got %v", err)
	}
	// 释放后又可分配
	d.Release(a1.ID)
	if _, err := d.Assign(); err != nil {
		t.Fatalf("after release should assign: %v", err)
	}
}

func TestWaitingQueuePriority(t *testing.T) {
	q := NewWaitingQueue(0)
	_, _ = q.Enqueue(100, 3000) // 普通用户，进线晚
	_, _ = q.Enqueue(200, 1000) // VIP，提权（priority 小）
	_, _ = q.Enqueue(300, 2000)

	// 出队顺序应按 priority：200(VIP) -> 300 -> 100
	want := []int64{200, 300, 100}
	for _, w := range want {
		uid, ok := q.Dequeue()
		if !ok || uid != w {
			t.Fatalf("dequeue = %d, want %d", uid, w)
		}
	}
}

func TestWaitingQueueFull(t *testing.T) {
	q := NewWaitingQueue(1)
	if _, err := q.Enqueue(1, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := q.Enqueue(2, 1); err != ErrQueueFull {
		t.Fatalf("want ErrQueueFull, got %v", err)
	}
}
