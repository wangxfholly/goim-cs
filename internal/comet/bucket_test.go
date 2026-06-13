package comet

import (
	"testing"

	"github.com/wangxfholly/goim-cs/internal/protocol"
)

func TestBucketPushToUser(t *testing.T) {
	bm := NewBucketManager(16)
	// 同一用户两端在线
	c1 := NewConn(1001, 1, 8)
	c2 := NewConn(1001, 2, 8)
	bm.Register(c1)
	bm.Register(c2)

	f := &protocol.Frame{Version: protocol.Version, Cmd: protocol.CmdPush, Body: []byte("hi")}
	n := bm.PushToUser(1001, f)
	if n != 2 {
		t.Fatalf("delivered = %d, want 2", n)
	}
	if got := <-c1.Recv(); got != f {
		t.Fatal("c1 did not receive frame")
	}
	if got := <-c2.Recv(); got != f {
		t.Fatal("c2 did not receive frame")
	}
}

func TestBucketUnregister(t *testing.T) {
	bm := NewBucketManager(8)
	c := NewConn(2002, 10, 4)
	bm.Register(c)
	if bm.Count() != 1 {
		t.Fatalf("count = %d, want 1", bm.Count())
	}
	bm.Unregister(c)
	if bm.Count() != 0 {
		t.Fatalf("count = %d, want 0 after unregister", bm.Count())
	}
	if n := bm.PushToUser(2002, &protocol.Frame{}); n != 0 {
		t.Fatalf("push after unregister delivered %d, want 0", n)
	}
}

func TestBucketPowerOfTwo(t *testing.T) {
	bm := NewBucketManager(100) // 应向上取到 128
	if len(bm.buckets) != 128 {
		t.Fatalf("buckets = %d, want 128", len(bm.buckets))
	}
}
