package idgen

import (
	"sync"
	"testing"
)

func TestSnowflakeUnique(t *testing.T) {
	sf, err := NewSnowflake(1)
	if err != nil {
		t.Fatal(err)
	}
	const n = 100000
	seen := make(map[int64]struct{}, n)
	for i := 0; i < n; i++ {
		id, err := sf.NextID()
		if err != nil {
			t.Fatalf("NextID: %v", err)
		}
		if _, dup := seen[id]; dup {
			t.Fatalf("duplicate id: %d", id)
		}
		seen[id] = struct{}{}
	}
}

func TestSnowflakeConcurrent(t *testing.T) {
	sf, _ := NewSnowflake(7)
	var mu sync.Mutex
	seen := make(map[int64]struct{})
	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 5000; i++ {
				id, err := sf.NextID()
				if err != nil {
					t.Errorf("NextID: %v", err)
					return
				}
				mu.Lock()
				if _, dup := seen[id]; dup {
					t.Errorf("duplicate id under concurrency: %d", id)
				}
				seen[id] = struct{}{}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
}

func TestSeqAllocatorMonotonic(t *testing.T) {
	a := NewSeqAllocator()
	last := int64(0)
	for i := 0; i < 1000; i++ {
		s := a.NextSeq("conv-1")
		if s != last+1 {
			t.Fatalf("seq not monotonic: got %d want %d", s, last+1)
		}
		last = s
	}
	// 不同会话相互独立
	if a.NextSeq("conv-2") != 1 {
		t.Fatal("conv-2 should start from 1")
	}
}
