package ratelimit

import "testing"

func TestTokenBucket(t *testing.T) {
	l := &Limiter{rate: 1, burst: 3}
	for i := range l.shards {
		l.shards[i].m = make(map[uint64]*bucket)
	}
	for i := 0; i < 3; i++ {
		if !l.Allow("1.2.3.4", 1) {
			t.Fatalf("burst %d rejected", i)
		}
	}
	if l.Allow("1.2.3.4", 1) {
		t.Fatal("burst should be exhausted")
	}
	if !l.Allow("5.6.7.8", 1) {
		t.Fatal("other IP should have its own bucket")
	}
}
