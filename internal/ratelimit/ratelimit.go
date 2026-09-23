// Package ratelimit is a per-IP token bucket for write endpoints (abuse
// control on a public shortener). Off by default: RATE_LIMIT=<req/s per IP>
// enables it; RATE_LIMIT_BURST sets bucket capacity (default = RATE_LIMIT).
// POST /api/shorten costs 1 token; /api/shorten/bulk costs urls count.
package ratelimit

import (
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const (
	shards  = 64
	maxKeys = 1 << 12 // per shard — bounds tracked IPs to ~256k
	idleMs  = 60_000
)

type bucket struct {
	tokens float64
	lastMs int64
}

// Limiter is a sharded token bucket keyed by client IP (FNV-1a hashed).
type Limiter struct {
	shards [shards]struct {
		mu sync.Mutex
		m  map[uint64]*bucket
	}
	rate  float64 // tokens per second
	burst float64 // bucket capacity
}

// Limited counts requests rejected by the limiter (exported via /metrics).
var Limited atomic.Uint64

func ipKey(ip string) uint64 {
	var h uint64 = 0xcbf29ce484222325
	for i := 0; i < len(ip); i++ {
		h ^= uint64(ip[i])
		h *= 0x100000001b3
	}
	return h
}

// New builds a limiter from env. RATE_LIMIT<=0 disables it.
func New() *Limiter {
	rate, _ := strconv.ParseFloat(os.Getenv("RATE_LIMIT"), 64)
	burst, _ := strconv.ParseFloat(os.Getenv("RATE_LIMIT_BURST"), 64)
	if burst <= 0 {
		burst = rate
	}
	if burst < 1 {
		burst = 1
	}
	l := &Limiter{rate: rate, burst: burst}
	for i := range l.shards {
		l.shards[i].m = make(map[uint64]*bucket)
	}
	return l
}

// Allow spends cost tokens from ip's bucket; true when allowed. Disabled
// limiter always allows.
func (l *Limiter) Allow(ip string, cost float64) bool {
	if l.rate <= 0 {
		return true
	}
	k := ipKey(ip)
	sh := &l.shards[k&(shards-1)]
	now := time.Now().UnixMilli()
	sh.mu.Lock()
	if len(sh.m) >= maxKeys {
		for k2, b := range sh.m {
			if now-b.lastMs > idleMs {
				delete(sh.m, k2)
			}
		}
	}
	b := sh.m[k]
	if b == nil {
		b = &bucket{tokens: l.burst, lastMs: now}
		sh.m[k] = b
	}
	b.tokens += float64(now-b.lastMs) / 1000 * l.rate
	if b.tokens > l.burst {
		b.tokens = l.burst
	}
	b.lastMs = now
	ok := b.tokens >= cost
	if ok {
		b.tokens -= cost
	}
	sh.mu.Unlock()
	return ok
}
