// Package metrics tracks in-process request counters: a per-second ring
// buffer plus totals, reported via /api/metrics.
package metrics

import (
	"strconv"
	"sync/atomic"
	"time"
)

const N = 60

var (
	buckets [N]atomic.Int64
	cur     atomic.Int64
	total   atomic.Int64
	started = time.Now().UnixMilli()
)

func init() {
	cur.Store(started / 1000)
}

func Tick() {
	total.Add(1)
	s := time.Now().UnixMilli() / 1000
	c := cur.Load()
	if s != c {
		if cur.CompareAndSwap(c, s) {
			for t := c + 1; t <= s; t++ {
				buckets[t%N].Store(0)
			}
		} else {
			s = cur.Load()
		}
	}
	buckets[s%N].Add(1)
}

// Snapshot renders {"req_s","total","uptime_s","per_second":[31]} — the last
// 30 complete seconds plus the current partial second.
func Snapshot() []byte {
	s := time.Now().UnixMilli() / 1000
	c := cur.Load()
	var window [31]int64
	for i := 0; i < 31; i++ {
		t := s - 30 + int64(i)
		if t <= c && c-t < N {
			window[i] = buckets[t%N].Load()
		}
	}
	var last5 int64
	for _, v := range window[25:30] {
		last5 += v
	}
	reqS := float64(last5) / 5

	b := make([]byte, 0, 320)
	b = append(b, `{"req_s":`...)
	b = strconv.AppendFloat(b, float64(int64(reqS*10+0.5))/10, 'f', -1, 64)
	b = append(b, `,"total":`...)
	b = strconv.AppendInt(b, total.Load(), 10)
	b = append(b, `,"uptime_s":`...)
	b = strconv.AppendInt(b, (time.Now().UnixMilli()-started)/1000, 10)
	b = append(b, `,"per_second":[`...)
	for i, v := range window {
		if i > 0 {
			b = append(b, ',')
		}
		b = strconv.AppendInt(b, v, 10)
	}
	return append(b, ']', '}')
}
