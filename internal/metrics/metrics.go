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

// ---- counters for the Prometheus /metrics endpoint ----
// Ops indexes: redirect, shorten, shorten_bulk, update, delete, list,
// stats, health, metrics, ui, other.
var Ops = [11]string{
	"redirect", "shorten", "shorten_bulk", "update", "delete", "list",
	"stats", "health", "metrics", "ui", "other",
}

var (
	opCounts     [11]atomic.Int64
	statusCounts [4]atomic.Int64 // 2xx 3xx 4xx 5xx
	cacheHit     atomic.Int64
	cacheMiss    atomic.Int64
	storeReads   atomic.Int64
	storeReadUS  atomic.Int64
	storeWrites  atomic.Int64
	linksTotal   atomic.Int64
)

func Op(i int)           { opCounts[i].Add(1) }
func CacheHit()          { cacheHit.Add(1) }
func CacheMiss()         { cacheMiss.Add(1) }
func StoreRead(us int64) { storeReads.Add(1); storeReadUS.Add(us) }
func StoreWrite()        { storeWrites.Add(1) }
func LinksDelta(n int64) { linksTotal.Add(n) }

// Status records a response status code into its class bucket.
func Status(code int) {
	var i int
	switch {
	case code >= 200 && code < 300:
		i = 0
	case code >= 300 && code < 400:
		i = 1
	case code >= 400 && code < 500:
		i = 2
	default:
		i = 3
	}
	statusCounts[i].Add(1)
}

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

func pline(b []byte, m, labels string, v int64) []byte {
	b = append(b, m...)
	if labels != "" {
		b = append(b, '{')
		b = append(b, labels...)
		b = append(b, '}')
	}
	b = append(b, ' ')
	b = strconv.AppendInt(b, v, 10)
	return append(b, '\n')
}

// Prometheus renders the text exposition served at /metrics.
func Prometheus(rateLimited uint64) []byte {
	var b []byte
	b = append(b, "# HELP shrt_requests_total Requests by operation\n"...)
	b = append(b, "# TYPE shrt_requests_total counter\n"...)
	for i, name := range Ops {
		b = pline(b, "shrt_requests_total", `op="`+name+`"`, opCounts[i].Load())
	}
	b = append(b, "# HELP shrt_responses_total Responses by status class\n"...)
	b = append(b, "# TYPE shrt_responses_total counter\n"...)
	for i, cls := range [4]string{"2xx", "3xx", "4xx", "5xx"} {
		b = pline(b, "shrt_responses_total", `class="`+cls+`"`, statusCounts[i].Load())
	}
	b = append(b, "# HELP shrt_cache_lookups_total Local hot-cache lookups\n"...)
	b = append(b, "# TYPE shrt_cache_lookups_total counter\n"...)
	b = pline(b, "shrt_cache_lookups_total", `result="hit"`, cacheHit.Load())
	b = pline(b, "shrt_cache_lookups_total", `result="miss"`, cacheMiss.Load())
	b = append(b, "# HELP shrt_store_reads_total Backing-store point reads (cache misses)\n"...)
	b = append(b, "# TYPE shrt_store_reads_total counter\n"...)
	b = pline(b, "shrt_store_reads_total", "", storeReads.Load())
	b = append(b, "# HELP shrt_store_read_us_total Cumulative backing-store read latency (us)\n"...)
	b = append(b, "# TYPE shrt_store_read_us_total counter\n"...)
	b = pline(b, "shrt_store_read_us_total", "", storeReadUS.Load())
	b = append(b, "# HELP shrt_store_writes_total Backing-store writes\n"...)
	b = append(b, "# TYPE shrt_store_writes_total counter\n"...)
	b = pline(b, "shrt_store_writes_total", "", storeWrites.Load())
	b = append(b, "# HELP shrt_rate_limited_total Requests rejected by the rate limiter\n"...)
	b = append(b, "# TYPE shrt_rate_limited_total counter\n"...)
	b = pline(b, "shrt_rate_limited_total", "", int64(rateLimited))
	b = append(b, "# HELP shrt_links_total Live links created minus deleted\n"...)
	b = append(b, "# TYPE shrt_links_total gauge\n"...)
	b = pline(b, "shrt_links_total", "", linksTotal.Load())
	b = append(b, "# HELP shrt_uptime_seconds Process uptime\n"...)
	b = append(b, "# TYPE shrt_uptime_seconds gauge\n"...)
	b = pline(b, "shrt_uptime_seconds", "", (time.Now().UnixMilli()-started)/1000)
	b = append(b, '\n')
	return b
}
