// kvstore.go — external-KV backend (DragonflyDB / Redis / any RESP server).
// The corpus lives in the KV store; this process keeps only a bounded hot
// FIFO cache + batched hit counters — memory stays flat as links grow.
//
// Keys:  l:{code} -> "{expires_ms}|{created_ms}|{url}"  (PX self-evicts)
//        h:{code} -> hit counter (INCRBY, flushed in 5ms batches)
//
// Multi-instance: the KV IS the shared state — no tailing, no convergence,
// admin mutations work on any node.

package store

import (
	"os"
	"crypto/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	"shrt-go/internal/base62"
)

type cacheEnt struct {
	u  string
	e  int64 // expires_at ms (0 = never)
	at int64 // cached_at ms (staleness bound)
}

type cacheShard struct {
	mu    sync.Mutex
	m     map[string]cacheEnt
	order []string // FIFO; oldest first, head-compacted
	head  int
}

type kvCache struct {
	shards [numShards]cacheShard
	cap    int
	ttlMs  int64
}

func newKvCache(capTotal int, ttlMs int64) *kvCache {
	c := &kvCache{cap: capTotal / numShards, ttlMs: ttlMs}
	if c.cap < 16 {
		c.cap = 16
	}
	for i := range c.shards {
		c.shards[i].m = make(map[string]cacheEnt, c.cap)
	}
	return c
}

func (c *kvCache) get(code string) (string, int64, bool) {
	sh := &c.shards[shardOf(code)]
	sh.mu.Lock()
	defer sh.mu.Unlock()
	e, ok := sh.m[code]
	if !ok {
		return "", 0, false
	}
	now := nowMs()
	if c.ttlMs > 0 && now-e.at > c.ttlMs {
		delete(sh.m, code)
		return "", 0, false
	}
	if e.e != 0 && e.e <= now {
		return "", 0, false
	}
	return e.u, e.e, true
}

func (c *kvCache) put(code, u string, e int64) {
	sh := &c.shards[shardOf(code)]
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if _, ok := sh.m[code]; ok {
		sh.m[code] = cacheEnt{u: u, e: e, at: nowMs()}
		return
	}
	for len(sh.m) >= c.cap && sh.head < len(sh.order) {
		old := sh.order[sh.head]
		sh.head++
		delete(sh.m, old)
	}
	if sh.head > 1024 && sh.head*2 >= len(sh.order) {
		sh.order = append([]string(nil), sh.order[sh.head:]...)
		sh.head = 0
	}
	sh.order = append(sh.order, code)
	sh.m[code] = cacheEnt{u: u, e: e, at: nowMs()}
}

func (c *kvCache) remove(code string) {
	sh := &c.shards[shardOf(code)]
	sh.mu.Lock()
	delete(sh.m, code)
	sh.mu.Unlock()
}

// KvStore implements API backed by an external RESP store.
type KvStore struct {
	kv       *Kv
	cache    *kvCache
	dirty    [numShards]struct {
		mu sync.Mutex
		m  map[string]int64
	}
	instance    int
	prefix      byte
	layoutHash  bool   // KV_LAYOUT=hash: fields in l:{shard%buckets} hashes
	buckets     uint64 // KV_BUCKETS — keep fields/bucket < hash-max-listpack-entries
	stop        chan struct{}
	wg          sync.WaitGroup
	capShard    int
}

// NewKV opens a KV-backed store. cacheEntries bounds local memory;
// cacheTTLMs bounds cross-node staleness (0 = never stale in-cache).
func NewKV(addr string, instance, cacheEntries int, cacheTTLMs int64) (*KvStore, error) {
	kv, err := Dial(addr)
	if err != nil {
		return nil, err
	}
	if instance < 0 {
		instance = 0
	}
	if instance >= maxInstances {
		instance = maxInstances - 1
	}
	s := &KvStore{
		kv:       kv,
		cache:    newKvCache(cacheEntries, cacheTTLMs),
		instance: instance,
		prefix:   base62.ALPHABET[instance],
		stop:     make(chan struct{}),
		capShard: cacheEntries / numShards,
	}
	if s.capShard < 16 {
		s.capShard = 16
	}
	s.cache.cap = s.capShard
	for i := range s.dirty {
		s.dirty[i].m = make(map[string]int64)
	}
	if os.Getenv("KV_LAYOUT") == "hash" {
		s.layoutHash = true
	}
	s.buckets = 1_000_000
	if v := os.Getenv("KV_BUCKETS"); v != "" {
		if n, err := strconv.ParseUint(v, 10, 32); err == nil && n > 0 {
			s.buckets = n
		}
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		t := time.NewTicker(flushMS * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-s.stop:
				return
			case <-t.C:
				_ = s.flushHits()
			}
		}
	}()
	if s.layoutHash {
		sweepMS := int64(3_600_000)
		if v := os.Getenv("KV_SWEEP_MS"); v != "" {
			if n, err := strconv.ParseInt(v, 10, 64); err == nil && n >= 50 {
				sweepMS = n
			}
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			t := time.NewTicker(time.Duration(sweepMS) * time.Millisecond)
			defer t.Stop()
			for {
				select {
				case <-s.stop:
					return
				case <-t.C:
					_ = s.sweepExpired()
				}
			}
		}()
	}
	return s, nil
}

func hfield(code string) string { return "h:" + code }
// bkey: hash-mode bucket key l:{shard(code) % buckets}
func (s *KvStore) bkey(code string) string {
	return "l:" + strconv.FormatUint(uint64(shardOf(code))%s.buckets, 10)
}

// sweepExpired HDELs fields whose embedded expiry is past — hash fields
// can't carry PX, so the janitor reclaims them.
func (s *KvStore) sweepExpired() error {
	var buckets [][]byte
	if err := s.kv.ScanEach("l:*", func(k []byte) { buckets = append(buckets, append([]byte(nil), k...)) }); err != nil {
		return err
	}
	now := nowMs()
	var dels [][][]byte
	for _, b := range buckets {
		var dead []string
		_ = s.kv.HScanEach(string(b), func(f, v []byte) {
			if len(f) >= 2 && f[0] == 'h' && f[1] == ':' {
				return
			}
			e, _, _, ok := decVal(v)
			if ok && e != 0 && e <= now {
				dead = append(dead, string(f))
			}
		})
		for _, f := range dead {
			dels = append(dels, [][]byte{[]byte("HDEL"), b, []byte(f)})
		}
	}
	if len(dels) > 0 {
		_, err := s.kv.pipe(dels)
		return err
	}
	return nil
}

// kvGet reads the link row from whichever layout is active.
func (s *KvStore) kvGet(code string) ([]byte, error) {
	if s.layoutHash {
		return s.kv.Hget([]byte(s.bkey(code)), []byte(code))
	}
	return s.kv.Get([]byte(lkey(code)))
}

func lkey(code string) string { return "l:" + code }
func hkey(code string) string { return "h:" + code }

// value codec "{e}|{c}|{u}" — legacy "{e}|{u}" decodes with c=0.
func encVal(e, c int64, u string) string {
	return strconv.FormatInt(e, 10) + "|" + strconv.FormatInt(c, 10) + "|" + u
}
func decVal(v []byte) (e, c int64, u string, ok bool) {
	s := string(v)
	p := strings.IndexByte(s, '|')
	if p < 0 {
		return 0, 0, "", false
	}
	e, err := strconv.ParseInt(s[:p], 10, 64)
	if err != nil {
		return 0, 0, "", false
	}
	rest := s[p+1:]
	if q := strings.IndexByte(rest, '|'); q >= 0 {
		c, err = strconv.ParseInt(rest[:q], 10, 64)
		if err != nil {
			return 0, 0, "", false
		}
		u = rest[q+1:]
	} else {
		u = rest
	}
	return e, c, u, true
}

func (s *KvStore) flushHits() error {
	if s.layoutHash {
		var deltas []hincr
		for i := range s.dirty {
			sh := &s.dirty[i]
			sh.mu.Lock()
			for c, n := range sh.m {
				deltas = append(deltas, hincr{s.bkey(c), hfield(c), n})
			}
			sh.m = make(map[string]int64, len(sh.m))
			sh.mu.Unlock()
		}
		return s.kv.HIncrByMany(deltas)
	}
	deltas := make(map[string]int64, 64)
	for i := range s.dirty {
		sh := &s.dirty[i]
		sh.mu.Lock()
		for c, n := range sh.m {
			deltas[hkey(c)] += n
		}
		sh.m = make(map[string]int64, len(sh.m))
		sh.mu.Unlock()
	}
	return s.kv.IncrByMany(deltas)
}

func (s *KvStore) bump(code string) {
	if !trackHits {
		return
	}
	sh := &s.dirty[shardOf(code)]
	sh.mu.Lock()
	sh.m[code]++
	sh.mu.Unlock()
}

func (s *KvStore) randCode() string {
	var buf [codeLen]byte
	buf[0] = s.prefix
	var r [codeLen - 1]byte
	_, _ = rand.Read(r[:])
	for i := 1; i < codeLen; i++ {
		buf[i] = base62.ALPHABET[int(r[i-1])%maxInstances]
	}
	return string(buf[:])
}

// Resolve returns the redirect target. Cache hit serves from RAM; a miss
// costs one KV GET and fills the cache.
func (s *KvStore) Resolve(code string) (string, bool) {
	if u, _, ok := s.cache.get(code); ok {
		s.bump(code)
		return u, true
	}
	v, err := s.kvGet(code)
	if err != nil || v == nil {
		return "", false
	}
	e, _, u, ok := decVal(v)
	if !ok || (e != 0 && e <= nowMs()) {
		return "", false
	}
	s.cache.put(code, u, e)
	s.bump(code)
	return u, true
}

// Shorten creates a link; "" if the alias is taken.
func (s *KvStore) Shorten(url, alias string, hasAlias bool, ttlMs int64) string {
	now := nowMs()
	var exp int64
	if ttlMs > 0 {
		exp = now + ttlMs
	}
	putNX := func(code string) (bool, error) {
		if s.layoutHash {
			return s.kv.Hsetnx([]byte(s.bkey(code)), []byte(code), []byte(encVal(exp, now, url)))
		}
		return s.kv.Set([]byte(lkey(code)), []byte(encVal(exp, now, url)), ttlMs, true)
	}
	if hasAlias {
		ok, err := putNX(alias)
		if err != nil || !ok {
			return ""
		}
		return alias
	}
	for {
		code := s.randCode()
		ok, err := putNX(code)
		if err == nil && ok {
			return code
		}
		if err != nil {
			return ""
		}
	}
}

// ShortenMany pipelines all SET NX in one round-trip; collisions (rare)
// retry serially.
func (s *KvStore) ShortenMany(urls []string, ttlMs int64) []string {
	now := nowMs()
	var exp int64
	if ttlMs > 0 {
		exp = now + ttlMs
	}
	codes := make([]string, len(urls))
	cmds := make([][][]byte, 0, len(urls))
	for i, u := range urls {
		c := s.randCode()
		codes[i] = c
		var args [][]byte
		if s.layoutHash {
			args = [][]byte{[]byte("HSETNX"), []byte(s.bkey(c)), []byte(c), []byte(encVal(exp, now, u))}
		} else {
			args = [][]byte{[]byte("SET"), []byte(lkey(c)), []byte(encVal(exp, now, u))}
			if ttlMs > 0 {
				args = append(args, []byte("PX"), []byte(strconv.FormatInt(ttlMs, 10)))
			}
			args = append(args, []byte("NX"))
		}
		cmds = append(cmds, args)
	}
	rs, err := s.kv.pipe(cmds)
	if err != nil {
		// fall back to serial on transport failure
		for i, u := range urls {
			codes[i] = s.Shorten(u, "", false, ttlMs)
		}
		return codes
	}
	for i, r := range rs {
		ok := false
		if s.layoutHash {
			ok = r.Kind == ':' && r.Int == 1
		} else {
			ok = r.Kind == '+' && string(r.Str) == "OK"
		}
		if !ok {
			codes[i] = s.Shorten(urls[i], "", false, ttlMs)
		}
	}
	return codes
}

// Update overwrites url (and optionally ttl) — keeps created_at.
func (s *KvStore) Update(code, url string, ttlMs int64, hasTTL bool) MutResult {
	v, err := s.kvGet(code)
	if err != nil || v == nil {
		return MutMissing
	}
	e, c, _, ok := decVal(v)
	if !ok {
		return MutMissing
	}
	exp := e
	if hasTTL {
		if ttlMs > 0 {
			exp = nowMs() + ttlMs
		} else {
			exp = 0
		}
	}
	if s.layoutHash {
		if err := s.kv.Hset([]byte(s.bkey(code)), []byte(code), []byte(encVal(exp, c, url))); err != nil {
			return MutMissing
		}
	} else {
		var px int64
		if exp > 0 {
			px = exp - nowMs()
		}
		okk, err := s.kv.Set([]byte(lkey(code)), []byte(encVal(exp, c, url)), px, false)
		if err != nil || !okk {
			return MutMissing
		}
	}
	s.cache.remove(code)
	return MutOK
}

// Remove deletes the link and its hit counter.
func (s *KvStore) Remove(code string) MutResult {
	if s.layoutHash {
		b := s.bkey(code)
		n, err := s.kv.Hdel([]byte(b), []byte(code))
		if err != nil || n == 0 {
			return MutMissing
		}
		_, _ = s.kv.Hdel([]byte(b), []byte(hfield(code)))
	} else {
		n, err := s.kv.Del([]byte(lkey(code)))
		if err != nil || n == 0 {
			return MutMissing
		}
		_, _ = s.kv.Del([]byte(hkey(code)))
	}
	s.cache.remove(code)
	return MutOK
}

// List scans l:* (admin path, O(corpus)) — one pipeline of GETs.
func (s *KvStore) List(limit, offset int, sortBy, q string) ([]Link, int) {
	var keys [][]byte
	_ = s.kv.ScanEach("l:*", func(k []byte) { keys = append(keys, append([]byte(nil), k...)) })
	if s.layoutHash {
		return s.listHash(limit, offset, sortBy, q, keys)
	}
	cmds := make([][][]byte, 0, len(keys)*2)
	for _, k := range keys {
		code := string(k[2:])
		cmds = append(cmds,
			[][]byte{[]byte("GET"), k},
			[][]byte{[]byte("GET"), []byte(hkey(code))})
	}
	rs, _ := s.kv.pipe(cmds)
	items := make([]Link, 0, len(keys))
	for i, k := range keys {
		code := string(k[2:])
		if 2*i >= len(rs) || rs[2*i].IsNull() {
			continue
		}
		e, c, u, ok := decVal(rs[2*i].Str)
		if !ok {
			continue
		}
		if q != "" && !strings.Contains(code, q) && !strings.Contains(u, q) {
			continue
		}
		var hits int64
		if 2*i+1 < len(rs) && !rs[2*i+1].IsNull() {
			hits, _ = strconv.ParseInt(string(rs[2*i+1].Str), 10, 64)
		}
		l := Link{Code: code, URL: u, Hits: hits, CreatedAt: c}
		if e != 0 {
			l.ExpiresAt = &e
		}
		items = append(items, l)
	}
	if sortBy == "hits" {
		for i := 1; i < len(items); i++ {
			for j := i; j > 0 && items[j-1].Hits < items[j].Hits; j-- {
				items[j-1], items[j] = items[j], items[j-1]
			}
		}
	}
	total := len(items)
	if offset > total {
		offset = total
	}
	end := offset + limit
	if end > total {
		end = total
	}
	return items[offset:end], total
}

// listHash reads every bucket's fields; "h:{code}" fields are hit counters.
func (s *KvStore) listHash(limit, offset int, sortBy, q string, buckets [][]byte) ([]Link, int) {
	hits := make(map[string]int64, 256)
	items := make([]Link, 0, 256)
	now := nowMs()
	for _, b := range buckets {
		_ = s.kv.HScanEach(string(b), func(f, v []byte) {
			fs := string(f)
			if rest, ok := strings.CutPrefix(fs, "h:"); ok {
				n, _ := strconv.ParseInt(string(v), 10, 64)
				hits[rest] = n
				return
			}
			e, c, u, ok := decVal(v)
			if !ok || (e != 0 && e <= now) {
				return
			}
			if q != "" && !strings.Contains(fs, q) && !strings.Contains(u, q) {
				return
			}
			l := Link{Code: fs, URL: u, CreatedAt: c}
			if e != 0 {
				l.ExpiresAt = &e
			}
			items = append(items, l)
		})
	}
	for i := range items {
		items[i].Hits = hits[items[i].Code]
	}
	if sortBy == "hits" {
		for i := 1; i < len(items); i++ {
			for j := i; j > 0 && items[j-1].Hits < items[j].Hits; j-- {
				items[j-1], items[j] = items[j], items[j-1]
			}
		}
	}
	total := len(items)
	if offset > total {
		offset = total
	}
	end := offset + limit
	if end > total {
		end = total
	}
	return items[offset:end], total
}

// Stats returns the link view incl. unflushed local hit deltas.
func (s *KvStore) Stats(code string) *Link {
	v, err := s.kvGet(code)
	if err != nil || v == nil {
		return nil
	}
	e, c, u, ok := decVal(v)
	if !ok {
		return nil
	}
	l := &Link{Code: code, URL: u, CreatedAt: c}
	if e != 0 {
		l.ExpiresAt = &e
	}
	var hv []byte
	if s.layoutHash {
		hv, _ = s.kv.Hget([]byte(s.bkey(code)), []byte(hfield(code)))
	} else {
		hv, _ = s.kv.Get([]byte(hkey(code)))
	}
	if hv != nil {
		l.Hits, _ = strconv.ParseInt(string(hv), 10, 64)
	}
	sh := &s.dirty[shardOf(code)]
	sh.mu.Lock()
	l.Hits += sh.m[code]
	sh.mu.Unlock()
	return l
}

// Seed bulk-creates links (random codes).
func (s *KvStore) Seed(urls []string) int {
	s.ShortenMany(urls, 0)
	_ = s.flushHits()
	return len(urls)
}

// IsEmpty reports whether the corpus has no links.
func (s *KvStore) IsEmpty() bool {
	any := false
	_ = s.kv.ScanEach("l:*", func([]byte) { any = true })
	return !any
}

// Instance returns the claimed instance id.
func (s *KvStore) Instance() int { return s.instance }

// Persistent — the KV is the persistence layer.
func (s *KvStore) Persistent() bool { return true }

// Flush forces pending hit deltas to the KV.
func (s *KvStore) Flush() { _ = s.flushHits() }

// PollTails is a no-op — the KV is the shared state.
func (s *KvStore) PollTails() {}

// Compact is a no-op — the KV manages its own persistence.
func (s *KvStore) Compact() {}

// Close flushes hits and stops the flush loop.
func (s *KvStore) Close() {
	select {
	case <-s.stop:
	default:
		close(s.stop)
	}
	s.wg.Wait()
	_ = s.flushHits()
}
