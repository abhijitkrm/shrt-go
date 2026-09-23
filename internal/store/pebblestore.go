// pebblestore.go — embedded Pebble (pure-Go LSM) backend (STORE=pebble).
//
// Keys:  l:{code} -> "v1|{expires_ms}|{created_ms}|{url}"
//        h:{code} -> u64 hit counter (Merge operands — no read-modify-write)
//
// Expiry is embedded in the value and enforced on read; a periodic sweep
// (PEBBLE_SWEEP_MS, default 1h) iterates l:* and deletes expired keys
// (Pebble has no value-based compaction filters). Reads go through a
// bounded hot FIFO so the DB only sees cache misses.
//
// Embedded means single-writer: Pebble holds an exclusive file lock on
// the DB dir — one process per PEBBLE_PATH. For multi-instance/multi-node
// use the RESP KV backend.

package store

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/pebble/v2"

	"shrt-go/internal/base62"
	"shrt-go/internal/metrics"
)

// u64 merge operator for the hits namespace: operands are LE u64 deltas.
type u64Merger struct{ base uint64 }

func (m *u64Merger) MergeNewer(v []byte) error {
	if len(v) >= 8 {
		m.base += binary.LittleEndian.Uint64(v)
	}
	return nil
}
func (m *u64Merger) MergeOlder(v []byte) error { return m.MergeNewer(v) }
func (m *u64Merger) Finish(bool) ([]byte, io.Closer, error) {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], m.base)
	return b[:], nil, nil
}

var u64Merge = &pebble.Merger{
	Name: "u64add",
	Merge: func(key, value []byte) (pebble.ValueMerger, error) {
		m := &u64Merger{}
		if len(value) >= 8 {
			m.base = binary.LittleEndian.Uint64(value)
		}
		return m, nil
	},
}

// PebbleStore implements API backed by an embedded Pebble DB.
type PebbleStore struct {
	db    *pebble.DB
	cache *kvCache
	dirty [numShards]struct {
		mu sync.Mutex
		m  map[string]int64
	}
	instance int
	prefix   byte
	stop     chan struct{}
	wg       sync.WaitGroup
}

// NewPebble opens a Pebble-backed store at path (created if missing).
func NewPebble(path string, instance, cacheEntries int, cacheTTLMs int64) (*PebbleStore, error) {
	if err := os.MkdirAll(path, 0o755); err != nil {
		return nil, err
	}
	opts := &pebble.Options{Merger: u64Merge}
	db, err := pebble.Open(path, opts)
	if err != nil {
		return nil, err
	}
	if instance < 0 {
		instance = 0
	}
	if instance >= maxInstances {
		instance = maxInstances - 1
	}
	s := &PebbleStore{
		db:       db,
		cache:    newKvCache(cacheEntries, cacheTTLMs),
		instance: instance,
		prefix:   base62.ALPHABET[instance],
		stop:     make(chan struct{}),
	}
	for i := range s.dirty {
		s.dirty[i].m = make(map[string]int64)
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		t := time.NewTicker(flushMS * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				s.flushHits()
			case <-s.stop:
				return
			}
		}
	}()
	// expired-key sweep — Pebble can't filter on values during compaction
	sweepMs := envInt("PEBBLE_SWEEP_MS", 3_600_000)
	if sweepMs > 0 {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			for {
				select {
				case <-time.After(time.Duration(sweepMs) * time.Millisecond):
					s.sweepExpired()
				case <-s.stop:
					return
				}
			}
		}()
	}
	return s, nil
}

func (s *PebbleStore) lkey(c string) []byte { return []byte("l:" + c) }
func (s *PebbleStore) hkey(c string) []byte { return []byte("h:" + c) }

func (s *PebbleStore) randCode() string {
	var buf [codeLen]byte
	buf[0] = s.prefix
	var r [codeLen - 1]byte
	_, _ = rand.Read(r[:])
	for i := 1; i < codeLen; i++ {
		buf[i] = base62.ALPHABET[int(r[i-1])%maxInstances]
	}
	return string(buf[:])
}

func (s *PebbleStore) bump(code string) {
	if os.Getenv("HITS") == "0" {
		return
	}
	d := &s.dirty[shardOf(code)]
	d.mu.Lock()
	d.m[code]++
	d.mu.Unlock()
}

func (s *PebbleStore) flushHits() {
	b := s.db.NewBatch()
	any := false
	var d [8]byte
	for i := range s.dirty {
		sh := &s.dirty[i]
		sh.mu.Lock()
		for code, n := range sh.m {
			binary.LittleEndian.PutUint64(d[:], uint64(n))
			_ = b.Merge(s.hkey(code), d[:], nil)
			any = true
		}
		for k := range sh.m {
			delete(sh.m, k)
		}
		sh.mu.Unlock()
	}
	if any {
		_ = b.Commit(pebble.NoSync)
	} else {
		_ = b.Close()
	}
}

// sweepExpired iterates l:* and deletes keys whose embedded expiry passed.
func (s *PebbleStore) sweepExpired() {
	it, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte("l:"),
		UpperBound: []byte("l;"),
	})
	if err != nil {
		return
	}
	now := nowMs()
	var dead [][]byte
	for it.First(); it.Valid(); it.Next() {
		e, _, _, ok := decVal(it.Value())
		if ok && e != 0 && e <= now {
			dead = append(dead, append([]byte(nil), it.Key()...))
		}
	}
	_ = it.Close()
	if len(dead) == 0 {
		return
	}
	b := s.db.NewBatch()
	for _, k := range dead {
		_ = b.Delete(k, nil)
	}
	_ = b.Commit(pebble.NoSync)
}

func (s *PebbleStore) getLink(code string) ([]byte, bool) {
	v, closer, err := s.db.Get(s.lkey(code))
	if err != nil {
		return nil, false // ErrNotFound or a real error both miss
	}
	out := append([]byte(nil), v...)
	_ = closer.Close()
	return out, true
}

func (s *PebbleStore) linkHits(code string) int64 {
	v, closer, err := s.db.Get(s.hkey(code))
	if err != nil {
		return 0
	}
	var n int64
	if len(v) >= 8 {
		n = int64(binary.LittleEndian.Uint64(v))
	}
	_ = closer.Close()
	n += s.dirty[shardOf(code)].m[code]
	return n
}

// Resolve returns the redirect target. Cache hit serves from RAM; a miss
// costs one point lookup (bloom-filtered) and fills the cache.
func (s *PebbleStore) Resolve(code string) (string, bool) {
	if u, _, ok := s.cache.get(code); ok {
		metrics.CacheHit()
		s.bump(code)
		return u, true
	}
	metrics.CacheMiss()
	t0 := time.Now()
	v, ok := s.getLink(code)
	metrics.StoreRead(time.Since(t0).Microseconds())
	if !ok {
		return "", false
	}
	e, _, u, ok2 := decVal(v)
	if !ok2 || (e != 0 && e <= nowMs()) {
		return "", false
	}
	s.cache.put(code, u, e)
	s.bump(code)
	return u, true
}

// Shorten persists then hot-fills — the DB write lands before the code is
// returned so a crash can't hand out an unpersisted link.
func (s *PebbleStore) Shorten(url, alias string, hasAlias bool, ttlMs int64) string {
	metrics.StoreWrite()
	now := nowMs()
	exp := int64(0)
	if ttlMs > 0 {
		exp = now + ttlMs
	}
	put := func(code string) bool {
		if _, exists := s.getLink(code); exists {
			return false
		}
		if err := s.db.Set(s.lkey(code), []byte(encVal(exp, now, url)), pebble.NoSync); err != nil {
			return false
		}
		s.cache.put(code, url, exp)
		return true
	}
	if hasAlias {
		if !put(alias) {
			return ""
		}
		return alias
	}
	for {
		c := s.randCode()
		if put(c) {
			return c
		}
	}
}

func (s *PebbleStore) ShortenMany(urls []string, ttlMs int64) []string {
	now := nowMs()
	exp := int64(0)
	if ttlMs > 0 {
		exp = now + ttlMs
	}
	codes := make([]string, len(urls))
	b := s.db.NewBatch()
	var retry []int
	for i, u := range urls {
		c := s.randCode()
		if _, exists := s.getLink(c); exists {
			retry = append(retry, i)
			continue
		}
		codes[i] = c
		_ = b.Set(s.lkey(c), []byte(encVal(exp, now, u)), nil)
	}
	if err := b.Commit(pebble.NoSync); err != nil {
		retry = retry[:0]
		for i := range codes {
			retry = append(retry, i)
		}
	} else {
		for i, c := range codes {
			if c != "" {
				s.cache.put(c, urls[i], exp)
			}
		}
	}
	for _, i := range retry {
		codes[i] = s.Shorten(urls[i], "", false, ttlMs)
	}
	return codes
}

func (s *PebbleStore) Update(code, url string, ttlMs int64, hasTTL bool) MutResult {
	v, ok := s.getLink(code)
	if !ok {
		return MutMissing
	}
	e, c, _, ok2 := decVal(v)
	if !ok2 {
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
	if err := s.db.Set(s.lkey(code), []byte(encVal(exp, c, url)), pebble.NoSync); err != nil {
		return MutMissing
	}
	s.cache.remove(code)
	return MutOK
}

func (s *PebbleStore) Remove(code string) MutResult {
	if _, ok := s.getLink(code); !ok {
		return MutMissing
	}
	b := s.db.NewBatch()
	_ = b.Delete(s.lkey(code), nil)
	_ = b.Delete(s.hkey(code), nil)
	_ = b.Commit(pebble.NoSync)
	s.cache.remove(code)
	return MutOK
}

func (s *PebbleStore) List(limit, offset int, sortBy, q string) ([]Link, int) {
	it, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte("l:"),
		UpperBound: []byte("l;"),
	})
	if err != nil {
		return nil, 0
	}
	now := nowMs()
	var items []Link
	for it.First(); it.Valid(); it.Next() {
		code := string(it.Key()[2:])
		e, c, u, ok := decVal(it.Value())
		if !ok || (e != 0 && e <= now) {
			continue
		}
		if q != "" && !strings.Contains(code, q) && !strings.Contains(u, q) {
			continue
		}
		l := Link{Code: code, URL: u, Hits: s.linkHits(code), CreatedAt: c}
		if e != 0 {
			l.ExpiresAt = &e
		}
		items = append(items, l)
	}
	_ = it.Close()
	if sortBy == "hits" {
		sort.Slice(items, func(i, j int) bool { return items[i].Hits > items[j].Hits })
	} else {
		sort.Slice(items, func(i, j int) bool { return items[i].CreatedAt > items[j].CreatedAt })
	}
	total := len(items)
	if offset >= total {
		return nil, total
	}
	end := offset + limit
	if end > total {
		end = total
	}
	return items[offset:end], total
}

func (s *PebbleStore) Stats(code string) *Link {
	v, ok := s.getLink(code)
	if !ok {
		return nil
	}
	e, c, u, ok2 := decVal(v)
	if !ok2 || (e != 0 && e <= nowMs()) {
		return nil
	}
	l := &Link{Code: code, URL: u, Hits: s.linkHits(code), CreatedAt: c}
	if e != 0 {
		l.ExpiresAt = &e
	}
	return l
}

func (s *PebbleStore) Seed(urls []string) int {
	s.ShortenMany(urls, 0)
	s.flushHits()
	return len(urls)
}

// Healthy probes the DB — a missing-key read proves it is open & readable.
func (s *PebbleStore) Healthy() bool {
	_, closer, err := s.db.Get([]byte(""))
	if err == nil {
		_ = closer.Close()
		return true
	}
	return err == pebble.ErrNotFound
}

func (s *PebbleStore) IsEmpty() bool {
	it, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte("l:"),
		UpperBound: []byte("l;"),
	})
	if err != nil {
		return true
	}
	ok := it.First()
	_ = it.Close()
	return !ok
}

func (s *PebbleStore) Flush()     { s.flushHits() }
func (s *PebbleStore) PollTails() {}

// Compact reclaims space — runs a full-range compaction, which also drops
// expired keys past the sweep via deletion on next sweep cycle.
func (s *PebbleStore) Compact() {
	_ = s.db.Compact(context.Background(), []byte("l:"), []byte("l;"), false)
}

func (s *PebbleStore) Close() {
	select {
	case <-s.stop:
	default:
		close(s.stop)
	}
	s.wg.Wait()
	s.flushHits()
	_ = s.db.Close()
}
