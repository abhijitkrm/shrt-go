// Package store implements the in-memory KV + append-only-log persistence.
//
//   - reads: pure map lookup (no disk, no SQL)
//   - writes: map set + buffered append; flush batch every FLUSH_MS (<=5ms
//     loss window), fsync every FSYNC_MS
//   - multi-instance: per-instance log shards (data-<i>.log). Generated codes
//     are 8 chars: ALPHABET[instance] + 7 random base62 chars — the prefix
//     keeps codes unique across instances with zero coordination and lets a
//     read-miss tail exactly the owning shard's log.
package store

import (
	"crypto/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"shrt-go/internal/aof"
	"shrt-go/internal/base62"
)

const (
	flushMS         = 5
	fsyncMS         = 500
	tailMinInterval = 200 // ms, rate limit for lazy on-miss tail polls
	flushBytes      = 256 << 10
	codeLen         = 8  // 1 instance-prefix char + 7 random base62 chars
	maxInstances    = 62 // prefix char space
	maxStrayHits    = 10_000
	numShards       = 256
)

var (
	tailMS    = envInt("TAIL_MS", 0) // 0 = lazy on-miss only
	trackHits = os.Getenv("HITS") != "0"
)

// Link is the public view of a stored entry.
type Link struct {
	Code      string `json:"code"`
	URL       string `json:"url"`
	Hits      int64  `json:"hits"`
	CreatedAt int64  `json:"created_at"`
	ExpiresAt *int64 `json:"expires_at"`
}

type entry struct {
	u  string
	a  int64 // created_at ms
	e  int64 // expires_at ms, 0 = never
	i  int32 // owning instance (-1 unknown)
	h  atomic.Int64
	oh atomic.Int64 // own hits only (what snapshots persist)
}

type strayHit struct {
	t int64 // total deltas seen for a code whose row hasn't arrived
	o int64 // of those, deltas this instance logged
}

type shard struct {
	mu      sync.RWMutex
	data    map[string]*entry
	dirtyMu sync.Mutex
	dirty   map[string]int64 // pending hit deltas to flush to our own log
}

// MutResult is the outcome of Update/Remove.
type MutResult int

const (
	MutOK MutResult = iota
	MutMissing
	MutRemote
)

// Store is the sharded in-memory index + AOF writer.
type Store struct {
	shards    [numShards]shard
	strayHits map[string]*strayHit

	aof      *aof.Aof
	instance int
	prefix   byte
	release  func()
	dir      string
	ownName  string

	tailMu   sync.Mutex // serializes all apply() paths
	tails    map[string]*aof.TailReader
	lastPoll atomic.Int64 // ms

	mutGate sync.RWMutex // RLock: mutations; Lock: compact (excludes them)

	flushTimer *time.Ticker
	syncTimer  *time.Ticker
	tailTimer  *time.Ticker
	done       chan struct{}
	closed     atomic.Bool
}

// New opens a Store. dir ":memory:" disables persistence. instance < 0
// auto-claims the lowest free instance id via lock files.
func New(dir string, instance int) (*Store, error) {
	s := &Store{
		strayHits: make(map[string]*strayHit),
		prefix:    base62.ALPHABET[0],
		release:   func() {},
		tails:     make(map[string]*aof.TailReader),
		done:      make(chan struct{}),
	}
	for i := range s.shards {
		s.shards[i].data = make(map[string]*entry)
		s.shards[i].dirty = make(map[string]int64)
	}

	if dir != ":memory:" {
		var id int
		if instance >= 0 {
			id = instance
		} else {
			claimed, rel, err := aof.ClaimInstance(dir)
			if err != nil {
				return nil, err
			}
			id, s.release = claimed, rel
		}
		if id >= maxInstances {
			s.release()
			return nil, &Error{"instance " + strconv.Itoa(id) + " >= max " + strconv.Itoa(maxInstances)}
		}
		s.instance = id
		s.prefix = base62.ALPHABET[id]
		s.dir = dir
		s.ownName = "data-" + strconv.Itoa(id) + ".log"
		a, err := aof.New(dir, s.ownName)
		if err != nil {
			s.release()
			return nil, err
		}
		s.aof = a
		s.loadAll()
	}

	s.flushTimer = time.NewTicker(flushMS * time.Millisecond)
	go func() {
		for {
			select {
			case <-s.flushTimer.C:
				s.Flush()
			case <-s.done:
				return
			}
		}
	}()
	if s.aof != nil {
		s.syncTimer = time.NewTicker(fsyncMS * time.Millisecond)
		go func() {
			for {
				select {
				case <-s.syncTimer.C:
					s.aof.Sync()
				case <-s.done:
					return
				}
			}
		}()
		if tailMS > 0 {
			s.tailTimer = time.NewTicker(time.Duration(tailMS) * time.Millisecond)
			go func() {
				for {
					select {
					case <-s.tailTimer.C:
						s.PollTails()
					case <-s.done:
						return
					}
				}
			}()
		}
	}
	return s, nil
}

type Error struct{ s string }

func (e *Error) Error() string { return e.s }

func envInt(k string, def int64) int64 {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n
		}
	}
	return def
}

func nowMs() int64 { return time.Now().UnixMilli() }

var alphaIdx = func() [256]int {
	var t [256]int
	for i := range t {
		t[i] = -1
	}
	for i := 0; i < len(base62.ALPHABET); i++ {
		t[base62.ALPHABET[i]] = i
	}
	return t
}()

// shardOf picks the shard for a code (FNV-1a, cheap and well-spread).
func shardOf(code string) uint32 {
	var h uint32 = 2166136261
	for i := 0; i < len(code); i++ {
		h ^= uint32(code[i])
		h *= 16777619
	}
	return h & (numShards - 1)
}

// ---------- log line serialization ----------

// esc appends u JSON-string-escaped (like JSON.stringify without quotes).
func esc(dst []byte, u string) []byte {
	const hex = "0123456789abcdef"
	for i := 0; i < len(u); i++ {
		c := u[i]
		switch c {
		case '"', '\\':
			dst = append(dst, '\\', c)
		case '\n':
			dst = append(dst, '\\', 'n')
		case '\r':
			dst = append(dst, '\\', 'r')
		case '\t':
			dst = append(dst, '\\', 't')
		case '\b':
			dst = append(dst, '\\', 'b')
		case '\f':
			dst = append(dst, '\\', 'f')
		default:
			if c < 0x20 {
				dst = append(dst, '\\', 'u', '0', '0', hex[c>>4], hex[c&0xf])
			} else {
				dst = append(dst, c)
			}
		}
	}
	return dst
}

func rowLine(c, u string, a, e, i, n int64) []byte {
	b := make([]byte, 0, len(u)+len(c)+48)
	b = append(b, `{"c":"`...)
	b = append(b, c...)
	b = append(b, `","u":"`...)
	b = esc(b, u)
	b = append(b, `","a":`...)
	b = strconv.AppendInt(b, a, 10)
	b = append(b, `,"e":`...)
	if e == 0 {
		b = append(b, "null"...)
	} else {
		b = strconv.AppendInt(b, e, 10)
	}
	b = append(b, `,"i":`...)
	b = strconv.AppendInt(b, i, 10)
	b = append(b, `,"n":`...)
	b = strconv.AppendInt(b, n, 10)
	return append(b, '}')
}

func hitLine(c string, d, i int64) []byte {
	b := make([]byte, 0, len(c)+24)
	b = append(b, `{"h":"`...)
	b = append(b, c...)
	b = append(b, `","d":`...)
	b = strconv.AppendInt(b, d, 10)
	b = append(b, `,"i":`...)
	b = strconv.AppendInt(b, i, 10)
	return append(b, '}')
}

func delLine(c string) []byte {
	b := make([]byte, 0, len(c)+8)
	b = append(b, `{"x":"`...)
	b = append(b, c...)
	return append(b, '"', '}')
}

// ---------- log line parsing ----------

// op is one parsed log line: row {"c","u","a","e","i","n"}, hit delta
// {"h","d","i"}, or tombstone {"x"}.
type op struct {
	c, u, h, x string
	a, e, i, n int64
	d          int64
	hasE       bool
}

// parseOp decodes one log line. Machine-generated input, but it tolerates
// field reordering and escapes in string values.
func parseOp(b []byte, o *op) bool {
	*o = op{}
	pos := 0
	for pos < len(b) {
		// next key
		for pos < len(b) && b[pos] != '"' {
			pos++
		}
		if pos >= len(b) {
			return o.c != "" || o.h != "" || o.x != ""
		}
		pos++
		ks := pos
		for pos < len(b) && b[pos] != '"' {
			pos++
		}
		if pos >= len(b) {
			return false
		}
		key := b[ks:pos]
		pos++
		for pos < len(b) && b[pos] != ':' {
			pos++
		}
		if pos >= len(b) {
			return false
		}
		pos++
		for pos < len(b) && (b[pos] == ' ' || b[pos] == '\t') {
			pos++
		}
		if pos >= len(b) {
			return false
		}
		if b[pos] == '"' {
			pos++
			vs := pos
			escapes := false
			for pos < len(b) {
				if b[pos] == '\\' {
					escapes = true
					pos += 2
					continue
				}
				if b[pos] == '"' {
					break
				}
				pos++
			}
			val := b[vs:pos]
			if escapes {
				val = unescape(val)
			}
			pos++
			switch key[0] {
			case 'c':
				o.c = string(val)
			case 'u':
				o.u = string(val)
			case 'h':
				o.h = string(val)
			case 'x':
				o.x = string(val)
			}
		} else {
			vs := pos
			for pos < len(b) && b[pos] != ',' && b[pos] != '}' {
				pos++
			}
			num := strings.TrimSpace(string(b[vs:pos]))
			switch key[0] {
			case 'a':
				o.a, _ = strconv.ParseInt(num, 10, 64)
			case 'e':
				if num != "null" {
					o.e, _ = strconv.ParseInt(num, 10, 64)
					o.hasE = true
				}
			case 'i':
				v, _ := strconv.ParseInt(num, 10, 64)
				o.i = v
			case 'n':
				o.n, _ = strconv.ParseInt(num, 10, 64)
			case 'd':
				o.d, _ = strconv.ParseInt(num, 10, 64)
			}
		}
	}
	return o.c != "" || o.h != "" || o.x != ""
}

func unescape(b []byte) []byte {
	out := make([]byte, 0, len(b))
	for i := 0; i < len(b); i++ {
		if b[i] != '\\' || i+1 >= len(b) {
			out = append(out, b[i])
			continue
		}
		i++
		switch b[i] {
		case 'n':
			out = append(out, '\n')
		case 'r':
			out = append(out, '\r')
		case 't':
			out = append(out, '\t')
		case 'b':
			out = append(out, '\b')
		case 'f':
			out = append(out, '\f')
		case 'u':
			if i+4 < len(b) {
				v, err := strconv.ParseUint(string(b[i+1:i+5]), 16, 32)
				if err == nil {
					out = append(out, []byte(string(rune(v)))...)
					i += 4
					continue
				}
			}
			out = append(out, 'u')
		default:
			out = append(out, b[i]) // covers \" \\ \/
		}
	}
	return out
}

// ---------- apply / replay / tailing ----------

// apply folds one parsed log line into the index. Caller must hold tailMu.
func (s *Store) apply(o *op) {
	if o.x != "" {
		sh := &s.shards[shardOf(o.x)]
		sh.mu.Lock()
		delete(sh.data, o.x)
		sh.mu.Unlock()
		delete(s.strayHits, o.x)
		return
	}
	if o.h != "" {
		own := int64(s.instance) == o.i
		sh := &s.shards[shardOf(o.h)]
		sh.mu.RLock()
		e := sh.data[o.h]
		sh.mu.RUnlock()
		if e != nil {
			e.h.Add(o.d)
			if own {
				e.oh.Add(o.d)
			}
			return
		}
		if len(s.strayHits) >= maxStrayHits {
			for k := range s.strayHits {
				delete(s.strayHits, k)
				break
			}
		}
		st := s.strayHits[o.h]
		if st == nil {
			st = &strayHit{}
			s.strayHits[o.h] = st
		}
		st.t += o.d
		if own {
			st.o += o.d
		}
		return
	}
	if o.c == "" {
		return
	}
	st := s.strayHits[o.c]
	var stT, stO int64
	if st != nil {
		stT, stO = st.t, st.o
		delete(s.strayHits, o.c)
	}
	e := &entry{u: o.u, a: o.a, i: int32(o.i)}
	if o.hasE {
		e.e = o.e
	}
	e.h.Store(o.n + stT)
	e.oh.Store(o.n + stO)
	sh := &s.shards[shardOf(o.c)]
	sh.mu.Lock()
	sh.data[o.c] = e
	sh.mu.Unlock()
}

func (s *Store) applyLine(line []byte) {
	var o op
	if parseOp(line, &o) {
		s.apply(&o)
	}
}

// loadAll replays own snapshot + own log + all sibling logs present at boot.
func (s *Store) loadAll() {
	s.tailMu.Lock()
	defer s.tailMu.Unlock()
	aof.ReplayFile(filepath.Join(s.dir, snapName(s.instance)), s.applyLine)
	aof.ReplayFile(filepath.Join(s.dir, s.ownName), s.applyLine)
	for _, f := range aof.ShardFiles(s.dir, s.ownName) {
		snap := strings.TrimSuffix(f, ".log") + ".snap"
		aof.ReplayFile(filepath.Join(s.dir, snap), s.applyLine)
		s.tails[f] = aof.TailFromStart(filepath.Join(s.dir, f))
	}
}

func snapName(i int) string { return "data-" + strconv.Itoa(i) + ".snap" }

// PollTails pulls newly appended lines from sibling logs and discovers new
// shards. Safe to call concurrently; serialized internally.
func (s *Store) PollTails() {
	if s.aof == nil {
		return
	}
	s.tailMu.Lock()
	defer s.tailMu.Unlock()
	for _, f := range aof.ShardFiles(s.dir, s.ownName) {
		if _, ok := s.tails[f]; !ok {
			s.tails[f] = aof.TailFromStart(filepath.Join(s.dir, f))
		}
	}
	for _, t := range s.tails {
		t.ReadNew(s.applyLine)
	}
	s.lastPoll.Store(nowMs())
}

// lazyPoll is a rate-limited PollTails used on read-miss.
func (s *Store) lazyPoll() {
	last := s.lastPoll.Load()
	now := nowMs()
	if now-last < tailMinInterval {
		return
	}
	if !s.lastPoll.CompareAndSwap(last, now) {
		return
	}
	s.PollTails()
}

// tailOwner tails the shard that owns code's prefix (generated codes only).
func (s *Store) tailOwner(code string) {
	if len(code) == 0 {
		return
	}
	owner := alphaIdx[code[0]]
	if owner < 0 || owner == s.instance || s.aof == nil {
		return
	}
	name := "data-" + strconv.Itoa(owner) + ".log"
	s.tailMu.Lock()
	t := s.tails[name]
	if t == nil {
		t = aof.TailFromStart(filepath.Join(s.dir, name))
		s.tails[name] = t
	}
	t.ReadNew(s.applyLine)
	s.tailMu.Unlock()
}

// ---------- public ops ----------

func (s *Store) randSuffix() string {
	var buf [codeLen - 1]byte
	_, _ = rand.Read(buf[:])
	var sb [codeLen - 1]byte
	for k := 0; k < codeLen-1; k++ {
		sb[k] = base62.ALPHABET[int(buf[k])%maxInstances]
	}
	return string(sb[:])
}

// Shorten creates a link; returns the code, or "" if the alias is taken.
func (s *Store) Shorten(url, alias string, hasAlias bool, ttlMs int64) string {
	s.mutGate.RLock()
	defer s.mutGate.RUnlock()
	now := nowMs()
	var exp int64
	if ttlMs > 0 {
		exp = now + ttlMs
	}
	code := alias
	var sh *shard
	if hasAlias {
		sh = &s.shards[shardOf(code)]
		sh.mu.Lock()
		if _, taken := sh.data[code]; taken {
			sh.mu.Unlock()
			return ""
		}
	} else {
		for {
			code = string(s.prefix) + s.randSuffix()
			sh = &s.shards[shardOf(code)]
			sh.mu.Lock()
			if _, taken := sh.data[code]; !taken {
				break
			}
			sh.mu.Unlock()
		}
	}
	e := &entry{u: url, a: now, e: exp, i: int32(s.instance)}
	sh.data[code] = e
	sh.mu.Unlock()
	if s.aof != nil {
		s.aof.Push(rowLine(code, url, now, exp, int64(s.instance), 0))
		if s.aof.PendingBytes() > flushBytes {
			s.aof.Flush()
		}
	}
	return code
}

// ShortenMany bulk-creates links; returns codes aligned with input order.
func (s *Store) ShortenMany(urls []string, ttlMs int64) []string {
	s.mutGate.RLock()
	defer s.mutGate.RUnlock()
	now := nowMs()
	var exp int64
	if ttlMs > 0 {
		exp = now + ttlMs
	}
	codes := make([]string, len(urls))
	rnd := make([]byte, len(urls)*(codeLen-1)) // one CSPRNG call
	_, _ = rand.Read(rnd)
	var suffix [codeLen]byte
	suffix[0] = s.prefix
	for i, u := range urls {
		for k := 0; k < codeLen-1; k++ {
			suffix[k+1] = base62.ALPHABET[int(rnd[i*(codeLen-1)+k])%maxInstances]
		}
		code := string(suffix[:])
		sh := &s.shards[shardOf(code)]
		sh.mu.Lock()
		for {
			if _, taken := sh.data[code]; !taken {
				break
			}
			sh.mu.Unlock()
			code = string(s.prefix) + s.randSuffix()
			sh = &s.shards[shardOf(code)]
			sh.mu.Lock()
		}
		sh.data[code] = &entry{u: u, a: now, e: exp, i: int32(s.instance)}
		sh.mu.Unlock()
		if s.aof != nil {
			s.aof.Push(rowLine(code, u, now, exp, int64(s.instance), 0))
		}
		codes[i] = code
	}
	if s.aof != nil && s.aof.PendingBytes() > flushBytes {
		s.aof.Flush()
	}
	return codes
}

// Resolve returns the target url, or ("", false) for miss/expired. Counts a
// hit on success.
func (s *Store) Resolve(code string) (string, bool) {
	sh := &s.shards[shardOf(code)]
	sh.mu.RLock()
	e := sh.data[code]
	sh.mu.RUnlock()
	if e == nil && s.aof != nil {
		// maybe a sibling wrote it and we haven't tailed yet: prefix targets
		// the owning shard; lazyPoll catches aliases and anything else
		s.tailOwner(code)
		sh.mu.RLock()
		e = sh.data[code]
		sh.mu.RUnlock()
		if e == nil {
			s.lazyPoll()
			sh.mu.RLock()
			e = sh.data[code]
			sh.mu.RUnlock()
		}
	}
	if e == nil || (e.e != 0 && e.e <= nowMs()) {
		return "", false
	}
	if trackHits {
		e.h.Add(1)
		e.oh.Add(1)
		sh.dirtyMu.Lock()
		sh.dirty[code]++
		sh.dirtyMu.Unlock()
	}
	return e.u, true
}

// IsEmpty reports whether the index is empty.
func (s *Store) IsEmpty() bool {
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.RLock()
		n := len(sh.data)
		sh.mu.RUnlock()
		if n > 0 {
			return false
		}
	}
	return true
}

// Update changes url/ttl in place. Durable only on the owning instance.
// hasTTL=false keeps the existing expiry; otherwise ttlMs>0 sets now+ttlMs
// and ttlMs<=0 clears expiry.
func (s *Store) Update(code, url string, ttlMs int64, hasTTL bool) MutResult {
	s.mutGate.RLock()
	defer s.mutGate.RUnlock()
	sh := &s.shards[shardOf(code)]
	sh.mu.Lock()
	e := sh.data[code]
	if e == nil {
		sh.mu.Unlock()
		return MutMissing
	}
	if int(e.i) != s.instance {
		sh.mu.Unlock()
		return MutRemote
	}
	exp := e.e
	if hasTTL {
		if ttlMs > 0 {
			exp = nowMs() + ttlMs
		} else {
			exp = 0
		}
	}
	e.u = url
	e.e = exp
	sh.mu.Unlock()
	if s.aof != nil {
		s.aof.Push(rowLine(code, url, e.a, exp, int64(s.instance), 0))
	}
	return MutOK
}

// Remove deletes a link. Same owner rule as Update.
func (s *Store) Remove(code string) MutResult {
	s.mutGate.RLock()
	defer s.mutGate.RUnlock()
	sh := &s.shards[shardOf(code)]
	sh.mu.Lock()
	e := sh.data[code]
	if e == nil {
		sh.mu.Unlock()
		return MutMissing
	}
	if int(e.i) != s.instance {
		sh.mu.Unlock()
		return MutRemote
	}
	delete(sh.data, code)
	sh.mu.Unlock()
	if s.aof != nil {
		s.aof.Push(delLine(code))
	}
	return MutOK
}

// List is an O(n) scan for UI listing — admin path, not the hot path.
func (s *Store) List(limit, offset int, sortBy string, q string) ([]Link, int) {
	items := make([]Link, 0, 256)
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.RLock()
		for code, e := range sh.data {
			if q != "" && !strings.Contains(code, q) && !strings.Contains(e.u, q) {
				continue
			}
			var exp *int64
			if e.e != 0 {
				ev := e.e
				exp = &ev
			}
			items = append(items, Link{
				Code:      code,
				URL:       e.u,
				Hits:      e.h.Load(),
				CreatedAt: e.a,
				ExpiresAt: exp,
			})
		}
		sh.mu.RUnlock()
	}
	if sortBy == "hits" {
		sort.Slice(items, func(x, y int) bool { return items[x].Hits > items[y].Hits })
	} else {
		sort.Slice(items, func(x, y int) bool { return items[x].CreatedAt > items[y].CreatedAt })
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

// Stats returns the public view of one link.
func (s *Store) Stats(code string) *Link {
	sh := &s.shards[shardOf(code)]
	sh.mu.RLock()
	e := sh.data[code]
	sh.mu.RUnlock()
	if e == nil {
		return nil
	}
	var exp *int64
	if e.e != 0 {
		ev := e.e
		exp = &ev
	}
	return &Link{
		Code:      code,
		URL:       e.u,
		Hits:      e.h.Load(),
		CreatedAt: e.a,
		ExpiresAt: exp,
	}
}

// Seed bulk-inserts urls through the normal write path. Returns count.
func (s *Store) Seed(urls []string) int {
	s.ShortenMany(urls, 0)
	s.Flush()
	return len(urls)
}

// Flush persists hit deltas + all queued rows: one write + fsync boundary.
func (s *Store) Flush() {
	s.mutGate.RLock()
	defer s.mutGate.RUnlock()
	if s.aof != nil {
		for i := range s.shards {
			sh := &s.shards[i]
			sh.dirtyMu.Lock()
			if len(sh.dirty) > 0 {
				for code, d := range sh.dirty {
					s.aof.Push(hitLine(code, d, int64(s.instance)))
				}
				clear(sh.dirty)
			}
			sh.dirtyMu.Unlock()
		}
		s.aof.Flush()
	}
}

// Compact rewrites own rows as a compact snapshot, then truncates own log.
func (s *Store) Compact() {
	if s.aof == nil {
		return
	}
	s.mutGate.Lock()
	defer s.mutGate.Unlock()
	s.flushLocked()
	snapPath := filepath.Join(s.dir, snapName(s.instance))
	tmp := snapPath + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return
	}
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.RLock()
		for code, e := range sh.data {
			if int(e.i) == s.instance {
				_, _ = f.Write(rowLine(code, e.u, e.a, e.e, e.i64(), e.oh.Load()))
				_, _ = f.WriteString("\n")
			}
		}
		sh.mu.RUnlock()
	}
	_ = f.Close()
	_ = os.Rename(tmp, snapPath)
	s.aof.Truncate()
}

func (e *entry) i64() int64 { return int64(e.i) }

// flushLocked is Flush for callers already holding mutGate.
func (s *Store) flushLocked() {
	if s.aof == nil {
		return
	}
	for i := range s.shards {
		sh := &s.shards[i]
		sh.dirtyMu.Lock()
		for code, d := range sh.dirty {
			s.aof.Push(hitLine(code, d, int64(s.instance)))
		}
		clear(sh.dirty)
		sh.dirtyMu.Unlock()
	}
	s.aof.Flush()
}

// Close stops timers, flushes, fsyncs, and releases the instance lock.
func (s *Store) Close() {
	if s.closed.Swap(true) {
		return
	}
	close(s.done)
	if s.flushTimer != nil {
		s.flushTimer.Stop()
	}
	if s.syncTimer != nil {
		s.syncTimer.Stop()
	}
	if s.tailTimer != nil {
		s.tailTimer.Stop()
	}
	s.tailMu.Lock()
	for _, t := range s.tails {
		t.Close()
	}
	s.tails = map[string]*aof.TailReader{}
	s.tailMu.Unlock()
	s.Flush()
	if s.aof != nil {
		s.aof.Sync()
		s.aof.Close()
	}
	s.release()
}

// API is the store surface the HTTP layer uses. *Store (AOF engine) and
// *KvStore (external RESP backend) both implement it.
type API interface {
	Shorten(url, alias string, hasAlias bool, ttlMs int64) string
	ShortenMany(urls []string, ttlMs int64) []string
	Resolve(code string) (string, bool)
	Update(code, url string, ttlMs int64, hasTTL bool) MutResult
	Remove(code string) MutResult
	List(limit, offset int, sortBy, q string) ([]Link, int)
	Stats(code string) *Link
	Seed(urls []string) int
	IsEmpty() bool
	Flush()
	PollTails()
	Compact()
	Close()
}

// NewFromEnv picks the backend: STORE=aof|local (default) uses the
// in-process AOF engine; STORE=dragonfly|redis|kv uses an external RESP
// store (DRAGONFLY_ADDR/KV_ADDR, CACHE entries, CACHE_TTL_MS);
// STORE=pebble|rocksdb uses embedded Pebble (PEBBLE_PATH or {dir}/pebble,
// single-writer file lock).
func NewFromEnv(dir string, instance int) (API, error) {
	switch os.Getenv("STORE") {
	case "pebble", "rocksdb":
		path := os.Getenv("PEBBLE_PATH")
		if path == "" {
			path = dir + "/pebble"
		}
		cache := int(envInt("CACHE", 100000))
		ttl := int64(envInt("CACHE_TTL_MS", 5000))
		return NewPebble(path, instance, cache, ttl)
	case "dragonfly", "redis", "kv":
		addr := os.Getenv("DRAGONFLY_ADDR")
		if addr == "" {
			addr = os.Getenv("KV_ADDR")
		}
		if addr == "" {
			addr = "127.0.0.1:6379"
		}
		cache := int(envInt("CACHE", 100000))
		ttl := int64(envInt("CACHE_TTL_MS", 5000))
		return NewKV(addr, instance, cache, ttl)
	default:
		return New(dir, instance)
	}
}
