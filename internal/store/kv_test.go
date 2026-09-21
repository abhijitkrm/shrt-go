// Live tests for the KV backend. Gated on SHRT_KV_ADDR — skipped when
// unset/unreachable so the suite stays hermetic.
//   SHRT_KV_ADDR=127.0.0.1:6379 go test ./internal/store -run TestKV -v

package store

import (
	"os"
	"sync"
	"testing"
	"time"
)

// tests share one logical DB — serialize so FlushDB isolates.
var kvDBMu sync.Mutex

func kvStore(t *testing.T) *KvStore {
	t.Helper()
	addr := os.Getenv("SHRT_KV_ADDR")
	if addr == "" {
		t.Skip("SHRT_KV_ADDR unset")
	}
	kvDBMu.Lock()
	t.Cleanup(kvDBMu.Unlock)
	st, err := NewKV(addr, 0, 1000, 50)
	if err != nil {
		t.Skipf("kv unreachable at %s: %v", addr, err)
	}
	t.Cleanup(st.Close)
	kv, err := Dial(addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	if err := kv.FlushDB(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}
	return st
}

func TestKVShortenResolve(t *testing.T) {
	st := kvStore(t)
	if c := st.Shorten("https://a.com", "gh", true, 0); c != "gh" {
		t.Fatalf("alias shorten: %q", c)
	}
	if u, ok := st.Resolve("gh"); !ok || u != "https://a.com" {
		t.Fatalf("resolve: %q %v", u, ok)
	}
	if c := st.Shorten("https://b.com", "gh", true, 0); c != "" {
		t.Fatalf("alias collision accepted: %q", c)
	}
	c := st.Shorten("https://c.com", "", false, 0)
	if u, ok := st.Resolve(c); !ok || u != "https://c.com" {
		t.Fatalf("gen resolve: %q %v", u, ok)
	}
}

func TestKVCacheBoundedColdMiss(t *testing.T) {
	st := kvStore(t)
	codes := make([]string, 200)
	for i := range codes {
		codes[i] = st.Shorten("https://x"+string(rune('a'+i%26))+".com/"+string(rune('0'+i%10))+itoa(i), "", false, 0)
	}
	for i, c := range codes {
		if _, ok := st.Resolve(c); !ok {
			t.Fatalf("cold miss for %s (i=%d)", c, i)
		}
	}
}

func TestKVHitsBatched(t *testing.T) {
	st := kvStore(t)
	st.Shorten("https://a.com", "h", true, 0)
	for i := 0; i < 5; i++ {
		st.Resolve("h")
	}
	st.Flush()
	if s := st.Stats("h"); s == nil || s.Hits != 5 {
		t.Fatalf("hits: %+v", s)
	}
}

func TestKVUpdateRemove(t *testing.T) {
	st := kvStore(t)
	st.Shorten("https://a.com", "u", true, 0)
	if st.Update("u", "https://b.com", 0, false) != MutOK {
		t.Fatal("update")
	}
	if u, _ := st.Resolve("u"); u != "https://b.com" {
		t.Fatalf("post-update resolve: %q", u)
	}
	if st.Update("missing", "https://x.com", 0, false) != MutMissing {
		t.Fatal("update missing")
	}
	if st.Remove("u") != MutOK {
		t.Fatal("remove")
	}
	if _, ok := st.Resolve("u"); ok {
		t.Fatal("resolve after remove")
	}
	if st.Remove("u") != MutMissing {
		t.Fatal("remove missing")
	}
}

func TestKVTTL(t *testing.T) {
	st := kvStore(t)
	st.Shorten("https://t.com", "ttl", true, 80)
	if _, ok := st.Resolve("ttl"); !ok {
		t.Fatal("resolve before expiry")
	}
	time.Sleep(120 * time.Millisecond)
	if _, ok := st.Resolve("ttl"); ok {
		t.Fatal("resolve after expiry")
	}
}

func TestKVListStats(t *testing.T) {
	st := kvStore(t)
	st.Shorten("https://one.com", "one", true, 0)
	st.Shorten("https://two.com", "two", true, 0)
	st.Resolve("one")
	st.Flush()
	links, total := st.List(10, 0, "", "")
	if total != 2 {
		t.Fatalf("total=%d", total)
	}
	var found bool
	for _, l := range links {
		if l.Code == "one" && l.Hits == 1 {
			found = true
		}
	}
	if !found {
		t.Fatalf("one/1 not in %+v", links)
	}
	s := st.Stats("two")
	if s == nil || s.URL != "https://two.com" || s.CreatedAt <= 0 {
		t.Fatalf("stats: %+v", s)
	}
}

func TestKVBulk(t *testing.T) {
	st := kvStore(t)
	urls := make([]string, 50)
	for i := range urls {
		urls[i] = "https://b.com/" + itoa(i)
	}
	codes := st.ShortenMany(urls, 0)
	for i, c := range codes {
		if u, ok := st.Resolve(c); !ok || u != urls[i] {
			t.Fatalf("bulk resolve %s: %q %v", c, u, ok)
		}
	}
}

func itoa(i int) string {
	return string(rune('0'+(i/10)%10)) + string(rune('0'+i%10))
}
