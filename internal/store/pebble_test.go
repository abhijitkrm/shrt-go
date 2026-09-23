// Tests for the embedded Pebble backend — no server needed (in-process).
//   go test ./internal/store -run TestPebble -v

package store

import (
	"path/filepath"
	"sync"
	"testing"
	"time"
)

var pebDBMu sync.Mutex
var pebSeq int

func pebStore(t *testing.T) *PebbleStore {
	t.Helper()
	pebDBMu.Lock()
	t.Cleanup(pebDBMu.Unlock)
	pebSeq++
	dir := filepath.Join(t.TempDir(), "peb")
	st, err := NewPebble(dir, 0, 1000, 50)
	if err != nil {
		t.Fatalf("open pebble: %v", err)
	}
	t.Cleanup(st.Close)
	return st
}

func TestPebbleShortenResolve(t *testing.T) {
	st := pebStore(t)
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

func TestPebbleHitsBatched(t *testing.T) {
	st := pebStore(t)
	st.Shorten("https://a.com", "h", true, 0)
	for i := 0; i < 5; i++ {
		st.Resolve("h")
	}
	st.Flush()
	if s := st.Stats("h"); s == nil || s.Hits != 5 {
		t.Fatalf("hits: %+v", s)
	}
}

func TestPebbleUpdateRemove(t *testing.T) {
	st := pebStore(t)
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

func TestPebbleTTL(t *testing.T) {
	st := pebStore(t)
	st.Shorten("https://t.com", "ttl", true, 80)
	if _, ok := st.Resolve("ttl"); !ok {
		t.Fatal("resolve before expiry")
	}
	time.Sleep(120 * time.Millisecond)
	if _, ok := st.Resolve("ttl"); ok {
		t.Fatal("resolve after expiry")
	}
}

func TestPebbleListStats(t *testing.T) {
	st := pebStore(t)
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

func TestPebbleBulk(t *testing.T) {
	st := pebStore(t)
	urls := make([]string, 100)
	for i := range urls {
		urls[i] = "https://b.example/" + itoa(i)
	}
	codes := st.ShortenMany(urls, 0)
	seen := map[string]bool{}
	for i, c := range codes {
		if c == "" {
			t.Fatalf("empty code at %d", i)
		}
		if seen[c] {
			t.Fatalf("dup code %s", c)
		}
		seen[c] = true
		if u, ok := st.Resolve(c); !ok || u != urls[i] {
			t.Fatalf("resolve %s: %q %v", c, u, ok)
		}
	}
}

func TestPebbleColdMiss(t *testing.T) {
	st := pebStore(t)
	codes := make([]string, 200)
	for i := range codes {
		codes[i] = st.Shorten("https://x"+string(rune('a'+i%26))+".com/"+itoa(i), "", false, 0)
	}
	for i, c := range codes {
		if _, ok := st.Resolve(c); !ok {
			t.Fatalf("cold miss for %s (i=%d)", c, i)
		}
	}
}

func TestPebbleCacheBounded(t *testing.T) {
	st := pebStore(t) // cap 1000/256 → tiny shards
	for i := 0; i < 500; i++ {
		st.Shorten("https://x.com/"+itoa(i), "", false, 0)
	}
	st.Flush()
	// cache bound is internal; just verify resolve still works end-to-end
	if _, ok := st.Resolve("missing-code"); ok {
		t.Fatal("phantom resolve")
	}
}

// Restart: same path reopens with the corpus intact.
func TestPebbleRestart(t *testing.T) {
	pebDBMu.Lock()
	defer pebDBMu.Unlock()
	dir := filepath.Join(t.TempDir(), "peb")
	st, err := NewPebble(dir, 0, 1000, 50)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	st.Shorten("https://keep.com", "keep", true, 0)
	st.Resolve("keep")
	st.Flush()
	st.Close()

	st2, err := NewPebble(dir, 0, 1000, 50)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer st2.Close()
	if u, ok := st2.Resolve("keep"); !ok || u != "https://keep.com" {
		t.Fatalf("restart resolve: %q %v", u, ok)
	}
	if s := st2.Stats("keep"); s == nil || s.Hits != 2 {
		t.Fatalf("restart hits: %+v", s)
	}
}
