package store

import (
	"os"
	"path/filepath"
	"regexp"
	"testing"

	"shrt-go/internal/base62"
)

func tmpDir(t *testing.T) string {
	t.Helper()
	return t.TempDir()
}

func TestBase62Encode(t *testing.T) {
	for in, want := range map[uint64]string{0: "0", 1: "1", 61: "Z", 62: "10", 3843: "ZZ"} {
		if got := base62.Encode(in); got != want {
			t.Fatalf("Encode(%d) = %q, want %q", in, got, want)
		}
	}
}

func TestShortenGeneratesRandom8CharCodes(t *testing.T) {
	s, _ := New(":memory:", -1)
	defer s.Close()
	a := s.Shorten("https://example.com", "", false, 0)
	b := s.Shorten("https://example.org", "", false, 0)
	re := regexp.MustCompile(`^[0-9a-zA-Z]{8}$`)
	if !re.MatchString(a) || !re.MatchString(b) {
		t.Fatalf("bad codes %q %q", a, b)
	}
	if a == b {
		t.Fatal("duplicate codes")
	}
}

func TestResolveCountsHits(t *testing.T) {
	s, _ := New(":memory:", -1)
	defer s.Close()
	code := s.Shorten("https://example.com", "", false, 0)
	for i := 0; i < 2; i++ {
		u, ok := s.Resolve(code)
		if !ok || u != "https://example.com" {
			t.Fatalf("resolve failed: %q %v", u, ok)
		}
	}
	st := s.Stats(code)
	if st == nil || st.Hits != 2 || st.URL != "https://example.com" {
		t.Fatalf("bad stats %+v", st)
	}
}

func TestResolveMisses(t *testing.T) {
	s, _ := New(":memory:", -1)
	defer s.Close()
	if _, ok := s.Resolve("nope"); ok {
		t.Fatal("resolved unknown code")
	}
	if s.Stats("nope") != nil {
		t.Fatal("stats for unknown code")
	}
}

func TestAliasCollision(t *testing.T) {
	s, _ := New(":memory:", -1)
	defer s.Close()
	if got := s.Shorten("https://a.com", "my-link", true, 0); got != "my-link" {
		t.Fatalf("alias shorten = %q", got)
	}
	if u, _ := s.Resolve("my-link"); u != "https://a.com" {
		t.Fatal("alias resolve failed")
	}
	if got := s.Shorten("https://b.com", "my-link", true, 0); got != "" {
		t.Fatalf("collision returned %q", got)
	}
	gen := s.Shorten("https://c.com", "", false, 0)
	if gen == "my-link" {
		t.Fatal("generated code collided with alias")
	}
	if u, _ := s.Resolve(gen); u != "https://c.com" {
		t.Fatal("generated code resolve failed")
	}
}

func TestExpiredLinksStopResolving(t *testing.T) {
	s, _ := New(":memory:", -1)
	defer s.Close()
	code := s.Shorten("https://example.com", "", false, 5)
	if _, ok := s.Resolve(code); !ok {
		t.Fatal("fresh link didn't resolve")
	}
	sh := &s.shards[shardOf(code)]
	sh.mu.Lock()
	sh.data[code].e = nowMs() - 1 // simulate passage of time
	sh.mu.Unlock()
	if _, ok := s.Resolve(code); ok {
		t.Fatal("expired link resolved")
	}
}

func TestShortenManyAligned(t *testing.T) {
	s, _ := New(":memory:", -1)
	defer s.Close()
	urls := []string{"https://a.com", "https://b.com", "https://c.com"}
	codes := s.ShortenMany(urls, 0)
	if len(codes) != 3 {
		t.Fatalf("got %d codes", len(codes))
	}
	seen := map[string]bool{}
	for _, c := range codes {
		seen[c] = true
	}
	if len(seen) != 3 {
		t.Fatal("duplicate codes")
	}
	for i, c := range codes {
		if u, _ := s.Resolve(c); u != urls[i] {
			t.Fatalf("code %s -> %q, want %q", c, u, urls[i])
		}
	}
}

func TestPersistsAcrossReopen(t *testing.T) {
	dir := tmpDir(t)
	s1, _ := New(dir, -1)
	code := s1.Shorten("https://example.com", "", false, 0)
	s1.Resolve(code)
	s1.Resolve(code)
	s1.Close()

	s2, err := New(dir, 0) // same instance replays own log
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if u, _ := s2.Resolve(code); u != "https://example.com" {
		t.Fatal("row not replayed")
	}
	if st := s2.Stats(code); st.Hits != 3 {
		t.Fatalf("hits = %d, want 3", st.Hits)
	}
}

func TestCodesPrefixSharded(t *testing.T) {
	dir := tmpDir(t)
	a, _ := New(dir, 0)
	b, _ := New(dir, 1)
	defer a.Close()
	defer b.Close()
	ca := a.Shorten("https://a.com", "", false, 0)
	cb := b.Shorten("https://b.com", "", false, 0)
	if ca == cb || ca[0] == cb[0] {
		t.Fatalf("codes not prefix-disjoint: %q %q", ca, cb)
	}
}

func TestSiblingTailingConverges(t *testing.T) {
	dir := tmpDir(t)
	a, _ := New(dir, 0)
	b, _ := New(dir, 1)
	defer a.Close()
	defer b.Close()
	code := a.Shorten("https://a.com", "", false, 0)
	a.Flush()
	b.PollTails()
	if u, _ := b.Resolve(code); u != "https://a.com" {
		t.Fatal("sibling didn't converge row")
	}
}

func TestSiblingSeesHitsViaTail(t *testing.T) {
	dir := tmpDir(t)
	a, _ := New(dir, 0)
	b, _ := New(dir, 1)
	defer a.Close()
	defer b.Close()
	code := a.Shorten("https://a.com", "", false, 0)
	a.Flush()
	b.PollTails()
	a.Resolve(code)
	a.Resolve(code)
	a.Flush()
	b.PollTails()
	if st := b.Stats(code); st.Hits != 2 {
		t.Fatalf("sibling hits = %d, want 2", st.Hits)
	}
}

func TestRemoveTombstoneSurvivesReopen(t *testing.T) {
	dir := tmpDir(t)
	s1, _ := New(dir, -1)
	code := s1.Shorten("https://example.com", "", false, 0)
	if r := s1.Remove(code); r != MutOK {
		t.Fatalf("remove = %v", r)
	}
	if _, ok := s1.Resolve(code); ok {
		t.Fatal("removed link resolved")
	}
	s1.Close()

	s2, _ := New(dir, 0)
	defer s2.Close()
	if _, ok := s2.Resolve(code); ok {
		t.Fatal("tombstone not replayed")
	}
	if r := s2.Remove("nope"); r != MutMissing {
		t.Fatalf("remove missing = %v", r)
	}
}

func TestUpdatePersistsAcrossReopen(t *testing.T) {
	dir := tmpDir(t)
	s1, _ := New(dir, -1)
	code := s1.Shorten("https://old.example", "", false, 0)
	s1.Resolve(code) // 1 hit
	if r := s1.Update(code, "https://new.example", 60_000, true); r != MutOK {
		t.Fatalf("update = %v", r)
	}
	if u, _ := s1.Resolve(code); u != "https://new.example" {
		t.Fatal("update not applied")
	}
	s1.Close()

	s2, _ := New(dir, 0)
	defer s2.Close()
	e := s2.Stats(code)
	if e == nil || e.URL != "https://new.example" {
		t.Fatalf("stats after reopen: %+v", e)
	}
	if e.ExpiresAt == nil || *e.ExpiresAt <= nowMs() {
		t.Fatal("expiry not preserved")
	}
	if e.Hits < 1 {
		t.Fatalf("hits = %d, want >= 1", e.Hits)
	}
}

func TestRemoteOwnedMutationsReturnRemote(t *testing.T) {
	dir := tmpDir(t)
	a, _ := New(dir, 0)
	b, _ := New(dir, 1)
	defer a.Close()
	defer b.Close()
	code := a.Shorten("https://a.example", "", false, 0)
	a.Flush()
	b.PollTails()
	if r := b.Update(code, "https://x.example", 0, false); r != MutRemote {
		t.Fatalf("remote update = %v", r)
	}
	if r := b.Remove(code); r != MutRemote {
		t.Fatalf("remote remove = %v", r)
	}
	if r := a.Remove(code); r != MutOK {
		t.Fatalf("owner remove = %v", r)
	}
}

func TestListPaginatesSortsFilters(t *testing.T) {
	s, _ := New(":memory:", -1)
	defer s.Close()
	a := s.Shorten("https://aaa.example", "", false, 0)
	s.Shorten("https://bbb.example", "", false, 0)
	s.Shorten("https://ccc.example", "", false, 0)
	s.Resolve(a)
	s.Resolve(a)

	links, total := s.List(50, 0, "created", "")
	if total != 3 || len(links) != 3 {
		t.Fatalf("list total=%d len=%d", total, len(links))
	}
	page, _ := s.List(2, 0, "created", "")
	if len(page) != 2 {
		t.Fatalf("page len=%d", len(page))
	}
	byHits, _ := s.List(50, 0, "hits", "")
	if byHits[0].Code != a {
		t.Fatalf("top hit = %s, want %s", byHits[0].Code, a)
	}
	filtered, ftotal := s.List(50, 0, "created", "bbb")
	if ftotal != 1 || filtered[0].URL != "https://bbb.example" {
		t.Fatalf("filtered total=%d %+v", ftotal, filtered)
	}
}

func TestCompactPreservesRowsAndTruncates(t *testing.T) {
	dir := tmpDir(t)
	s, _ := New(dir, 0)
	code := s.Shorten("https://example.com", "", false, 0)
	s.Resolve(code)
	s.Compact()
	s.Close()

	if fi, err := os.Stat(filepath.Join(dir, "data-0.snap")); err != nil || fi.Size() == 0 {
		t.Fatal("snapshot missing")
	}
	s2, _ := New(dir, 0)
	defer s2.Close()
	if u, _ := s2.Resolve(code); u != "https://example.com" {
		t.Fatal("row lost after compact")
	}
	if st := s2.Stats(code); st.Hits != 2 {
		t.Fatalf("hits = %d, want 2", st.Hits)
	}
}
