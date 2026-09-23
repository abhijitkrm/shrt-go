package app

// RATE_LIMIT integration test — runs last (file order) so the OnceLock'd
// limiter can't poison other tests' POST traffic in this binary.

import (
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"shrt-go/internal/ratelimit"
	"shrt-go/internal/store"
)

func TestPerIPRateLimit(t *testing.T) {
	os.Setenv("RATE_LIMIT", "1")
	os.Setenv("RATE_LIMIT_BURST", "2")
	defer func() {
		os.Unsetenv("RATE_LIMIT")
		os.Unsetenv("RATE_LIMIT_BURST")
	}()
	rl = ratelimit.New() // rebind: OnceLock may have fired before env was set
	st, err := store.New(":memory:", -1)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	fs := NewFastServer(st)
	go func() { _ = fs.Serve(ln) }()
	defer fs.Shutdown()
	base := "http://" + ln.Addr().String()
	c := &http.Client{Timeout: 10 * time.Second}

	post := func() int {
		res, err := c.Post(base+"/api/shorten", "application/json",
			strings.NewReader(`{"url":"https://rl.example"}`))
		if err != nil {
			t.Fatal(err)
		}
		_ = body(t, res)
		return res.StatusCode
	}
	if got := post(); got != 201 {
		t.Fatalf("first post %d", got)
	}
	if got := post(); got != 201 {
		t.Fatalf("second post %d", got)
	}
	if got := post(); got != 429 {
		t.Fatalf("burst exhausted: %d", got)
	}
	// GET is not rate limited
	res := get(t, c, base+"/nope")
	if res.StatusCode != 404 {
		t.Fatalf("get %d", res.StatusCode)
	}
	_ = body(t, res)
}
