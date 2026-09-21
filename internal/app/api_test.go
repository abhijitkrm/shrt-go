package app

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"shrt-go/internal/store"
)

// runSuite boots the handler on each frontend and runs fn against it.
func runSuite(t *testing.T, fn func(t *testing.T, base string, c *http.Client)) {
	for _, srv := range []string{"std", "fast"} {
		t.Run(srv, func(t *testing.T) {
			st, err := store.New(":memory:", -1)
			if err != nil {
				t.Fatal(err)
			}
			defer st.Close()
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			var stop func()
			if srv == "fast" {
				fs := NewFastServer(st)
				go func() { _ = fs.Serve(ln) }()
				stop = func() { fs.Shutdown() }
			} else {
				hs := &http.Server{Handler: StdHandler(st)}
				go func() { _ = hs.Serve(ln) }()
				stop = func() { hs.Close() }
			}
			defer stop()
			c := &http.Client{
				Timeout: 10 * time.Second,
				CheckRedirect: func(req *http.Request, via []*http.Request) error {
					return http.ErrUseLastResponse
				},
			}
			fn(t, "http://"+ln.Addr().String(), c)
		})
	}
}

func get(t *testing.T, c *http.Client, url string) *http.Response {
	t.Helper()
	res, err := c.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func post(t *testing.T, c *http.Client, url, body string) *http.Response {
	t.Helper()
	res, err := c.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func body(t *testing.T, res *http.Response) []byte {
	t.Helper()
	defer res.Body.Close()
	b, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func jmap(t *testing.T, res *http.Response) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(body(t, res), &m); err != nil {
		t.Fatal(err)
	}
	return m
}

func shortenURL(t *testing.T, c *http.Client, base, u string) string {
	t.Helper()
	res := post(t, c, base+"/api/shorten", fmt.Sprintf(`{"url":%q}`, u))
	if res.StatusCode != 201 {
		t.Fatalf("shorten %q -> %d", u, res.StatusCode)
	}
	return jmap(t, res)["code"].(string)
}

func TestMain(m *testing.M) {
	// ui/index.html lives at the repo root; tests run from the package dir.
	_ = os.Chdir("../..")
	os.Exit(m.Run())
}

func TestHealth(t *testing.T) {
	runSuite(t, func(t *testing.T, base string, c *http.Client) {
		res := get(t, c, base+"/api/health")
		if res.StatusCode != 200 {
			t.Fatalf("status %d", res.StatusCode)
		}
		m := jmap(t, res)
		if m["ok"] != true {
			t.Fatalf("body %v", m)
		}
	})
}

func TestMetrics(t *testing.T) {
	runSuite(t, func(t *testing.T, base string, c *http.Client) {
		res := get(t, c, base+"/api/metrics")
		if res.StatusCode != 200 {
			t.Fatalf("status %d", res.StatusCode)
		}
		m := jmap(t, res)
		if _, ok := m["req_s"].(float64); !ok {
			t.Fatal("req_s missing")
		}
		if m["total"].(float64) < 1 {
			t.Fatal("total not counting")
		}
		if len(m["per_second"].([]any)) != 31 {
			t.Fatal("per_second length")
		}
	})
}

func TestUIServed(t *testing.T) {
	runSuite(t, func(t *testing.T, base string, c *http.Client) {
		res := get(t, c, base+"/")
		if res.StatusCode != 200 {
			t.Fatalf("status %d", res.StatusCode)
		}
		if ct := res.Header.Get("content-type"); !strings.Contains(ct, "text/html") {
			t.Fatalf("content-type %q", ct)
		}
		if !strings.Contains(string(body(t, res)), "<title>shrt") {
			t.Fatal("ui body")
		}
	})
}

func TestShortenRedirectStatsFlow(t *testing.T) {
	runSuite(t, func(t *testing.T, base string, c *http.Client) {
		res := post(t, c, base+"/api/shorten", `{"url":"https://example.com/some/path"}`)
		if res.StatusCode != 201 {
			t.Fatalf("status %d", res.StatusCode)
		}
		m := jmap(t, res)
		code := m["code"].(string)
		if m["short_url"] != "/"+code {
			t.Fatalf("short_url %v", m["short_url"])
		}

		redir := get(t, c, base+"/"+code)
		_ = body(t, redir)
		if redir.StatusCode != 302 {
			t.Fatalf("redirect %d", redir.StatusCode)
		}
		if loc := redir.Header.Get("location"); loc != "https://example.com/some/path" {
			t.Fatalf("location %q", loc)
		}

		st := jmap(t, get(t, c, base+"/api/stats/"+code))
		if st["url"] != "https://example.com/some/path" || st["hits"].(float64) != 1 {
			t.Fatalf("stats %v", st)
		}
	})
}

func TestTTLDefaultsAndCap(t *testing.T) {
	runSuite(t, func(t *testing.T, base string, c *http.Client) {
		const DAY = 86_400_000.0
		now := float64(time.Now().UnixMilli())

		code := shortenURL(t, c, base, "https://ttl-default.example")
		st := jmap(t, get(t, c, base+"/api/stats/"+code))
		if d := st["expires_at"].(float64) - (now + DAY); d < -5000 || d > 5000 {
			t.Fatalf("default ttl off by %f", d)
		}

		res := post(t, c, base+"/api/shorten", fmt.Sprintf(`{"url":"https://ttl-cap.example","ttl_ms":%f}`, 365*DAY))
		code2 := jmap(t, res)["code"].(string)
		st2 := jmap(t, get(t, c, base+"/api/stats/"+code2))
		if d := st2["expires_at"].(float64) - (now + DAY); d < -5000 || d > 5000 {
			t.Fatalf("capped ttl off by %f", d)
		}

		code3 := shortenURLBody(t, c, base, `{"url":"https://ttl-short.example","ttl_ms":5000}`)
		st3 := jmap(t, get(t, c, base+"/api/stats/"+code3))
		if d := st3["expires_at"].(float64) - (now + 5000); d < -5000 || d > 5000 {
			t.Fatalf("short ttl off by %f", d)
		}
	})
}

func shortenURLBody(t *testing.T, c *http.Client, base, bodyStr string) string {
	t.Helper()
	res := post(t, c, base+"/api/shorten", bodyStr)
	if res.StatusCode != 201 {
		t.Fatalf("status %d", res.StatusCode)
	}
	return jmap(t, res)["code"].(string)
}

func TestCustomAlias(t *testing.T) {
	runSuite(t, func(t *testing.T, base string, c *http.Client) {
		res := post(t, c, base+"/api/shorten", `{"url":"https://a.com","alias":"cool"}`)
		if res.StatusCode != 201 {
			t.Fatalf("status %d", res.StatusCode)
		}
		_ = body(t, res)
		redir := get(t, c, base+"/cool")
		_ = body(t, redir)
		if redir.Header.Get("location") != "https://a.com" {
			t.Fatal("alias redirect")
		}
		dup := post(t, c, base+"/api/shorten", `{"url":"https://b.com","alias":"cool"}`)
		_ = body(t, dup)
		if dup.StatusCode != 409 {
			t.Fatalf("dup status %d", dup.StatusCode)
		}
	})
}

func TestRejectsInvalidURL(t *testing.T) {
	runSuite(t, func(t *testing.T, base string, c *http.Client) {
		for _, u := range []string{"notaurl", "ftp://x.com", "javascript:alert(1)"} {
			res := post(t, c, base+"/api/shorten", fmt.Sprintf(`{"url":%q}`, u))
			_ = body(t, res)
			if res.StatusCode != 400 {
				t.Fatalf("%s -> %d", u, res.StatusCode)
			}
		}
	})
}

func TestRejectsInvalidJSONAndMissingURL(t *testing.T) {
	runSuite(t, func(t *testing.T, base string, c *http.Client) {
		for _, b := range []string{"{bad", "{}"} {
			res := post(t, c, base+"/api/shorten", b)
			_ = body(t, res)
			if res.StatusCode != 400 {
				t.Fatalf("%s -> %d", b, res.StatusCode)
			}
		}
	})
}

func TestNotFound(t *testing.T) {
	runSuite(t, func(t *testing.T, base string, c *http.Client) {
		for _, p := range []string{"/zzz", "/api/stats/zzz", "/a/b/c"} {
			res := get(t, c, base+p)
			_ = body(t, res)
			if res.StatusCode != 404 {
				t.Fatalf("%s -> %d", p, res.StatusCode)
			}
		}
	})
}

func TestBulkShorten(t *testing.T) {
	runSuite(t, func(t *testing.T, base string, c *http.Client) {
		urls := make([]string, 50)
		for i := range urls {
			urls[i] = fmt.Sprintf("https://bulk.example/%d", i)
		}
		jb, _ := json.Marshal(map[string]any{"urls": urls})
		res := post(t, c, base+"/api/shorten/bulk", string(jb))
		if res.StatusCode != 201 {
			t.Fatalf("status %d", res.StatusCode)
		}
		m := jmap(t, res)
		if m["count"].(float64) != 50 {
			t.Fatalf("count %v", m["count"])
		}
		codes := m["codes"].([]any)
		seen := map[string]bool{}
		for _, c := range codes {
			seen[c.(string)] = true
		}
		if len(seen) != 50 {
			t.Fatal("dup codes")
		}
		redir := get(t, c, base+"/"+codes[10].(string))
		_ = body(t, redir)
		if redir.Header.Get("location") != urls[10] {
			t.Fatalf("bulk redirect %q", redir.Header.Get("location"))
		}
	})
}

func TestBulkRejectsBadInput(t *testing.T) {
	runSuite(t, func(t *testing.T, base string, c *http.Client) {
		for _, urls := range []string{`[]`, `["ftp://x"]`, `["https://ok.com","nope"]`, `"notarray"`} {
			res := post(t, c, base+"/api/shorten/bulk", `{"urls":`+urls+`}`)
			_ = body(t, res)
			if res.StatusCode != 400 {
				t.Fatalf("%s -> %d", urls, res.StatusCode)
			}
		}
	})
}

func TestRejectsOversizedBody(t *testing.T) {
	runSuite(t, func(t *testing.T, base string, c *http.Client) {
		big := `{"url":"https://x.com/` + strings.Repeat("a", 5000) + `"}`
		res := post(t, c, base+"/api/shorten", big)
		_ = body(t, res)
		if res.StatusCode != 400 && res.StatusCode != 413 {
			t.Fatalf("oversized -> %d", res.StatusCode)
		}
	})
}

func TestCORS(t *testing.T) {
	runSuite(t, func(t *testing.T, base string, c *http.Client) {
		req, _ := http.NewRequest("OPTIONS", base+"/api/shorten", nil)
		pre, err := c.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = body(t, pre)
		if pre.StatusCode != 204 {
			t.Fatalf("preflight %d", pre.StatusCode)
		}
		if pre.Header.Get("access-control-allow-origin") != "*" {
			t.Fatal("allow-origin")
		}
		if !strings.Contains(pre.Header.Get("access-control-allow-methods"), "DELETE") {
			t.Fatal("allow-methods")
		}
		res := get(t, c, base+"/api/health")
		_ = body(t, res)
		if res.Header.Get("access-control-allow-origin") != "*" {
			t.Fatal("origin on response")
		}
	})
}

func TestListPaginationSortSearch(t *testing.T) {
	runSuite(t, func(t *testing.T, base string, c *http.Client) {
		shortenURL(t, c, base, "https://list-one.example")
		shortenURL(t, c, base, "https://list-two.example")

		res := get(t, c, base+"/api/links?limit=5&offset=0")
		m := jmap(t, res)
		if m["total"].(float64) < 1 {
			t.Fatal("total")
		}
		links := m["links"].([]any)
		if len(links) > 5 || len(links) == 0 {
			t.Fatal("limit")
		}
		first := links[0].(map[string]any)
		if first["code"] == nil || first["url"] == nil {
			t.Fatal("link shape")
		}

		sm := jmap(t, get(t, c, base+"/api/links?q="+("list-one")))
		for _, l := range sm["links"].([]any) {
			lm := l.(map[string]any)
			if !strings.Contains(lm["url"].(string), "list-one") && !strings.Contains(lm["code"].(string), "list-one") {
				t.Fatal("q filter")
			}
		}

		tm := jmap(t, get(t, c, base+"/api/links?sort=hits&limit=3"))
		var prev float64 = 1 << 60
		for _, l := range tm["links"].([]any) {
			h := l.(map[string]any)["hits"].(float64)
			if h > prev {
				t.Fatal("hits not sorted desc")
			}
			prev = h
		}
	})
}

func TestAdminMutations(t *testing.T) {
	runSuite(t, func(t *testing.T, base string, c *http.Client) {
		os.Unsetenv("ADMIN_TOKEN")
		code := shortenURL(t, c, base, "https://before.example")

		req, _ := http.NewRequest("DELETE", base+"/api/links/"+code, nil)
		res, err := c.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = body(t, res)
		if res.StatusCode != 404 {
			t.Fatalf("delete without token %d", res.StatusCode)
		}

		os.Setenv("ADMIN_TOKEN", "secret")
		defer os.Unsetenv("ADMIN_TOKEN")

		// wrong token still 404
		req2, _ := http.NewRequest("DELETE", base+"/api/links/"+code, nil)
		req2.Header.Set("x-admin-token", "wrong")
		res2, _ := c.Do(req2)
		_ = body(t, res2)
		if res2.StatusCode != 404 {
			t.Fatalf("delete wrong token %d", res2.StatusCode)
		}

		// correct token -> PATCH works
		pr, _ := http.NewRequest("PATCH", base+"/api/links/"+code,
			strings.NewReader(`{"url":"https://after.example","ttl_ms":60000}`))
		pr.Header.Set("content-type", "application/json")
		pr.Header.Set("x-admin-token", "secret")
		pres, err := c.Do(pr)
		if err != nil {
			t.Fatal(err)
		}
		_ = body(t, pres)
		if pres.StatusCode != 200 {
			t.Fatalf("patch %d", pres.StatusCode)
		}
		redir := get(t, c, base+"/"+code)
		_ = body(t, redir)
		if redir.Header.Get("location") != "https://after.example" {
			t.Fatal("patch not applied")
		}
		st := jmap(t, get(t, c, base+"/api/stats/"+code))
		if st["expires_at"].(float64) <= float64(time.Now().UnixMilli()) {
			t.Fatal("ttl not applied")
		}

		// invalid url -> 400
		br, _ := http.NewRequest("PATCH", base+"/api/links/"+code,
			strings.NewReader(`{"url":"notaurl"}`))
		br.Header.Set("content-type", "application/json")
		br.Header.Set("x-admin-token", "secret")
		bres, _ := c.Do(br)
		_ = body(t, bres)
		if bres.StatusCode != 400 {
			t.Fatalf("patch bad url %d", bres.StatusCode)
		}

		// nothing to update -> 400
		nr, _ := http.NewRequest("PATCH", base+"/api/links/nope", strings.NewReader(`{}`))
		nr.Header.Set("x-admin-token", "secret")
		nres, _ := c.Do(nr)
		_ = body(t, nres)
		if nres.StatusCode != 400 {
			t.Fatalf("patch empty %d", nres.StatusCode)
		}
	})
}

func TestDeleteRemovesAndFreesAlias(t *testing.T) {
	runSuite(t, func(t *testing.T, base string, c *http.Client) {
		os.Setenv("ADMIN_TOKEN", "secret")
		defer os.Unsetenv("ADMIN_TOKEN")

		res := post(t, c, base+"/api/shorten", `{"url":"https://del.example","alias":"todelete"}`)
		_ = body(t, res)
		if res.StatusCode != 201 {
			t.Fatal("create")
		}

		dr, _ := http.NewRequest("DELETE", base+"/api/links/todelete", nil)
		dr.Header.Set("x-admin-token", "secret")
		dres, _ := c.Do(dr)
		_ = body(t, dres)
		if dres.StatusCode != 204 {
			t.Fatalf("delete %d", dres.StatusCode)
		}
		gone := get(t, c, base+"/todelete")
		_ = body(t, gone)
		if gone.StatusCode != 404 {
			t.Fatal("deleted still resolves")
		}
		dr2, _ := http.NewRequest("DELETE", base+"/api/links/todelete", nil)
		dr2.Header.Set("x-admin-token", "secret")
		dres2, _ := c.Do(dr2)
		_ = body(t, dres2)
		if dres2.StatusCode != 404 {
			t.Fatal("re-delete")
		}
		reuse := post(t, c, base+"/api/shorten", `{"url":"https://new.example","alias":"todelete"}`)
		_ = body(t, reuse)
		if reuse.StatusCode != 201 {
			t.Fatal("alias not freed")
		}
	})
}
