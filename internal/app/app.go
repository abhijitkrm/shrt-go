// Package app holds the transport-agnostic request handler shared by the
// fasthttp and net/http frontends, plus response types and validation.
package app

import (
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/bytedance/sonic"

	"shrt-go/internal/metrics"
	"shrt-go/internal/store"
)

const (
	MaxBody      = 4096
	MaxBulkBody  = 1 << 20
	MaxBulkURLs  = 10_000
	MaxListLimit = 1000
)

// LinkTTLMS is the default AND max link lifetime (env LINK_TTL_MS, 1 day).
var LinkTTLMS = envInt64("LINK_TTL_MS", 86_400_000)

// CORSOrigin is the Access-Control-Allow-Origin value (env CORS_ORIGIN).
var CORSOrigin = envStr("CORS_ORIGIN", "*")

func envInt64(k string, def int64) int64 {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n
		}
	}
	return def
}

func envStr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// Links are immutable for the public API. PATCH/DELETE exist only when
// ADMIN_TOKEN is set, and require the x-admin-token header.
func adminOk(token string) bool {
	t := os.Getenv("ADMIN_TOKEN")
	return t != "" && token == t
}

// Reply is the transport-agnostic response.
type Reply struct {
	Status   int
	Location string
	Body     []byte
	CType    string
}

// ---------- UI ----------

var (
	uiOnce  sync.Once
	uiBytes []byte // nil when absent
)

// UIHTML loads ui/index.html once; nil when absent. SetUI injects content
// (tests / embedded fallback).
func UIHTML() []byte {
	uiOnce.Do(func() {
		b, err := os.ReadFile(filepath.Join("ui", "index.html"))
		if err == nil {
			uiBytes = b
		}
	})
	return uiBytes
}

// ---------- validation ----------

func codeOK(s string) bool {
	if len(s) == 0 || len(s) > 64 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c == '_' || c == '-') {
			return false
		}
	}
	return true
}

func isValidURL(raw string) bool {
	if len(raw) == 0 || len(raw) > 2048 {
		return false
	}
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return false
	}
	return strings.EqualFold(u.Scheme, "http") || strings.EqualFold(u.Scheme, "https")
}

func bad(err string) Reply {
	return Reply{Status: 400, Body: []byte(`{"error":"` + err + `"}`)}
}

func notFound() Reply {
	return Reply{Status: 404, Body: []byte(`{"error":"not found"}`)}
}

// ---------- request bodies ----------

type shortenReq struct {
	URL   *string  `json:"url"`
	Alias *string  `json:"alias"`
	TTLMS *float64 `json:"ttl_ms"`
}

func shortenOne(st *store.Store, p *shortenReq) Reply {
	if p.URL == nil || !isValidURL(*p.URL) {
		return bad("invalid url")
	}
	if p.Alias != nil && !codeOK(*p.Alias) {
		return bad("invalid alias")
	}
	ttl := LinkTTLMS
	if p.TTLMS != nil {
		if *p.TTLMS <= 0 {
			return bad("invalid ttl_ms")
		}
		ttl = int64(*p.TTLMS)
		if ttl > LinkTTLMS {
			ttl = LinkTTLMS
		}
	}
	alias := ""
	hasAlias := false
	if p.Alias != nil {
		alias, hasAlias = *p.Alias, true
	}
	code := st.Shorten(*p.URL, alias, hasAlias, ttl)
	if code == "" {
		return Reply{Status: 409, Body: []byte(`{"error":"alias taken"}`)}
	}
	b := make([]byte, 0, len(code)+32)
	b = append(b, `{"code":"`...)
	b = append(b, code...)
	b = append(b, `","short_url":"/`...)
	b = append(b, code...)
	b = append(b, '"', '}')
	return Reply{Status: 201, Body: b}
}

// bulk fast-path: prefix + length + no chars that could break the log line
func okBulkURL(u string) bool {
	return len(u) > 7 && len(u) <= 2048 &&
		(strings.HasPrefix(u, "http://") || strings.HasPrefix(u, "https://")) &&
		!strings.ContainsAny(u, "\"\\\n\r")
}

type bulkReq struct {
	URLs []string `json:"urls"`
}

func shortenBulk(st *store.Store, p *bulkReq) Reply {
	if len(p.URLs) == 0 || len(p.URLs) > MaxBulkURLs {
		return bad(`urls must be 1-` + strconv.Itoa(MaxBulkURLs) + ` valid http(s) urls`)
	}
	for _, u := range p.URLs {
		if !okBulkURL(u) {
			return bad(`urls must be 1-` + strconv.Itoa(MaxBulkURLs) + ` valid http(s) urls`)
		}
	}
	codes := st.ShortenMany(p.URLs, LinkTTLMS)
	b := make([]byte, 0, len(codes)*10+24)
	b = append(b, `{"count":`...)
	b = strconv.AppendInt(b, int64(len(codes)), 10)
	b = append(b, `,"codes":[`...)
	for i, c := range codes {
		if i > 0 {
			b = append(b, ',')
		}
		b = append(b, '"')
		b = append(b, c...)
		b = append(b, '"')
	}
	b = append(b, ']', '}')
	return Reply{Status: 201, Body: b}
}

type patchReq struct {
	URL   *string  `json:"url"`
	TTLMS *float64 `json:"ttl_ms"`
}

// ---------- handler ----------

// Handle is the transport-agnostic request handler. path includes the query
// string ("...?..."); body is the raw request body (nil when absent).
func Handle(st *store.Store, method, path string, body []byte, adminToken string) Reply {
	pathname := path
	query := ""
	if i := strings.IndexByte(path, '?'); i >= 0 {
		pathname, query = path[:i], path[i+1:]
	}

	metrics.Tick()

	if method == "OPTIONS" {
		return Reply{Status: 204}
	}

	switch method {
	case "GET":
		switch {
		case pathname == "/api/health":
			return Reply{Status: 200, Body: []byte(`{"ok":true}`)}
		case pathname == "/api/metrics":
			return Reply{Status: 200, Body: metrics.Snapshot()}
		case pathname == "/":
			html := UIHTML()
			if html == nil {
				return notFound()
			}
			return Reply{Status: 200, Body: html, CType: "text/html; charset=utf-8"}
		case pathname == "/api/links":
			p, _ := url.ParseQuery(query)
			limit := int64(50)
			if v := p.Get("limit"); v != "" {
				if n, err := strconv.ParseInt(v, 10, 64); err == nil && n != 0 {
					limit = n
				}
			}
			if limit < 1 {
				limit = 1
			}
			if limit > MaxListLimit {
				limit = MaxListLimit
			}
			offset := int64(0)
			if v := p.Get("offset"); v != "" {
				if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
					offset = n
				}
			}
			sortBy := "created"
			if p.Get("sort") == "hits" {
				sortBy = "hits"
			}
			links, total := st.List(int(limit), int(offset), sortBy, p.Get("q"))
			out, _ := sonic.Marshal(map[string]any{"links": links, "total": total})
			return Reply{Status: 200, Body: out}
		case strings.HasPrefix(pathname, "/api/stats/"):
			code := pathname[len("/api/stats/"):]
			link := st.Stats(code)
			if link == nil {
				return notFound()
			}
			out, _ := sonic.Marshal(link)
			return Reply{Status: 200, Body: out}
		default:
			code := pathname[1:]
			if codeOK(code) {
				if target, ok := st.Resolve(code); ok {
					return Reply{Status: 302, Location: target}
				}
			}
			return notFound()
		}

	case "POST":
		if pathname != "/api/shorten" && pathname != "/api/shorten/bulk" {
			return notFound()
		}
		if pathname == "/api/shorten" {
			var p shortenReq
			if err := sonic.Unmarshal(body, &p); err != nil {
				return bad("invalid json")
			}
			return shortenOne(st, &p)
		}
		var p bulkReq
		if err := sonic.Unmarshal(body, &p); err != nil {
			return bad("invalid json")
		}
		return shortenBulk(st, &p)

	case "PATCH", "DELETE":
		if !strings.HasPrefix(pathname, "/api/links/") || !adminOk(adminToken) {
			return notFound()
		}
		code := pathname[len("/api/links/"):]
		if !codeOK(code) {
			return bad("invalid code")
		}
		if method == "DELETE" {
			switch st.Remove(code) {
			case store.MutOK:
				return Reply{Status: 204}
			case store.MutMissing:
				return notFound()
			default:
				return Reply{Status: 409, Body: []byte(`{"error":"owned by another instance"}`)}
			}
		}
		var p patchReq
		if err := sonic.Unmarshal(body, &p); err != nil {
			return bad("invalid json")
		}
		if p.URL != nil {
			if !isValidURL(*p.URL) {
				return bad("invalid url")
			}
			ttl := int64(0)
			hasTTL := p.TTLMS != nil
			if hasTTL {
				ttl = int64(*p.TTLMS)
				if ttl > LinkTTLMS {
					ttl = LinkTTLMS
				}
			}
			switch st.Update(code, *p.URL, ttl, hasTTL) {
			case store.MutOK:
				return Reply{Status: 200, Body: []byte(`{"ok":true}`)}
			case store.MutMissing:
				return notFound()
			default:
				return Reply{Status: 409, Body: []byte(`{"error":"owned by another instance"}`)}
			}
		}
		return bad("nothing to update")
	}

	return notFound()
}
