package app

import (
	"io"
	"net"
	"net/http"
	"os"
	"strings"

	"shrt-go/internal/store"
)

var trustProxyStd = os.Getenv("TRUST_PROXY") != ""

func clientIPStd(r *http.Request) string {
	if trustProxyStd {
		if xff := r.Header.Get("x-forwarded-for"); xff != "" {
			if i := strings.IndexByte(xff, ','); i >= 0 {
				return strings.TrimSpace(xff[:i])
			}
			return strings.TrimSpace(xff)
		}
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// StdHandler adapts Handle to net/http (the stdlib fallback frontend).
func StdHandler(st store.API) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		method := r.Method
		path := r.URL.RequestURI()
		var body []byte
		if method == "POST" || method == "PATCH" {
			limit := int64(MaxBody)
			if pn, _, _ := strings.Cut(path, "?"); pn == "/api/shorten/bulk" {
				limit = MaxBulkBody
			}
			b, err := io.ReadAll(io.LimitReader(r.Body, limit+1))
			_ = r.Body.Close()
			if err != nil {
				w.WriteHeader(400)
				return
			}
			if int64(len(b)) > limit {
				writeStd(w, &Reply{Status: 413, Body: []byte(`{"error":"body too large"}`)})
				return
			}
			body = b
		}
		rp := Handle(st, method, path, body, r.Header.Get("x-admin-token"), clientIPStd(r))
		writeStd(w, &rp)
	})
}

func writeStd(w http.ResponseWriter, r *Reply) {
	h := w.Header()
	h.Set("access-control-allow-origin", CORSOrigin)
	for _, kv := range corsHeaders {
		h.Set(kv[0], kv[1])
	}
	if r.Location != "" {
		h.Set("location", r.Location)
		w.WriteHeader(r.Status)
		return
	}
	if r.CType != "" {
		h.Set("content-type", r.CType)
	} else {
		h.Set("content-type", "application/json")
	}
	w.WriteHeader(r.Status)
	_, _ = w.Write(r.Body)
}
