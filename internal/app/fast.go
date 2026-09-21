package app

import (
	"strings"

	"github.com/valyala/fasthttp"

	"shrt-go/internal/store"
)

var corsHeaders = [][2]string{
	{"access-control-allow-methods", "GET,POST,PATCH,DELETE,OPTIONS"},
	{"access-control-allow-headers", "content-type"},
	{"access-control-max-age", "86400"},
}

func respond(ctx *fasthttp.RequestCtx, r *Reply) {
	resp := &ctx.Response
	resp.SetStatusCode(r.Status)
	h := &resp.Header
	h.Set("access-control-allow-origin", CORSOrigin)
	for _, kv := range corsHeaders {
		h.Set(kv[0], kv[1])
	}
	if r.Location != "" {
		h.Set("location", r.Location)
		return
	}
	if r.CType != "" {
		h.Set("content-type", r.CType)
	} else {
		h.Set("content-type", "application/json")
	}
	resp.SetBody(r.Body)
}

// FastHandler adapts Handle to fasthttp (the high-performance frontend).
func FastHandler(st store.API) fasthttp.RequestHandler {
	return func(ctx *fasthttp.RequestCtx) {
		method := string(ctx.Method())
		path := string(ctx.RequestURI())
		var body []byte
		if method == "POST" || method == "PATCH" {
			limit := MaxBody
			if pn, _, _ := strings.Cut(path, "?"); pn == "/api/shorten/bulk" {
				limit = MaxBulkBody
			}
			body = ctx.PostBody()
			if len(body) > limit {
				respond(ctx, &Reply{Status: 413, Body: []byte(`{"error":"body too large"}`)})
				return
			}
		}
		r := Handle(st, method, path, body, string(ctx.Request.Header.Peek("x-admin-token")))
		respond(ctx, &r)
	}
}

// NewFastServer builds a tuned fasthttp server for the store.
func NewFastServer(st store.API) *fasthttp.Server {
	return &fasthttp.Server{
		Handler:                      FastHandler(st),
		NoDefaultServerHeader:        true,
		NoDefaultContentType:         true,
		DisablePreParseMultipartForm: true,
		ReadBufferSize:               4096,
		// keep-alive on, unlimited requests per conn (like the TS uWS build)
		MaxRequestsPerConn: 0,
	}
}
