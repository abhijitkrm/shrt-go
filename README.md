# shrt-go

High-performance URL shortener backend — Go port of
[shrt-ts](https://github.com/abhijitkrm/shrt-ts), tuned for maximum throughput.

* **HTTP**: [`fasthttp`](https://github.com/valyala/fasthttp) default (zero-alloc
  C-style engine, HTTP pipelining supported); `net/http` fallback (`SERVER=std`)
* **Storage**: custom append-only log (AOF) — sharded in-memory index (256
  shards, `RWMutex` + atomic hit counters) + batched `write()`/`fsync`. Reads
  never touch disk; writes are ~ns enqueue + one syscall batch per 5 ms.
* **JSON**: [`sonic`](https://github.com/bytedance/sonic) (SIMD) on the write
  path; log lines are hand-serialized (zero JSON on the read hot path).
* **Codes**: 8 chars = `ALPHABET[instance]` + 7 random base62 chars
  (62⁷ ≈ 3.5T per instance). The prefix shard-marks every code — unique across
  processes with zero coordination, and a read-miss knows exactly which sibling
  log to tail. Checked against the index and retried on collision.
* **Multi-instance**: `WORKERS=N` spawns N processes that **share one port via
  SO_REUSEPORT** (the kernel load-balances connections) with per-instance log
  shards (`data-<i>.log`). Siblings are discovered and tailed **lazily on
  read-miss** — writes never pay replication cost, so write throughput scales
  ~linearly with instance count.
* **Durability**: every append reaches the OS page cache within 5 ms and is
  fsync'd every 500 ms — a process crash loses ≤5 ms of writes, a machine
  crash ≤~500 ms (tunable constants in `internal/store/store.go`;
  snapshot+truncate via `Compact()`).

## Quickstart

```sh
go build -o shrt ./cmd/shrt
./shrt                      # fasthttp on :3000
SERVER=std ./shrt           # net/http fallback
WORKERS=4 ./shrt            # 4 instances sharing :3000 via SO_REUSEPORT
go test ./...               # test suite
go run ./cmd/bench          # build + spawn real servers + load scenarios
bash scripts/smoke.sh       # CLI smoke test
```

## API

| Method   | Path                | Body / Response |
|----------|---------------------|-----------------|
| `POST`   | `/api/shorten`      | `{url, alias?, ttl_ms?}` → `201 {code, short_url}`; `409` alias taken; `400` invalid. `ttl_ms` defaults to and is capped at `LINK_TTL_MS` (1 day) |
| `POST`   | `/api/shorten/bulk` | `{urls: […≤10000]}` → `201 {count, codes}` (body ≤1 MB, fast-path validation) |
| `GET`    | `/{code}`           | `302` + `Location`; `404` unknown/expired |
| `GET`    | `/api/links`        | `?limit(≤1000)&offset&sort=created\|hits&q=` → `{links, total}` (O(n) scan — admin path) |
| `GET`    | `/api/stats/{code}` | `{code, url, hits, created_at, expires_at}` |
| `PATCH`  | `/api/links/{code}` | admin only: `{url?, ttl_ms?}` → `200`; `404` missing/not-authorized; `409` remote-owned |
| `DELETE` | `/api/links/{code}` | admin only → `204`; `404` missing/not-authorized; `409` remote-owned |
| `OPTIONS`| any                 | `204` CORS preflight |
| `GET`    | `/`                 | single-file UI (`ui/index.html`) — 404 if absent |
| `GET`    | `/api/metrics`      | `{req_s, total, uptime_s, per_second[31]}` — live request counters |
| `GET`    | `/api/health`       | `{ok: true}` |

CORS: `Access-Control-Allow-Origin` on every response (`CORS_ORIGIN` env,
default `*`).

`PATCH`/`DELETE` are hidden unless `ADMIN_TOKEN` is set, then require the
`x-admin-token` header — links are immutable to the public. They're durable
only on the instance that owns the code (mutations are ordered within the
owner's log); `409` means route to the owning instance — for generated codes
that's `ALPHABET.indexOf(code[0])`.

Generated codes: exactly 8 chars, `[0-9a-zA-Z]` (`ALPHABET[instance]` prefix +
7 random). Aliases: `[0-9A-Za-z_-]{1,64}`. Single POST body ≤4 KB.

## Config (env)

| Var           | Default    | Meaning |
|---------------|------------|---------|
| `PORT`        | `3000`     | listen port (shared across workers via SO_REUSEPORT) |
| `DATA_DIR`    | `data`     | shard log directory (`data-<i>.log`, `data-<i>.snap`) |
| `SERVER`      | `fast`     | `fast` (fasthttp) or `std` (net/http) |
| `WORKERS`     | `1`        | processes sharing `PORT` |
| `INSTANCE`    | auto       | instance id (auto-claimed via `instance-<i>.lock` files) |
| `SEED`        | `0`        | bulk-insert N links if empty (random codes) |
| `HITS`        | `1`        | `0` disables hit counting |
| `TAIL_MS`     | `0`        | >0 enables periodic sibling-log polling (on-miss always on) |
| `CORS_ORIGIN` | `*`        | value of `Access-Control-Allow-Origin` |
| `ADMIN_TOKEN` | unset      | enables PATCH/DELETE; requests need `x-admin-token: <value>` |
| `LINK_TTL_MS` | `86400000` | default **and max** link lifetime — every link expires ≤1 day |
| `STORE`       | `aof`      | `aof` in-process engine, or `dragonfly`/`redis` external RESP KV |
| `DRAGONFLY_ADDR` | `127.0.0.1:6379` | RESP endpoint (`KV_ADDR` also accepted) |
| `CACHE`       | `100000`   | bounded hot FIFO entries kept in-process over the KV |
| `CACHE_TTL_MS` | `5000`    | staleness bound for cached entries |

With `STORE=dragonfly` the whole corpus lives in the RESP store (keys
`l:{code}` → `{exp}|{created}|{url}`, `h:{code}` → hit counter) — process
memory stays flat as links grow; a cache miss costs one `GET`. Writes and
admin mutations work on any node since the KV is the shared state. Live
tests: `SHRT_KV_ADDR=127.0.0.1:6379 go test ./internal/store -run TestKV`.

## Performance

Measured on an Apple Silicon MacBook, client and server sharing the machine —
absolute numbers are a floor. `go run ./cmd/bench` (64 conns, ~5s, 50k seeded
links).

| scenario                     | req/s      | rows/s      |
|------------------------------|-----------:|------------:|
| redirect (fast)              | ~197k      | —           |
| redirect (fast, pipelined ×10)| ~457k     | —           |
| mixed 95% read / 5% write    | ~185k      | —           |
| shorten (fast)               | ~158k      | 158k        |
| **bulk ×1000 (fast)**        | ~2.1k      | **~2.1M**   |
| bulk ×1000 (fast ×4 inst.)   | ~2.1k      | **~2.1M**   |
| redirect (std)               | ~134k      | —           |
| shorten (std)                | ~120k      | 120k        |

vs. the TypeScript original on similar hardware: 133k redirect / 71.6k shorten
/ 1.41M rows/s bulk across 4 instances. The Go port reads ~1.5× faster and
writes ~1.5–2× faster; the bulk path is limited by the client, not the server.

The read path is at its floor: one map lookup + one atomic add + one dirty-map
bump. The write path wins from write-behind + group commit + instance-disjoint
code space (no coordination).

## Layout

```
cmd/shrt        entry point (env config, worker supervisor, SO_REUSEPORT)
cmd/bench       self-contained load generator (keep-alive + pipelining)
internal/aof    append-only log: buffered appends, replay, tail readers, instance locks
internal/store  sharded index + hit batching + sibling tailing + compact
internal/app    transport-agnostic handler + fasthttp/net-http adapters
internal/metrics per-second ring-buffer request counters
ui/             single-page UI served at GET /
scripts/        smoke.sh, flood.sh
```

## Notes on differences from shrt-ts

* `WORKERS` shares a single port via `SO_REUSEPORT` instead of binding
  `PORT..PORT+N-1` — one port to load-balance against, no client-side routing.
* The in-memory index is sharded 256-way so the read hot path scales across
  cores; hit deltas batch per shard into the same 5 ms group commit.
* Log file format is byte-compatible: `{"c","u","a","e","i","n"}` rows,
  `{"h","d","i"}` hit deltas, `{"x"}` tombstones.
