// bench spawns real shrt servers and drives load with a small raw-TCP
// load generator (keep-alive + optional HTTP pipelining), mirroring the
// shrt-ts autocannon scenarios.
//
// Usage: go run ./cmd/bench   (env: BENCH_DURATION=5 CONNECTIONS=64)
package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	duration    = envInt("BENCH_DURATION", 5)
	connections = envInt("CONNECTIONS", 64)
	keyspace    = 50_000
	bin         = filepath.Join(os.TempDir(), "shrt-bench-bin")
	portSeq     = 4300
)

func envInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func fmtN(n float64) string {
	return fmt.Sprintf("%.0f", n)
}

// ---------- tiny load generator ----------

type result struct {
	reqs    int64
	non2xx  int64
	errs    int64
	latSum  float64
	latVals []float64
}

// readResp parses one HTTP/1.1 response; returns the status code.
func readResp(r *bufio.Reader) (int, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return 0, err
	}
	parts := strings.SplitN(line, " ", 3)
	if len(parts) < 2 {
		return 0, fmt.Errorf("bad status line %q", line)
	}
	code, err := strconv.Atoi(parts[1])
	if err != nil {
		return 0, err
	}
	cl := 0
	chunked := false
	for {
		h, err := r.ReadString('\n')
		if err != nil {
			return 0, err
		}
		if h == "\r\n" || h == "\n" {
			break
		}
		k, v, ok := strings.Cut(h, ":")
		if !ok {
			continue
		}
		v = strings.TrimSpace(v)
		if strings.EqualFold(k, "content-length") {
			cl, _ = strconv.Atoi(v)
		} else if strings.EqualFold(k, "transfer-encoding") && strings.Contains(v, "chunked") {
			chunked = true
		}
	}
	if chunked {
		// consume chunked body
		for {
			sz, err := r.ReadString('\n')
			if err != nil {
				return 0, err
			}
			n, err := strconv.ParseInt(strings.TrimSpace(sz), 16, 64)
			if err != nil {
				return 0, err
			}
			if n == 0 {
				_, _ = r.ReadString('\n') // trailing CRLF (ignore trailers)
				break
			}
			if _, err := io.CopyN(io.Discard, r, n+2); err != nil {
				return 0, err
			}
		}
	} else if cl > 0 {
		if _, err := io.CopyN(io.Discard, r, int64(cl)); err != nil {
			return 0, err
		}
	}
	return code, nil
}

// blast drives `reqs` (cycled) over `conns` keep-alive connections for
// `duration`, with `pipe`-deep HTTP pipelining.
func blast(host string, conns int, reqs [][]byte, pipe int, dur time.Duration) result {
	var wg sync.WaitGroup
	var total, non2xx, errs atomic.Int64
	var latSum atomic.Int64 // µs * 1000 fixed point
	var mu sync.Mutex
	var lats []float64
	deadline := time.Now().Add(dur)

	for c := 0; c < conns; c++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			conn, err := net.Dial("tcp", host)
			if err != nil {
				errs.Add(1)
				return
			}
			defer conn.Close()
			br := bufio.NewReaderSize(conn, 64<<10)
			i := seed % len(reqs)
			var myLat []float64
			for time.Now().Before(deadline) {
				start := time.Now()
				batch := pipe
				for p := 0; p < pipe; p++ {
					if _, err := conn.Write(reqs[i]); err != nil {
						errs.Add(1)
						batch = p
						break
					}
					i = (i + 1) % len(reqs)
				}
				if batch == 0 {
					break
				}
				got := 0
				for got < batch {
					code, err := readResp(br)
					if err != nil {
						errs.Add(1)
						break
					}
					got++
					if code < 200 || code >= 400 {
						non2xx.Add(1)
					}
				}
				if got == 0 {
					break
				}
				el := float64(time.Since(start).Microseconds()) / 1000.0 / float64(got) // ms/req
				latSum.Add(int64(el * 1000))
				total.Add(int64(got))
				myLat = append(myLat, el)
			}
			mu.Lock()
			lats = append(lats, myLat...)
			mu.Unlock()
		}(c)
	}
	wg.Wait()
	sort.Float64s(lats)
	p99 := 0.0
	if len(lats) > 0 {
		p99 = lats[int(float64(len(lats))*0.99)]
	}
	avg := 0.0
	if total.Load() > 0 {
		avg = float64(latSum.Load()) / 1000.0 / float64(total.Load())
	}
	return result{reqs: total.Load(), non2xx: non2xx.Load(), errs: errs.Load(), latSum: avg, latVals: []float64{avg, p99}}
}

func report(name string, res result, rowsPerReq float64) {
	rps := float64(res.reqs) / float64(duration)
	fmt.Printf("%-26s %10s req/s  %10s rows/s  lat avg %6.2fms  p99 %6.2fms  non2xx/3xx %d  err %d\n",
		name, fmtN(rps), fmtN(rps*rowsPerReq), res.latVals[0], res.latVals[1], res.non2xx, res.errs)
}

// ---------- server lifecycle ----------

func waitHealthy(port int) {
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		res, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/api/health", port))
		if err == nil && res.StatusCode == 200 {
			res.Body.Close()
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	panic(fmt.Sprintf("server :%d did not start", port))
}

func startServer(env map[string]string, ports ...int) *exec.Cmd {
	e := os.Environ()
	for k, v := range env {
		e = append(e, k+"="+v)
	}
	cmd := exec.Command(bin)
	cmd.Env = e
	cmd.Stdout = nil
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		panic(err)
	}
	for _, p := range ports {
		waitHealthy(p)
	}
	return cmd
}

func stopServer(cmd *exec.Cmd) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	_ = cmd.Process.Signal(os.Interrupt)
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		_ = cmd.Process.Kill()
		<-done
	}
}

// ---------- request templates ----------

func getReq(path string) []byte {
	return []byte("GET " + path + " HTTP/1.1\r\nHost: x\r\n\r\n")
}

func postReq(path, body string) []byte {
	return []byte("POST " + path + " HTTP/1.1\r\nHost: x\r\ncontent-type: application/json\r\ncontent-length: " +
		strconv.Itoa(len(body)) + "\r\n\r\n" + body)
}

// makeCodes creates n aliased links via the API so the bench knows valid codes.
func makeCodes(port, n int) []string {
	codes := make([]string, n)
	for i := 0; i < n; i++ {
		codes[i] = fmt.Sprintf("bk%d", i)
		res, err := http.Post(fmt.Sprintf("http://127.0.0.1:%d/api/shorten", port),
			"application/json",
			strings.NewReader(fmt.Sprintf(`{"url":"https://bench.example/%d","alias":"%s"}`, i, codes[i])))
		if err != nil || res.StatusCode != 201 {
			panic(fmt.Sprintf("alias seed failed: %v", err))
		}
		res.Body.Close()
	}
	return codes
}

func envFor(extra map[string]string, tag string) map[string]string {
	p := portSeq
	portSeq += 10
	dir := filepath.Join(os.TempDir(), fmt.Sprintf("bench-go-%s-%d", tag, p))
	os.RemoveAll(dir)
	env := map[string]string{"PORT": strconv.Itoa(p), "DATA_DIR": dir}
	for k, v := range extra {
		env[k] = v
	}
	return env
}

func main() {
	fmt.Printf("bench: %d conns x %ds, keyspace %d\n\n", connections, duration, keyspace)

	// build the server binary once
	build := exec.Command("go", "build", "-o", bin, "./cmd/shrt")
	build.Stderr = os.Stderr
	if err := build.Run(); err != nil {
		panic(err)
	}
	defer os.Remove(bin)

	writeReq := postReq("/api/shorten", `{"url":"https://bench.example/write"}`)
	const bulkN = 1000
	var sb strings.Builder
	sb.WriteString(`{"urls":[`)
	for i := 0; i < bulkN; i++ {
		if i > 0 {
			sb.WriteByte(',')
		}
		fmt.Fprintf(&sb, `"https://b.example/%d"`, i)
	}
	sb.WriteString(`]}`)
	bulkReq := postReq("/api/shorten/bulk", sb.String())

	// --- fasthttp, single instance ---
	{
		env := envFor(map[string]string{"SERVER": "fast", "SEED": strconv.Itoa(keyspace)}, "fast1")
		port, _ := strconv.Atoi(env["PORT"])
		srv := startServer(env, port)
		codes := makeCodes(port, 100)
		var reqs [][]byte
		for _, c := range codes {
			reqs = append(reqs, getReq("/"+c))
		}
		report("redirect (fast)", blast(fmt.Sprintf("127.0.0.1:%d", port), connections, reqs, 1, time.Duration(duration)*time.Second), 1)
		report("redirect (fast, p10)", blast(fmt.Sprintf("127.0.0.1:%d", port), connections, reqs, 10, time.Duration(duration)*time.Second), 1)
		mixed := append([][]byte{}, reqs[:95]...)
		for i := 0; i < 5; i++ {
			mixed = append(mixed, writeReq)
		}
		report("mixed 95/5 (fast)", blast(fmt.Sprintf("127.0.0.1:%d", port), connections, mixed, 1, time.Duration(duration)*time.Second), 1)
		report("shorten (fast)", blast(fmt.Sprintf("127.0.0.1:%d", port), connections, [][]byte{writeReq}, 1, time.Duration(duration)*time.Second), 1)
		report("bulk x1000 (fast)", blast(fmt.Sprintf("127.0.0.1:%d", port), connections/4, [][]byte{bulkReq}, 1, time.Duration(duration)*time.Second), bulkN)
		stopServer(srv)
	}

	// --- net/http comparison ---
	{
		env := envFor(map[string]string{"SERVER": "std", "SEED": strconv.Itoa(keyspace)}, "std1")
		port, _ := strconv.Atoi(env["PORT"])
		srv := startServer(env, port)
		codes := makeCodes(port, 100)
		var reqs [][]byte
		for _, c := range codes {
			reqs = append(reqs, getReq("/"+c))
		}
		report("redirect (std)", blast(fmt.Sprintf("127.0.0.1:%d", port), connections, reqs, 1, time.Duration(duration)*time.Second), 1)
		report("shorten (std)", blast(fmt.Sprintf("127.0.0.1:%d", port), connections, [][]byte{writeReq}, 1, time.Duration(duration)*time.Second), 1)
		stopServer(srv)
	}

	// --- fast multi-instance: 4 procs sharing the port via SO_REUSEPORT ---
	{
		env := envFor(map[string]string{"SERVER": "fast", "WORKERS": "4", "SEED": strconv.Itoa(keyspace)}, "fast4")
		port, _ := strconv.Atoi(env["PORT"])
		srv := startServer(env, port)
		report("bulk x1000 (fast x4)", blast(fmt.Sprintf("127.0.0.1:%d", port), connections/2, [][]byte{bulkReq}, 1, time.Duration(duration)*time.Second), bulkN)
		codes := makeCodes(port, 100)
		var reqs [][]byte
		for _, c := range codes {
			reqs = append(reqs, getReq("/"+c))
		}
		// warm every connection once so lazy tailing merges aliases
		for _, c := range codes {
			res, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/%s", port, c))
			if err == nil {
				res.Body.Close()
			}
		}
		report("redirect (fast x4)", blast(fmt.Sprintf("127.0.0.1:%d", port), connections, reqs, 1, time.Duration(duration)*time.Second), 1)
		stopServer(srv)
	}
}
