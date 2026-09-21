// shrt — high-performance URL shortener.
//
// Env config:
//
//	PORT        3000    base listen port
//	DATA_DIR    data    shard log directory (data-<i>.log, data-<i>.snap)
//	SERVER      fast    "fast" (fasthttp) or "std" (net/http)
//	WORKERS     1       processes; all share PORT via SO_REUSEPORT
//	INSTANCE    auto    instance id (auto-claimed via instance-<i>.lock files)
//	SEED        0       bulk-insert N links if empty (random codes)
//	HITS        1       "0" disables hit counting
//	TAIL_MS     0       >0 enables periodic sibling-log polling (on-miss always on)
//	CORS_ORIGIN *       value of Access-Control-Allow-Origin
//	ADMIN_TOKEN unset   enables PATCH/DELETE; requests need x-admin-token: <value>
//	LINK_TTL_MS 86400000  default AND max link lifetime
package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"shrt-go/internal/app"
	"shrt-go/internal/store"
)

var (
	port     = envInt("PORT", 3000)
	dataDir  = envStr("DATA_DIR", "data")
	workers  = envInt("WORKERS", 1)
	seedN    = envInt("SEED", 0)
	server   = envStr("SERVER", "fast")
	instance = envInt("INSTANCE", -1)
)

func envInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
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

func main() {
	if workers > 1 && os.Getenv("SHRT_CHILD") == "" {
		runSupervisor(workers)
		return
	}
	if seedN > 0 {
		seed(seedN)
	}
	serve()
}

// seed bulk-inserts N links if the store is empty (through the write path).
func seed(n int) {
	s, err := store.New(dataDir, 0)
	if err != nil {
		fmt.Fprintln(os.Stderr, "seed:", err)
		return
	}
	if s.IsEmpty() {
		urls := make([]string, n)
		for i := range urls {
			urls[i] = "https://example.com/" + strconv.Itoa(i)
		}
		s.Seed(urls)
	}
	s.Close()
}

// runSupervisor spawns workers child processes. All children bind the same
// PORT via SO_REUSEPORT — the kernel load-balances connections across them.
func runSupervisor(n int) {
	if seedN > 0 {
		seed(seedN)
	}
	children := make([]*exec.Cmd, 0, n)
	for i := 0; i < n; i++ {
		cmd := exec.Command(os.Args[0], os.Args[1:]...)
		cmd.Env = append(os.Environ(),
			"SHRT_CHILD=1",
			"INSTANCE="+strconv.Itoa(i),
			"WORKERS=1",
			"SEED=0",
		)
		cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
		if err := cmd.Start(); err != nil {
			fmt.Fprintln(os.Stderr, "spawn worker", i, ":", err)
			continue
		}
		children = append(children, cmd)
	}
	fmt.Printf("supervisor: %d workers sharing :%d (pid %d)\n", len(children), port, os.Getpid())

	sig := make(chan os.Signal, 2)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	for _, c := range children {
		_ = c.Process.Signal(syscall.SIGTERM)
	}
	for _, c := range children {
		_ = c.Wait()
	}
}

// listen binds a TCP socket with SO_REUSEPORT so sibling processes can share
// the port (and single-process restarts don't hit TIME_WAIT bind errors).
func listen(p int) (net.Listener, error) {
	lc := net.ListenConfig{
		Control: func(_, _ string, c syscall.RawConn) error {
			var serr error
			if err := c.Control(func(fd uintptr) {
				serr = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_REUSEPORT, 1)
			}); err != nil {
				return err
			}
			return serr
		},
	}
	return lc.Listen(context.Background(), "tcp", ":"+strconv.Itoa(p))
}

func serve() {
	st, err := store.New(dataDir, instance)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	ln, err := listen(port)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to bind :%d: %v\n", port, err)
		os.Exit(1)
	}

	sig := make(chan os.Signal, 2)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	if server == "std" {
		srv := &http.Server{
			Handler:           app.StdHandler(st),
			ReadHeaderTimeout: 5 * time.Second,
		}
		go func() {
			<-sig
			_ = srv.Close()
		}()
		fmt.Printf("listening on :%d (pid %d)\n", port, os.Getpid())
		_ = srv.Serve(ln)
		st.Close()
		return
	}

	srv := app.NewFastServer(st)
	go func() {
		<-sig
		srv.Shutdown()
	}()
	fmt.Printf("fast listening on :%d (pid %d)\n", port, os.Getpid())
	_ = srv.Serve(ln)
	st.Close()
}
