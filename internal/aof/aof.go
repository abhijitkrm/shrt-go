// Package aof implements the append-only-log persistence used by the store:
// buffered line writes, batched fsync, replay, sibling-log tailing, and
// instance-id claiming via lock files.
package aof

import (
	"bytes"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
)

const NL = '\n'

// Aof is an append-only log: queued line writes flushed in one syscall batch.
type Aof struct {
	mu   sync.Mutex
	buf  bytes.Buffer
	f    *os.File
	Path string
}

func New(dir, name string) (*Aof, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	p := filepath.Join(dir, name)
	f, err := os.OpenFile(p, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &Aof{f: f, Path: p}, nil
}

// Push queues one line (newline added) for the next flush.
func (a *Aof) Push(line []byte) {
	a.mu.Lock()
	a.buf.Write(line)
	a.buf.WriteByte(NL)
	a.mu.Unlock()
}

// PendingBytes is the number of bytes queued but not yet written.
func (a *Aof) PendingBytes() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.buf.Len()
}

// Flush appends all queued lines in one write (page cache only; Sync fsyncs).
func (a *Aof) Flush() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.flushLocked()
}

func (a *Aof) flushLocked() {
	if a.buf.Len() == 0 {
		return
	}
	// O_APPEND positioned write of the whole batch.
	_, _ = a.f.Write(a.buf.Bytes())
	a.buf.Reset()
}

// Sync fsyncs the log file.
func (a *Aof) Sync() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.flushLocked()
	_ = a.f.Sync()
}

// Size returns the log's current size in bytes.
func (a *Aof) Size() int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	fi, err := a.f.Stat()
	if err != nil {
		return 0
	}
	return fi.Size()
}

// Truncate discards log contents (after a snapshot was written).
func (a *Aof) Truncate() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.flushLocked()
	_ = a.f.Close()
	f, err := os.OpenFile(a.Path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC|os.O_APPEND, 0o644)
	if err == nil {
		a.f = f
	}
}

// Close flushes pending bytes and closes the file.
func (a *Aof) Close() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.flushLocked()
	_ = a.f.Close()
}

// ReplayFile reads path and invokes cb for each complete line (torn tail and
// unparseable lines are ignored).
func ReplayFile(path string, cb func([]byte)) {
	buf, err := os.ReadFile(path)
	if err != nil {
		return
	}
	for start := 0; start < len(buf); {
		i := bytes.IndexByte(buf[start:], NL)
		if i < 0 {
			break
		}
		if i > 0 {
			cb(buf[start : start+i])
		}
		start += i + 1
	}
}

// TailReader tracks a sibling log's read offset; ReadNew returns new lines.
type TailReader struct {
	Path     string
	offset   int64
	leftover []byte
}

func TailFromStart(path string) *TailReader {
	return &TailReader{Path: path}
}

// ReadNew invokes cb for each complete line appended since the last call.
// If the file shrank (compacted/replaced), it rescans from the start — row
// applies are idempotent and snapshot rows were already merged.
func (t *TailReader) ReadNew(cb func([]byte)) {
	size := fileSize(t.Path)
	if size < t.offset {
		t.offset = 0
		t.leftover = t.leftover[:0]
	}
	if size <= t.offset {
		return
	}
	f, err := os.Open(t.Path)
	if err != nil {
		return
	}
	buf := make([]byte, size-t.offset)
	n, _ := f.ReadAt(buf, t.offset)
	_ = f.Close()
	buf = buf[:n]
	t.offset += int64(n)

	data := buf
	if len(t.leftover) > 0 {
		data = append(t.leftover, buf...)
	}
	start := 0
	for i := 0; i < len(data); i++ {
		if data[i] == NL {
			if i > start {
				cb(data[start:i])
			}
			start = i + 1
		}
	}
	t.leftover = append(t.leftover[:0], data[start:]...)
}

func (t *TailReader) Close() {}

// ClaimInstance claims the lowest free instance index via lock files in dir;
// stale locks (dead pid) are stolen. Returns the id and a release func.
func ClaimInstance(dir string) (int, func(), error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return 0, nil, err
	}
	for i := 0; i < 1024; i++ {
		lock := filepath.Join(dir, "instance-"+strconv.Itoa(i)+".lock")
		if tryLock(lock) {
			return i, func() { _ = os.Remove(lock) }, nil
		}
		// Lock exists: steal it if the holder is dead.
		b, err := os.ReadFile(lock)
		if err != nil {
			continue
		}
		pid, err := strconv.Atoi(strings.TrimSpace(string(b)))
		if err != nil || pid <= 0 {
			continue
		}
		if err := syscall.Kill(pid, 0); err == nil || err == syscall.EPERM {
			continue // alive
		}
		_ = os.Remove(lock)
		if tryLock(lock) {
			return i, func() { _ = os.Remove(lock) }, nil
		}
	}
	return 0, nil, errNoFreeInstance
}

var errNoFreeInstance = &claimError{"no free instance id"}

type claimError struct{ s string }

func (e *claimError) Error() string { return e.s }

func tryLock(lock string) bool {
	f, err := os.OpenFile(lock, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return false
	}
	_, _ = f.WriteString(strconv.Itoa(os.Getpid()))
	_ = f.Close()
	return true
}

// ShardFiles lists shard log names (data-*.log) present in dir, excluding own.
func ShardFiles(dir, own string) []string {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	var out []string
	for _, e := range ents {
		n := e.Name()
		if strings.HasPrefix(n, "data-") && strings.HasSuffix(n, ".log") && n != own {
			out = append(out, n)
		}
	}
	sort.Strings(out)
	return out
}

func fileSize(path string) int64 {
	fi, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return fi.Size()
}
