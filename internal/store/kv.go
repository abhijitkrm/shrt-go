// kv.go — minimal RESP (REdis Serialization Protocol) client. Works with
// Redis, DragonflyDB, KeyDB — anything speaking RESP. Zero dependencies.
//
// A Kv is a bounded pool of connections; each command checks a conn out,
// writes one request, reads one reply, and checks it back in. Pipeline
// writes N requests then reads N replies on one conn — one round-trip.

package store

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"time"
)

const kvPoolSize = 32

var errKv = errors.New("kv error")

// Resp is one decoded RESP reply.
type Resp struct {
	Kind byte   // '+', '-', ':', '$', '*'
	Str  []byte // '+'/'-'/'$' payload
	Int  int64  // ':'
	Arr  []Resp // '*'
}

// IsNull reports a $-1 / *-1 null.
func (r Resp) IsNull() bool { return r.Kind == '$' && r.Str == nil || r.Kind == '*' && r.Arr == nil }

type kvConn struct {
	c net.Conn
	r *bufio.Reader
}

// Kv is a thread-safe RESP client pool.
type Kv struct {
	addr string
	pool chan *kvConn
}

// Dial connects to addr (host:port) and verifies with PING.
func Dial(addr string) (*Kv, error) {
	k := &Kv{addr: addr, pool: make(chan *kvConn, kvPoolSize)}
	cn, err := k.conn()
	if err != nil {
		return nil, err
	}
	k.put(cn)
	if _, err := k.cmd([][]byte{[]byte("PING")}); err != nil {
		return nil, fmt.Errorf("ping %s: %w", addr, err)
	}
	return k, nil
}

func (k *Kv) conn() (*kvConn, error) {
	select {
	case cn := <-k.pool:
		return cn, nil
	default:
	}
	c, err := net.DialTimeout("tcp", k.addr, 3*time.Second)
	if err != nil {
		return nil, err
	}
	return &kvConn{c: c, r: bufio.NewReaderSize(c, 64<<10)}, nil
}

func (k *Kv) put(cn *kvConn) {
	select {
	case k.pool <- cn:
	default:
		_ = cn.c.Close()
	}
}

// drop discards a conn that may be mid-reply (protocol desync risk).
func (k *Kv) drop(cn *kvConn) { _ = cn.c.Close() }

var respScratch = sync.Pool{New: func() any {
	b := make([]byte, 0, 512)
	return &b
}}

func appendArg(dst []byte, a []byte) []byte {
	dst = append(dst, '$')
	dst = strconv.AppendInt(dst, int64(len(a)), 10)
	dst = append(dst, '\r', '\n')
	dst = append(dst, a...)
	return append(dst, '\r', '\n')
}

// cmd issues one command; args[0] is the verb.
func (k *Kv) cmd(args [][]byte) (Resp, error) {
	rs, err := k.pipe([][][]byte{args})
	if err != nil {
		return Resp{}, err
	}
	return rs[0], nil
}

// pipe sends every command then reads every reply — one flush, one read
// stream, N round-trips collapsed to one.
func (k *Kv) pipe(cmds [][][]byte) ([]Resp, error) {
	cn, err := k.conn()
	if err != nil {
		return nil, err
	}
	bp := respScratch.Get().(*[]byte)
	buf := (*bp)[:0]
	for _, args := range cmds {
		buf = append(buf, '*')
		buf = strconv.AppendInt(buf, int64(len(args)), 10)
		buf = append(buf, '\r', '\n')
		for _, a := range args {
			buf = appendArg(buf, a)
		}
	}
	if _, err = cn.c.Write(buf); err != nil {
		respScratch.Put(bp)
		k.drop(cn)
		return nil, err
	}
	*bp = buf[:0]
	respScratch.Put(bp)

	rs := make([]Resp, len(cmds))
	for i := range rs {
		rs[i], err = readResp(cn.r)
		if err != nil {
			k.drop(cn)
			return nil, err
		}
	}
	k.put(cn)
	return rs, nil
}

func readLine(r *bufio.Reader) ([]byte, error) {
	l, err := r.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	if len(l) < 2 || l[len(l)-2] != '\r' {
		return nil, errKv
	}
	return l[:len(l)-2], nil
}

func readResp(r *bufio.Reader) (Resp, error) {
	b, err := r.ReadByte()
	if err != nil {
		return Resp{}, err
	}
	switch b {
	case '+', '-':
		l, err := readLine(r)
		return Resp{Kind: b, Str: l}, err
	case ':':
		l, err := readLine(r)
		if err != nil {
			return Resp{}, err
		}
		n, err := strconv.ParseInt(string(l), 10, 64)
		return Resp{Kind: ':', Int: n}, err
	case '$':
		l, err := readLine(r)
		if err != nil {
			return Resp{}, err
		}
		n, _ := strconv.ParseInt(string(l), 10, 64)
		if n < 0 {
			return Resp{Kind: '$'}, nil
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(r, buf); err != nil {
			return Resp{}, err
		}
		return Resp{Kind: '$', Str: buf[:n]}, nil
	case '*':
		l, err := readLine(r)
		if err != nil {
			return Resp{}, err
		}
		n, _ := strconv.ParseInt(string(l), 10, 64)
		if n < 0 {
			return Resp{Kind: '*'}, nil
		}
		arr := make([]Resp, n)
		for i := range arr {
			arr[i], err = readResp(r)
			if err != nil {
				return Resp{}, err
			}
		}
		return Resp{Kind: '*', Arr: arr}, nil
	}
	return Resp{}, errKv
}

// ---- typed helpers ----

// Get returns the value at key, or nil.
func (k *Kv) Get(key []byte) ([]byte, error) {
	r, err := k.cmd([][]byte{[]byte("GET"), key})
	if err != nil || r.IsNull() {
		return nil, err
	}
	return r.Str, nil
}

// Set runs SET key val [PX ms] [NX]; returns whether the server stored it.
func (k *Kv) Set(key, val []byte, pxMs int64, nx bool) (bool, error) {
	args := [][]byte{[]byte("SET"), key, val}
	if pxMs > 0 {
		args = append(args, []byte("PX"), []byte(strconv.FormatInt(pxMs, 10)))
	}
	if nx {
		args = append(args, []byte("NX"))
	}
	r, err := k.cmd(args)
	if err != nil {
		return false, err
	}
	return r.Kind == '+' && string(r.Str) == "OK", nil
}

// Del returns keys removed.
func (k *Kv) Del(key []byte) (int64, error) {
	r, err := k.cmd([][]byte{[]byte("DEL"), key})
	return r.Int, err
}

// IncrByMany pipelines all deltas in one round-trip.
func (k *Kv) IncrByMany(deltas map[string]int64) error {
	if len(deltas) == 0 {
		return nil
	}
	cmds := make([][][]byte, 0, len(deltas))
	for key, d := range deltas {
		cmds = append(cmds, [][]byte{
			[]byte("INCRBY"), []byte(key), []byte(strconv.FormatInt(d, 10)),
		})
	}
	_, err := k.pipe(cmds)
	return err
}

// ScanEach invokes cb for every key matching pat. Admin path.
func (k *Kv) ScanEach(pat string, cb func([]byte)) error {
	cursor := []byte("0")
	for {
		r, err := k.cmd([][]byte{
			[]byte("SCAN"), cursor,
			[]byte("MATCH"), []byte(pat),
			[]byte("COUNT"), []byte("500"),
		})
		if err != nil || r.Kind != '*' || len(r.Arr) != 2 {
			return err
		}
		cursor = r.Arr[0].Str
		if r.Arr[1].Kind == '*' {
			for _, key := range r.Arr[1].Arr {
				cb(key.Str)
			}
		}
		if string(cursor) == "0" {
			return nil
		}
	}
}

// FlushDB clears the logical database (tests only).
func (k *Kv) FlushDB() error {
	_, err := k.cmd([][]byte{[]byte("FLUSHDB")})
	return err
}
