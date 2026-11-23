package remote

import (
	"io"
	"io/fs"
	"net"
	"os"
	"runtime"
	"testing"
	"time"

	filesystem "github.com/go-filesystems/interface"
	"github.com/go-filesystems/osfs"
)

type budgetPeer struct {
	t *testing.T
	l *link
	c net.Conn
}

func budgetDial(t *testing.T, fsys filesystem.Filesystem) *budgetPeer {
	t.Helper()
	a, b := net.Pipe()
	go func() { _ = Serve(b, fsys) }()
	t.Cleanup(func() { a.Close() })
	p := &budgetPeer{t: t, l: &link{rw: a}, c: a}
	a.SetDeadline(time.Now().Add(50 * time.Second))
	if _, op, _, err := p.l.recv(); err != nil || op != opHello {
		t.Fatalf("hello: op %d, %v", op, err)
	}
	return p
}

func (p *budgetPeer) call(op byte, payload []byte) (byte, []byte) {
	p.t.Helper()
	if err := p.l.send(1, op, payload); err != nil {
		p.t.Fatal(err)
	}
	_, st, out, err := p.l.recv()
	if err != nil {
		p.t.Fatal(err)
	}
	return st, out
}

func readFileReq(path string) []byte {
	var e enc
	e.str(path)
	return e.b
}

func sparseFile(t *testing.T, size int64) filesystem.Filesystem {
	t.Helper()
	fsys, err := osfs.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { fsys.Close() })
	p := budgetDial(t, fsys)
	var e enc
	e.str("/big")
	e.bytes([]byte("x"))
	e.u64(0o644)
	if st, _ := p.call(opWriteFile, e.b); st != statusOK {
		t.Fatalf("WriteFile: status %d", st)
	}
	e = enc{}
	e.str("/big")
	e.i64(size)
	if st, _ := p.call(opTruncate, e.b); st != statusOK {
		t.Fatalf("Truncate: status %d", st)
	}
	return fsys
}

// ReadFile used to read the WHOLE file into memory and only then compare it
// with the frame limit (audit 2026-11-22, finding 3: 513 MiB allocated to
// refuse a 21-byte request). The size is now checked first.
func TestReadFileRefusedBeforeReading(t *testing.T) {
	const sparse = 200 << 20
	fsys := sparseFile(t, sparse)
	p := budgetDial(t, fsys)

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	st, _ := p.call(opReadFile, readFileReq("/big"))
	runtime.ReadMemStats(&after)
	spent := int64(after.TotalAlloc - before.TotalAlloc)
	if st != statusErr {
		t.Fatalf("expected a refusal, got status %d", st)
	}
	if bound := int64(MaxFrame / 4); spent > bound {
		t.Fatalf("server allocated %d MiB to refuse one ReadFile request; bound %d MiB", spent>>20, bound>>20)
	}
}

// The same refusal when the driver cannot open files, where the size comes
// from Stat.
func TestReadFileRefusedBeforeReadingWithoutOpener(t *testing.T) {
	fsys := plain{sparseFile(t, 200<<20)}
	p := budgetDial(t, fsys)
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	st, _ := p.call(opReadFile, readFileReq("/big"))
	runtime.ReadMemStats(&after)
	if st != statusErr {
		t.Fatalf("expected a refusal, got status %d", st)
	}
	if spent := int64(after.TotalAlloc - before.TotalAlloc); spent > MaxFrame/4 {
		t.Fatalf("server allocated %d MiB to refuse one ReadFile request", spent>>20)
	}
}

// plain hides every optional interface of the filesystem it wraps.
type plain struct{ filesystem.Filesystem }

func TestReadFileSmallFileBothPaths(t *testing.T) {
	for name, wrap := range map[string]func(filesystem.Filesystem) filesystem.Filesystem{
		"opener":  func(f filesystem.Filesystem) filesystem.Filesystem { return f },
		"nopener": func(f filesystem.Filesystem) filesystem.Filesystem { return plain{f} },
	} {
		t.Run(name, func(t *testing.T) {
			inner, err := osfs.Open(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer inner.Close()
			data := make([]byte, 3*maxChunk+17) // several pieces
			for i := range data {
				data[i] = byte(i)
			}
			if err := inner.WriteFile("/f", data, 0o644); err != nil {
				t.Fatal(err)
			}
			p := budgetDial(t, wrap(inner))
			st, out := p.call(opReadFile, readFileReq("/f"))
			if st != statusOK {
				t.Fatalf("status %d", st)
			}
			d := &dec{b: out}
			got := d.bytes(MaxFrame)
			if d.done() != nil || string(got) != string(data) {
				t.Fatalf("read back %d bytes, want %d", len(got), len(data))
			}
			if st, _ := p.call(opReadFile, readFileReq("/missing")); st != statusErr {
				t.Fatalf("missing file: status %d", st)
			}
		})
	}
}

// liar reports a size it does not honour: Stat says small, ReadFile returns
// more than a frame.
type liar struct {
	plain
	n int
}

func (l liar) ReadFile(string) ([]byte, error) { return make([]byte, l.n), nil }

func TestReadFileLengthChecked(t *testing.T) {
	inner := emptyFS(t)
	if err := inner.WriteFile("/x", []byte("tiny"), 0o644); err != nil {
		t.Fatal(err)
	}
	p := budgetDial(t, liar{plain{inner}, MaxFrame})
	// A server that sent the oversized reply (or none) must fail this test at
	// once, not after budgetDial's long deadline.
	p.c.SetDeadline(time.Now().Add(5 * time.Second))
	if st, _ := p.call(opReadFile, readFileReq("/x")); st != statusErr {
		t.Fatalf("a reply over the frame limit: status %d", st)
	}
}

func emptyFS(t *testing.T) filesystem.Filesystem {
	t.Helper()
	fsys, err := osfs.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { fsys.Close() })
	return fsys
}

// shrinking is an opener whose files are shorter than their Size says, or
// report a negative one, or make no progress.
type shrinking struct {
	plain
	size, have int64
	err        error
	stall      bool
}

func (s shrinking) OpenFile(string) (filesystem.File, error) { return shrunk(s), nil }

type shrunk shrinking

func (s shrunk) Size() int64  { return s.size }
func (s shrunk) Close() error { return nil }
func (s shrunk) ReadAt(b []byte, off int64) (int, error) {
	if s.err != nil {
		return 0, s.err
	}
	if s.stall {
		return 0, nil
	}
	if off >= s.have {
		return 0, io.EOF
	}
	n := copy(b, make([]byte, min(int64(len(b)), s.have-off)))
	return n, nil
}

func TestReadFileOpenerEdgeCases(t *testing.T) {
	cases := map[string]struct {
		fs     shrinking
		status byte
		length int
	}{
		"shrank":   {shrinking{size: 100, have: 40}, statusOK, 40},
		"negative": {shrinking{size: -5}, statusOK, 0},
		"error":    {shrinking{size: 10, err: fs.ErrPermission}, statusErr, 0},
		"exact":    {shrinking{size: 10, have: 10}, statusOK, 10},
		"stalled":  {shrinking{size: 10, stall: true}, statusErr, 0},
		"toobig":   {shrinking{size: MaxFrame}, statusErr, 0},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			c.fs.plain = plain{emptyFS(t)}
			p := budgetDial(t, c.fs)
			st, out := p.call(opReadFile, readFileReq("/x"))
			if st != c.status {
				t.Fatalf("status %d, want %d", st, c.status)
			}
			if st == statusOK {
				d := &dec{b: out}
				if got := d.bytes(MaxFrame); len(got) != c.length {
					t.Fatalf("%d bytes, want %d", len(got), c.length)
				}
			}
		})
	}
}

// blockingFS holds every WriteFile until released, as a slow disk does.
type blockingFS struct {
	*osfs.FS
	release chan struct{}
}

func (b blockingFS) WriteFile(p string, data []byte, perm os.FileMode) error {
	<-b.release
	runtime.KeepAlive(data) // a real write is still using its data while it waits
	return nil
}

// wire.go promises "the reader allocates no more than one frame's worth".
// It allocated one frame per request IN FLIGHT: 64 frames of 32 MiB, 2 GiB per
// connection (audit 2026-11-22, finding 6). The payloads now come from a
// budget of MaxFrame bytes taken before they are allocated; the requests
// that do not fit wait in the transport, and all of them are answered once
// the filesystem lets go.
func TestInFlightFramesBounded(t *testing.T) {
	inner, err := osfs.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer inner.Close()
	fsys := blockingFS{FS: inner, release: make(chan struct{})}
	p := budgetDial(t, fsys)

	var e enc
	e.str("/f")
	e.bytes(make([]byte, MaxFrame-64))
	e.u64(0o644)
	fr := frame(7, opWriteFile, e.b)

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	const n = 6
	wrote := make(chan error, 1)
	go func() {
		for i := 0; i < n; i++ {
			if _, err := p.c.Write(fr); err != nil {
				wrote <- err
				return
			}
		}
		wrote <- nil
	}()
	time.Sleep(time.Second) // the first frame is in; the others wait unread
	runtime.GC()
	runtime.ReadMemStats(&after)
	retained := int64(after.HeapAlloc) - int64(before.HeapAlloc)
	if bound := int64(2 * MaxFrame); retained > bound {
		t.Fatalf("%d MiB retained with the first of %d requests in flight; bound %d MiB", retained>>20, n, bound>>20)
	}
	close(fsys.release)
	// Replies are read while the writer is still writing: the pipe is
	// unbuffered, so a reply nobody reads holds its budget.
	for i := 0; i < n; i++ {
		if _, st, _, err := p.l.recv(); err != nil || st != statusOK {
			t.Fatalf("reply %d: status %d, %v", i, st, err)
		}
	}
	if err := <-wrote; err != nil {
		t.Fatal(err)
	}
}
