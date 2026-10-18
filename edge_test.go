// SPDX-License-Identifier: BSD-3-Clause

package remote

import (
	"bytes"
	"errors"
	"io"
	"io/fs"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	filesystem "github.com/go-filesystems/interface"
)

// plainFS has Filesystem and nothing else. Its Stat answers with a ModTime of
// either form, depending on the path, and its errors are not errnos.
type plainFS struct{ big bool }

var errPlain = errors.New("plain failure")

func (plainFS) Close() error { return nil }
func (p plainFS) ReadFile(path string) ([]byte, error) {
	if p.big {
		return make([]byte, MaxFrame), nil
	}
	return nil, &fs.PathError{Op: "read", Path: path, Err: fs.ErrNotExist}
}
func (plainFS) ListDir(string) ([]filesystem.DirEntry, error) { return nil, errPlain }
func (plainFS) Stat(path string) (filesystem.Stat, error) {
	base := filesystem.NewStat(0o100644, 3, 7)
	switch path {
	case "unix":
		return unixStatOf{base}, nil
	case "time":
		return timeStatOf{base}, nil
	}
	return base, nil
}
func (plainFS) WriteFile(string, []byte, os.FileMode) error {
	return &os.SyscallError{Syscall: "write", Err: errPlain}
}
func (plainFS) ReadLink(string) (string, error) { return "", errPlain }
func (plainFS) MkDir(string, os.FileMode) error { return errPlain }
func (plainFS) DeleteFile(string) error         { return errPlain }
func (plainFS) DeleteDir(string) error          { return errPlain }
func (plainFS) Rename(string, string) error     { return errPlain }

type unixStatOf struct{ filesystem.Stat }

func (unixStatOf) ModTime() int64 { return 1700000000 }

type timeStatOf struct{ filesystem.Stat }

func (timeStatOf) ModTime() time.Time { return time.Unix(1700000000, 42) }

// A filesystem with none of the optional interfaces: remote's type still
// has them, says it cannot (Can), and they answer errors.ErrUnsupported.
func TestPlainFilesystem(t *testing.T) {
	rem := pair(t, plainFS{})
	for _, c := range []Capability{CanLabel, CanSymlink, CanLink, CanTruncate, CanMetadata, CanOpen} {
		if rem.Can(c) {
			t.Errorf("capability %b announced", c)
		}
	}
	for name, err := range map[string]error{
		"symlink":  rem.Symlink("a", "b"),
		"link":     rem.Link("a", "b"),
		"truncate": rem.Truncate("a", 1),
		"chmod":    rem.Chmod("a", 0),
		"chown":    rem.Chown("a", 1, 1),
		"chtimes":  rem.Chtimes("a", time.Now(), time.Now()),
	} {
		if !errors.Is(err, errors.ErrUnsupported) {
			t.Errorf("%s: %v", name, err)
		}
	}
	if _, err := rem.OpenFile("a"); !errors.Is(err, errors.ErrUnsupported) {
		t.Errorf("open: %v", err)
	}
	if rem.Label() != "" {
		t.Error("a label from a filesystem with none")
	}
	// Stat keeps the ModTime the driver gave, in its form; none if it gave none.
	st, _ := rem.Stat("unix")
	if m, ok := st.(interface{ ModTime() int64 }); !ok || m.ModTime() != 1700000000 {
		t.Errorf("unix ModTime: %#v", st)
	}
	st, _ = rem.Stat("time")
	if m, ok := st.(interface{ ModTime() time.Time }); !ok || !m.ModTime().Equal(time.Unix(1700000000, 42)) {
		t.Errorf("time ModTime: %#v", st)
	}
	st, _ = rem.Stat("none")
	if _, ok := st.(interface{ ModTime() int64 }); ok {
		t.Error("a ModTime invented")
	}
	if st.Mode() != 0o100644 || st.Size() != 3 || st.Inode() != 7 {
		t.Errorf("stat %v %v %v", st.Mode(), st.Size(), st.Inode())
	}
	// Errors that are not errnos keep their text and their kinds.
	_, err := rem.ReadFile("x")
	var pe *fs.PathError
	if !errors.As(err, &pe) || !errors.Is(err, fs.ErrNotExist) || errors.Is(err, fs.ErrExist) || pe.Op != "read" {
		t.Errorf("read: %v", err)
	}
	var se *os.SyscallError
	if err := rem.WriteFile("x", nil, 0); !errors.As(err, &se) || se.Syscall != "write" || err.Error() != "write: plain failure" {
		t.Errorf("write: %v", err)
	}
	if err := rem.MkDir("x", 0); err == nil || err.Error() != "plain failure" {
		t.Errorf("mkdir: %v", err)
	}
	if _, err := rem.ListDir("x"); err == nil {
		t.Error("listdir")
	}
	if _, err := rem.ReadLink("x"); err == nil {
		t.Error("readlink")
	}
	if rem.DeleteFile("x") == nil || rem.DeleteDir("x") == nil || rem.Rename("x", "y") == nil {
		t.Error("delete or rename")
	}
}

// A file larger than a frame is refused by ReadFile, which says to open it.
func TestReadFileTooLarge(t *testing.T) {
	rem := pair(t, plainFS{big: true})
	if _, err := rem.ReadFile("x"); err == nil || !strings.Contains(err.Error(), "open the file instead") {
		t.Fatalf("%v", err)
	}
	if err := rem.WriteFile("x", make([]byte, MaxFrame), 0); err == nil || !strings.Contains(err.Error(), "over the limit") {
		t.Fatalf("a write over a frame: %v", err)
	}
}

// fullFS has every optional interface, and files that fail on demand.
type fullFS struct {
	plainFS
	opened atomic.Int64
	closed atomic.Int64
	ro     bool
}

func (*fullFS) Label() string                              { return "VOL" }
func (*fullFS) Symlink(string, string) error               { return nil }
func (*fullFS) Link(string, string) error                  { return nil }
func (*fullFS) Truncate(string, int64) error               { return nil }
func (*fullFS) Chmod(string, os.FileMode) error            { return nil }
func (*fullFS) Chown(string, uint32, uint32) error         { return nil }
func (*fullFS) Chtimes(string, time.Time, time.Time) error { return nil }
func (f *fullFS) OpenFile(path string) (filesystem.File, error) {
	f.opened.Add(1)
	h := &fakeFile{fs: f, name: path}
	if f.ro {
		return h, nil
	}
	return &fakeWritable{h}, nil
}

type fakeFile struct {
	fs   *fullFS
	name string
}

func (h *fakeFile) ReadAt(p []byte, off int64) (int, error) {
	if h.name == "bad" {
		return 0, errPlain
	}
	return copy(p, "abc"), nil // never EOF, and short: a broken reader
}
func (h *fakeFile) Size() int64  { return 3 }
func (h *fakeFile) Close() error { h.fs.closed.Add(1); return nil }

type fakeWritable struct{ *fakeFile }

func (h *fakeWritable) WriteAt(p []byte, off int64) (int, error) {
	if h.name == "bad" {
		return 1, errPlain
	}
	return len(p) / 2, nil // short, with no error: a broken writer
}
func (h *fakeWritable) Truncate(int64) error { return errPlain }
func (h *fakeWritable) Sync() error          { return errPlain }

func TestFullFilesystem(t *testing.T) {
	ffs := &fullFS{}
	rem := pair(t, ffs)
	if rem.Label() != "VOL" {
		t.Error("label")
	}
	if rem.Symlink("a", "b") != nil || rem.Link("a", "b") != nil || rem.Truncate("a", 1) != nil ||
		rem.Chmod("a", 0) != nil || rem.Chown("a", 1, 2) != nil || rem.Chtimes("a", time.Now(), time.Now()) != nil {
		t.Error("an optional interface failed")
	}
	// A reader that comes back short without EOF is caught.
	f, _ := rem.OpenFile("a")
	if _, err := f.ReadAt(make([]byte, 10), 0); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("short read: %v", err)
	}
	// A writer that comes back short without an error is caught too.
	w := f.(filesystem.WritableFile)
	if n, err := w.WriteAt(make([]byte, 10), 0); n != 5 || !errors.Is(err, io.ErrShortWrite) {
		t.Errorf("short write: %d, %v", n, err)
	}
	if w.Truncate(1) == nil || w.Sync() == nil {
		t.Error("truncate or sync")
	}
	_ = f.Close()
	// Errors from the file, with a partial count where there is one.
	bad, _ := rem.OpenFile("bad")
	if _, err := bad.ReadAt(make([]byte, 4), 0); err == nil || err.Error() != "plain failure" {
		t.Errorf("read error: %v", err)
	}
	if n, err := bad.(filesystem.WritableFile).WriteAt(make([]byte, 4), 0); n != 1 || err == nil {
		t.Errorf("write error: %d, %v", n, err)
	}
	// A file left open is closed by the server when the connection ends.
	_, _ = rem.OpenFile("left")
	_ = rem.Close()
	deadline := time.Now().Add(5 * time.Second)
	for ffs.closed.Load() < ffs.opened.Load() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if ffs.closed.Load() != ffs.opened.Load() {
		// "a" closed by the client; "bad" and "left", which it never
		// closed, by the server when the connection ended.
		t.Errorf("opened %d, closed %d", ffs.opened.Load(), ffs.closed.Load())
	}
	// Calls on a closed filesystem.
	if _, err := rem.Stat("a"); !errors.Is(err, fs.ErrClosed) {
		t.Errorf("after close: %v", err)
	}
	if bad.Size() != 3 {
		t.Error("the size at open, when the server is gone")
	}
}

// A read-only file from a server: writing to it is refused by the server
// even when a client asks (raw).
func TestServerRefusesWritesToReadOnly(t *testing.T) {
	c, l := rawServer(t, &fullFS{ro: true})
	_ = c
	id := l.open(t, "a")
	for _, op := range []byte{opFileWriteAt, opFileTruncate, opFileSync} {
		e := &enc{}
		e.u64(id)
		switch op {
		case opFileWriteAt:
			e.i64(0)
			e.bytes([]byte("x"))
		case opFileTruncate:
			e.i64(0)
		}
		if err := l.expectErr(t, op, e.b); !errors.Is(err, fs.ErrPermission) {
			t.Errorf("op %d: %v", op, err)
		}
	}
}

// rawLink is a client speaking the wire directly, to send what a well-behaved
// client never would.
type rawLink struct {
	*link
	id uint64
}

func rawServer(t *testing.T, fsys filesystem.Filesystem) (net.Conn, *rawLink) {
	t.Helper()
	a, b := net.Pipe()
	done := make(chan error, 1)
	go func() { done <- Serve(b, fsys) }()
	l := &rawLink{link: &link{rw: a}}
	if _, code, _, err := l.recv(); err != nil || code != opHello {
		t.Fatalf("hello: %d %v", code, err)
	}
	t.Cleanup(func() { _ = a.Close(); <-done })
	return a, l
}

func (l *rawLink) roundTrip(t *testing.T, op byte, payload []byte) (byte, []byte) {
	t.Helper()
	l.id++
	if err := l.send(l.id, op, payload); err != nil {
		t.Fatal(err)
	}
	id, status, out, err := l.recv()
	if err != nil || id != l.id {
		t.Fatalf("reply %d to %d: %v", id, l.id, err)
	}
	return status, out
}

func (l *rawLink) expectErr(t *testing.T, op byte, payload []byte) error {
	t.Helper()
	status, out := l.roundTrip(t, op, payload)
	if status != statusErr {
		t.Fatalf("op %d answered %d, not an error", op, status)
	}
	return decodeErr(&dec{b: out})
}

func (l *rawLink) open(t *testing.T, path string) uint64 {
	t.Helper()
	status, out := l.roundTrip(t, opOpenFile, pathEnc(path).b)
	if status != statusOK {
		t.Fatalf("open: %v", decodeErr(&dec{b: out}))
	}
	return (&dec{b: out}).u64()
}

// Every op, given a payload that does not decode, answers malformed -- and
// the connection goes on.
func TestServerMalformedRequests(t *testing.T) {
	_, l := rawServer(t, &fullFS{})
	for op := opHello + 1; op < opLast; op++ {
		for _, payload := range [][]byte{{0xff}, append(make([]byte, 200), 1)} {
			if err := l.expectErr(t, op, payload); err == nil || !strings.Contains(err.Error(), "malformed") {
				t.Errorf("op %d with %d bytes: %v", op, len(payload), err)
			}
		}
	}
	// Bounds the decoder alone cannot know.
	id := l.open(t, "a")
	read := func(off int64) []byte { e := &enc{}; e.u64(id); e.i64(off); e.u64(1); return e.b }
	write := func(off int64) []byte { e := &enc{}; e.u64(id); e.i64(off); e.bytes([]byte("x")); return e.b }
	chown := func(uid uint64) []byte { e := pathEnc("a"); e.u64(uid); e.u64(0); return e.b }
	for name, c := range map[string]struct {
		op      byte
		payload []byte
	}{
		"negative read":    {opFileReadAt, read(-1)},
		"negative write":   {opFileWriteAt, write(-1)},
		"uid over 32 bits": {opChown, chown(1 << 40)},
		"read too large":   {opFileReadAt, func() []byte { e := &enc{}; e.u64(id); e.i64(0); e.u64(maxChunk + 1); return e.b }()},
	} {
		if err := l.expectErr(t, c.op, c.payload); err == nil || !strings.Contains(err.Error(), "malformed") {
			t.Errorf("%s: %v", name, err)
		}
	}
	// A handle that is not open.
	for _, op := range []byte{opFileSize, opFileClose, opFileReadAt} {
		e := &enc{}
		e.u64(999)
		if op == opFileReadAt {
			e.i64(0)
			e.u64(1)
		}
		if err := l.expectErr(t, op, e.b); !errors.Is(err, fs.ErrClosed) {
			t.Errorf("op %d on a closed handle: %v", op, err)
		}
	}
}

// Frames that end the connection, each for its own reason: an unknown op or a
// length out of bounds is malformed -- refused before anything is allocated
// for it -- and a frame cut short is an unexpected EOF. Checking only that
// Serve returned an error once let a server that believed a 4 GiB length,
// allocated it, and then failed on the closed pipe pass.
func TestServerMalformedFrames(t *testing.T) {
	for name, c := range map[string]struct {
		raw  []byte
		want error
	}{
		"unknown op":         {frame(1, opLast, nil), errMalformed},
		"hello as a request": {frame(1, opHello, nil), errMalformed},
		"too short":          {[]byte{0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0}, errMalformed},
		"too long":           {[]byte{0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0, 0, 0, 0, 0, 0}, errMalformed},
		"cut short":          {frame(1, opStat, pathEnc("abcdef").b)[:16], io.ErrUnexpectedEOF},
	} {
		t.Run(name, func(t *testing.T) {
			a, b := net.Pipe()
			done := make(chan error, 1)
			go func() { done <- Serve(b, plainFS{}) }()
			l := &link{rw: a}
			if _, _, _, err := l.recv(); err != nil {
				t.Fatal(err)
			}
			go func() { _, _ = a.Write(c.raw); _ = a.Close() }()
			if err := <-done; !errors.Is(err, c.want) {
				t.Fatalf("Serve: %v, want %v", err, c.want)
			}
		})
	}
}

// The server holds at most maxOpen files for one connection.
func TestServerMaxOpen(t *testing.T) {
	_, l := rawServer(t, &fullFS{})
	for range maxOpen {
		l.open(t, "a")
	}
	if err := l.expectErr(t, opOpenFile, pathEnc("a").b); err == nil || !strings.Contains(err.Error(), "files open") {
		t.Fatalf("%v", err)
	}
}

// fakeServer answers with whatever the test writes, after a hello.
func fakeServer(t *testing.T, hello []byte, answer func(l *link, id uint64, op byte, payload []byte)) *FS {
	t.Helper()
	a, b := net.Pipe()
	go func() {
		l := &link{rw: b}
		if hello != nil {
			_, _ = b.Write(hello)
		}
		for {
			id, op, payload, err := l.recv()
			if err != nil {
				_ = b.Close()
				return
			}
			answer(l, id, op, payload)
		}
	}()
	f, err := New(a)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })
	return f
}

func okHello() []byte { e := &enc{}; e.u64(0); return frame(0, opHello, e.b) }

func TestClientRefusesWhatIsNotAServer(t *testing.T) {
	extra := &enc{}
	extra.u64(0)
	extra.u64(0)
	for name, raw := range map[string][]byte{
		"not a hello":      frame(0, opStat, nil),
		"hello with an id": frame(5, opHello, nil),
		"hello too long":   frame(0, opHello, extra.b),
		"nothing":          nil,
	} {
		t.Run(name, func(t *testing.T) {
			a, b := net.Pipe()
			go func() { _, _ = b.Write(raw); _ = b.Close() }()
			if _, err := New(a); err == nil {
				t.Fatal("New took it")
			}
		})
	}
}

// Replies a client must not believe.
func TestClientRefusesBadReplies(t *testing.T) {
	ok := func(e *enc) func(l *link, id uint64, op byte, p []byte) {
		return func(l *link, id uint64, op byte, p []byte) { _ = l.send(id, statusOK, e.b) }
	}
	lie := &enc{}
	lie.u64(1 << 40) // a ListDir count no frame could hold
	badStat := &enc{}
	badStat.u64(1 << 20) // a mode over 16 bits
	badStat.u64(0)
	badStat.u64(0)
	badStat.u64(0)
	badStat.i64(0)
	badFlags := &enc{}
	for range 3 {
		badFlags.u64(0)
	}
	badFlags.u64(3) // both ModTime forms at once
	badFlags.i64(0)
	badType := &enc{}
	badType.u64(1)
	badType.u64(1)
	badType.str("x")
	badType.u64(300) // a file type over 8 bits
	for name, c := range map[string]struct {
		reply func(l *link, id uint64, op byte, p []byte)
		call  func(f *FS) error
	}{
		"listdir count": {ok(lie), func(f *FS) error { _, err := f.ListDir("x"); return err }},
		"listdir type":  {ok(badType), func(f *FS) error { _, err := f.ListDir("x"); return err }},
		"stat mode":     {ok(badStat), func(f *FS) error { _, err := f.Stat("x"); return err }},
		"stat flags":    {ok(badFlags), func(f *FS) error { _, err := f.Stat("x"); return err }},
		"readfile":      {ok(&enc{b: []byte{1}}), func(f *FS) error { _, err := f.ReadFile("x"); return err }},
		"readlink":      {ok(&enc{b: []byte{1}}), func(f *FS) error { _, err := f.ReadLink("x"); return err }},
		"mkdir extra":   {ok(&enc{b: []byte{1}}), func(f *FS) error { return f.MkDir("x", 0) }},
		"open":          {ok(&enc{b: []byte{1}}), func(f *FS) error { _, err := f.OpenFile("x"); return err }},
		"error form": {func(l *link, id uint64, op byte, p []byte) {
			e := &enc{}
			e.u64(0)
			e.u64(0)
			e.u64(9) // no such form
			for range 4 {
				e.str("")
			}
			_ = l.send(id, statusErr, e.b)
		}, func(f *FS) error { _, err := f.Stat("x"); return err }},
		"error cut": {func(l *link, id uint64, op byte, p []byte) { _ = l.send(id, statusErr, []byte{1}) }, func(f *FS) error { return f.MkDir("x", 0) }},
	} {
		t.Run(name, func(t *testing.T) {
			f := fakeServer(t, okHello(), c.reply)
			if err := c.call(f); err == nil || !strings.Contains(err.Error(), "malformed") {
				t.Fatalf("%v", err)
			}
		})
	}
	// A label that does not decode is no label.
	f := fakeServer(t, okHello(), ok(&enc{b: []byte{1}}))
	if f.Label() != "" {
		t.Error("label")
	}
}

// Replies that end the connection: an unknown status, a reply to a call never
// made. Every call waiting gets the failure, and so does every later one.
func TestClientConnectionFailures(t *testing.T) {
	for name, reply := range map[string]func(l *link, id uint64, op byte, p []byte){
		"unknown status": func(l *link, id uint64, op byte, p []byte) { _ = l.send(id, 7, nil) },
		"unasked reply":  func(l *link, id uint64, op byte, p []byte) { _ = l.send(id+100, statusOK, nil) },
		"hangs up":       func(l *link, id uint64, op byte, p []byte) { _ = l.rw.(net.Conn).Close() },
	} {
		t.Run(name, func(t *testing.T) {
			f := fakeServer(t, okHello(), reply)
			if _, err := f.Stat("x"); err == nil {
				t.Fatal("a call answered")
			}
			if _, err := f.Stat("x"); err == nil {
				t.Fatal("a later call answered")
			}
		})
	}
}

// File replies a client must not believe.
func TestClientFileReplies(t *testing.T) {
	var size, extra atomic.Bool
	f := fakeServer(t, okHello(), func(l *link, id uint64, op byte, p []byte) {
		e := &enc{}
		switch op {
		case opOpenFile:
			e.u64(1)
			e.u64(openWritable)
			e.i64(10)
		case opFileReadAt:
			e.bytes([]byte("ab"))
			if extra.Load() {
				e.u64(0)
				e.u64(9) // trailing bytes
			} else {
				e.u64(0) // short, no EOF
			}
		case opFileWriteAt:
			e.u64(1)
			if extra.Load() {
				e.u64(0)
				e.u64(9)
			} else {
				e.u64(1) // an error follows, but it does not decode
				e.u64(1)
			}
		case opFileSize:
			if size.Load() {
				e.b = []byte{1}
			} else {
				_ = l.send(id, statusErr, func() []byte { o := &enc{}; encodeErr(o, errPlain); return o.b }())
				return
			}
		case opFileTruncate, opFileSync, opFileClose:
			e.b = []byte{1}
		}
		_ = l.send(id, statusOK, e.b)
	})
	h, err := f.OpenFile("x")
	if err != nil {
		t.Fatal(err)
	}
	w := h.(filesystem.WritableFile)
	if h.Size() != 10 {
		t.Error("size falls back to the size at open on an error")
	}
	size.Store(true)
	if h.Size() != 10 {
		t.Error("size falls back on a malformed answer")
	}
	if _, err := h.ReadAt(make([]byte, 4), 0); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("short read: %v", err)
	}
	if _, err := w.WriteAt([]byte("ab"), 0); err == nil || !strings.Contains(err.Error(), "malformed") {
		t.Errorf("write error that does not decode: %v", err)
	}
	extra.Store(true)
	if _, err := h.ReadAt(make([]byte, 4), 0); err == nil {
		t.Error("read with trailing bytes")
	}
	if _, err := w.WriteAt([]byte("ab"), 0); err == nil {
		t.Error("write with trailing bytes")
	}
	if w.Truncate(1) == nil || w.Sync() == nil {
		t.Error("truncate or sync with trailing bytes")
	}
	if err := h.Close(); err == nil {
		t.Error("close with trailing bytes")
	}
	// The connection gone: file calls fail.
	_ = f.Close()
	h2 := &writableFile{&file{fs: f, id: 2}}
	if _, err := h2.WriteAt([]byte("x"), 0); err == nil {
		t.Error("write after close")
	}
	if h2.Truncate(0) == nil || h2.Sync() == nil || h2.Close() == nil {
		t.Error("calls after close")
	}
}

// The wire's own bounds.
func TestWire(t *testing.T) {
	d := &dec{b: []byte{0, 0, 0, 5, 1}}
	if d.bytes(100); d.done() == nil {
		t.Error("a string longer than what is left")
	}
	d = &dec{b: []byte{0, 0, 0, 2, 1, 2}}
	if d.bytes(1); d.done() == nil {
		t.Error("a string over its limit")
	}
	d = &dec{b: []byte{0, 0}}
	if d.bytes(1); d.done() == nil {
		t.Error("a length cut short")
	}
	e := &enc{}
	e.u64(10)
	d = &dec{b: e.b}
	if d.int(5); d.done() == nil {
		t.Error("an int over its limit")
	}
	d = &dec{}
	d.u64()
	d.u64() // a second failure keeps the first
	if !errors.Is(d.done(), errMalformed) {
		t.Error("done")
	}
	if (&Error{kinds: kindExist}).Is(fs.ErrNotExist) {
		t.Error("Is matched a kind it does not have")
	}
	l := &link{rw: &bytes.Buffer{}}
	if err := l.send(1, opStat, make([]byte, MaxFrame+1)); err == nil {
		t.Error("a frame over the limit sent")
	}
	if err := l.send(1, opStat, nil); err != nil {
		t.Fatal(err)
	}
	l.rw.(*bytes.Buffer).Truncate(10)
	if _, _, _, err := l.recv(); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("a frame cut short: %v", err)
	}
}

// The last gaps, each a path a peer can take.
func TestEdges(t *testing.T) {
	// A read filled exactly by the last piece, which also says EOF: no
	// error, all of it.
	f := fakeServer(t, okHello(), func(l *link, id uint64, op byte, p []byte) {
		e := &enc{}
		switch op {
		case opOpenFile:
			e.u64(1)
			e.u64(0)
			e.i64(3)
		case opFileReadAt:
			e.bytes([]byte("abc"))
			e.u64(1)
		case opStat: // a stat with a byte too many
			for range 4 {
				e.u64(0)
			}
			e.i64(0)
			e.b = append(e.b, 9)
		}
		_ = l.send(id, statusOK, e.b)
	})
	h, _ := f.OpenFile("x")
	if n, err := h.ReadAt(make([]byte, 3), 0); n != 3 || err != nil {
		t.Errorf("an exact read at the end: %d, %v", n, err)
	}
	if _, err := f.Stat("x"); err == nil {
		t.Error("a stat with trailing bytes")
	}
	// A header with none of its payload after it.
	b := &bytes.Buffer{}
	b.Write(frame(1, opStat, []byte("12345"))[:headerSize])
	if _, _, _, err := (&link{rw: b}).recv(); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("a payload that never came: %v", err)
	}
	// A server whose hello cannot be written.
	a, c := net.Pipe()
	_ = a.Close()
	if err := Serve(c, plainFS{}); err == nil {
		t.Error("Serve on a closed connection")
	}
}
