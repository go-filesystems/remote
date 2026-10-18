// SPDX-License-Identifier: BSD-3-Clause

package remote

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"sync"
	"time"

	filesystem "github.com/go-filesystems/interface"
)

// FS is a filesystem served by Serve at the other end of a connection. It
// implements filesystem.Filesystem and every optional interface a protocol
// server looks for -- Opener, Symlinker, HardLinker, Truncater,
// MetadataSetter, LabelReader. What the server's filesystem does not have
// answers errors.ErrUnsupported (Label: ""), so a type assertion succeeds
// where the server's would have failed; Can says what is really there.
//
// Calls may be made from any number of goroutines; each waits for its own
// reply.
type FS struct {
	l    *link
	conn io.Closer
	caps uint64

	mu      sync.Mutex
	pending map[uint64]chan reply
	nextID  uint64
	err     error // set once the connection has failed; every call returns it
}

type reply struct {
	status  byte
	payload []byte
}

var (
	_ filesystem.Filesystem     = (*FS)(nil)
	_ filesystem.Opener         = (*FS)(nil)
	_ filesystem.Symlinker      = (*FS)(nil)
	_ filesystem.HardLinker     = (*FS)(nil)
	_ filesystem.Truncater      = (*FS)(nil)
	_ filesystem.MetadataSetter = (*FS)(nil)
	_ filesystem.LabelReader    = (*FS)(nil)
)

// New is the filesystem served at the other end of conn. It reads the
// server's hello before returning, so a connection that is not a remote
// filesystem fails here rather than at the first call.
func New(conn io.ReadWriteCloser) (*FS, error) {
	l := &link{rw: conn}
	id, code, payload, err := l.recv()
	if err == nil && (id != 0 || code != opHello) {
		err = fmt.Errorf("%w: no hello", errMalformed)
	}
	var caps uint64
	if err == nil {
		d := &dec{b: payload}
		caps = d.u64()
		err = d.done()
	}
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("remote: %w", err)
	}
	f := &FS{l: l, conn: conn, caps: caps, pending: map[uint64]chan reply{}}
	go f.read()
	return f, nil
}

// Capability is something a server's filesystem may or may not do.
type Capability uint64

const (
	CanLabel    = Capability(capLabel)
	CanSymlink  = Capability(capSymlink)
	CanLink     = Capability(capLink)
	CanTruncate = Capability(capTruncate)
	CanMetadata = Capability(capMetadata)
	CanOpen     = Capability(capOpen)
)

// Can says whether the server's filesystem has c.
func (f *FS) Can(c Capability) bool { return f.caps&uint64(c) != 0 }

// read delivers replies to the calls waiting for them, until the connection
// fails; then every waiting call, and every later one, gets the failure.
func (f *FS) read() {
	for {
		id, status, payload, err := f.l.recv()
		if err == nil && status != statusOK && status != statusErr {
			err = fmt.Errorf("%w: status %d", errMalformed, status)
		}
		f.mu.Lock()
		ch, ok := f.pending[id]
		if err == nil && !ok {
			err = fmt.Errorf("%w: a reply to %d, which was not asked", errMalformed, id)
		}
		if err != nil {
			f.fail(err)
			f.mu.Unlock()
			_ = f.conn.Close()
			return
		}
		delete(f.pending, id)
		f.mu.Unlock()
		ch <- reply{status, payload}
	}
}

// fail ends every waiting call with err; f.mu is held.
func (f *FS) fail(err error) {
	if f.err == nil {
		if errors.Is(err, io.EOF) || errors.Is(err, os.ErrClosed) {
			err = fs.ErrClosed
		}
		f.err = fmt.Errorf("remote: %w", err)
	}
	for id, ch := range f.pending {
		delete(f.pending, id)
		close(ch)
	}
}

// Close closes the connection. The server's filesystem stays open -- it is
// its owner's -- and the files opened through f are closed by the server.
func (f *FS) Close() error {
	f.mu.Lock()
	if f.err == nil {
		f.err = fmt.Errorf("remote: %w", fs.ErrClosed)
	}
	f.mu.Unlock()
	return f.conn.Close()
}

// call sends one request and waits for its reply: the reply's payload, or
// the error it carries.
func (f *FS) call(op byte, payload []byte) (*dec, error) {
	f.mu.Lock()
	if f.err != nil {
		err := f.err
		f.mu.Unlock()
		return nil, err
	}
	f.nextID++
	id := f.nextID
	ch := make(chan reply, 1)
	f.pending[id] = ch
	f.mu.Unlock()
	if err := f.l.send(id, op, payload); err != nil {
		f.mu.Lock()
		delete(f.pending, id)
		f.mu.Unlock()
		return nil, fmt.Errorf("remote: %w", err)
	}
	r, ok := <-ch
	if !ok {
		f.mu.Lock()
		err := f.err
		f.mu.Unlock()
		return nil, err
	}
	d := &dec{b: r.payload}
	if r.status == statusErr {
		return nil, decodeErr(d)
	}
	return d, nil
}

// pathCall is a call whose reply has nothing in it.
func (f *FS) pathCall(op byte, e *enc) error {
	d, err := f.call(op, e.b)
	if err != nil {
		return err
	}
	return d.done()
}

func pathEnc(paths ...string) *enc {
	e := &enc{}
	for _, p := range paths {
		e.str(p)
	}
	return e
}

func (f *FS) ReadFile(path string) ([]byte, error) {
	d, err := f.call(opReadFile, pathEnc(path).b)
	if err != nil {
		return nil, err
	}
	b := d.bytes(MaxFrame)
	if err := d.done(); err != nil {
		return nil, err
	}
	return append([]byte(nil), b...), nil
}

func (f *FS) ListDir(path string) ([]filesystem.DirEntry, error) {
	d, err := f.call(opListDir, pathEnc(path).b)
	if err != nil {
		return nil, err
	}
	// Each entry is at least 8+4+8 bytes: a count larger than that allows
	// is a lie, refused before allocating for it.
	n := d.int(len(d.b) / 20)
	out := make([]filesystem.DirEntry, 0, n)
	for range n {
		ino, name, typ := d.u64(), d.str(), d.u64()
		if typ > 0xff {
			d.fail()
		}
		out = append(out, filesystem.NewDirEntry(ino, name, uint8(typ)))
	}
	if err := d.done(); err != nil {
		return nil, err
	}
	return out, nil
}

func (f *FS) Stat(path string) (filesystem.Stat, error) {
	d, err := f.call(opStat, pathEnc(path).b)
	if err != nil {
		return nil, err
	}
	return decodeStat(d)
}

func (f *FS) WriteFile(path string, data []byte, perm os.FileMode) error {
	e := pathEnc(path)
	e.bytes(data)
	e.u64(uint64(perm))
	return f.pathCall(opWriteFile, e)
}

func (f *FS) ReadLink(path string) (string, error) {
	d, err := f.call(opReadLink, pathEnc(path).b)
	if err != nil {
		return "", err
	}
	t := d.str()
	return t, d.done()
}

func (f *FS) MkDir(path string, perm os.FileMode) error {
	e := pathEnc(path)
	e.u64(uint64(perm))
	return f.pathCall(opMkDir, e)
}

func (f *FS) DeleteFile(path string) error { return f.pathCall(opDeleteFile, pathEnc(path)) }

func (f *FS) DeleteDir(path string) error { return f.pathCall(opDeleteDir, pathEnc(path)) }

func (f *FS) Rename(oldPath, newPath string) error {
	return f.pathCall(opRename, pathEnc(oldPath, newPath))
}

// Label is the server's volume label, or "" when its filesystem has none to
// read (or the call fails: Label has no error to return).
func (f *FS) Label() string {
	d, err := f.call(opLabel, nil)
	if err != nil {
		return ""
	}
	l := d.str()
	if d.done() != nil {
		return ""
	}
	return l
}

func (f *FS) Symlink(target, linkPath string) error {
	return f.pathCall(opSymlink, pathEnc(target, linkPath))
}

func (f *FS) Link(oldPath, newPath string) error {
	return f.pathCall(opLink, pathEnc(oldPath, newPath))
}

func (f *FS) Truncate(path string, newSize int64) error {
	e := pathEnc(path)
	e.i64(newSize)
	return f.pathCall(opTruncate, e)
}

func (f *FS) Chmod(path string, perm os.FileMode) error {
	e := pathEnc(path)
	e.u64(uint64(perm))
	return f.pathCall(opChmod, e)
}

func (f *FS) Chown(path string, uid, gid uint32) error {
	e := pathEnc(path)
	e.u64(uint64(uid))
	e.u64(uint64(gid))
	return f.pathCall(opChown, e)
}

func (f *FS) Chtimes(path string, atime, mtime time.Time) error {
	e := pathEnc(path)
	e.i64(atime.UnixNano())
	e.i64(mtime.UnixNano())
	return f.pathCall(opChtimes, e)
}

// OpenFile opens path on the server, which holds it open until Close. The
// result is a filesystem.WritableFile exactly when the server's was.
func (f *FS) OpenFile(path string) (filesystem.File, error) {
	d, err := f.call(opOpenFile, pathEnc(path).b)
	if err != nil {
		return nil, err
	}
	id, flags, size := d.u64(), d.u64(), d.i64()
	if err := d.done(); err != nil {
		return nil, err
	}
	h := &file{fs: f, id: id, size: size}
	if flags&openWritable != 0 {
		return &writableFile{h}, nil
	}
	return h, nil
}
