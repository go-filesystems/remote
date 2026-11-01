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

// maxInFlight bounds the requests a server works on at once; the next one
// waits for a slot rather than starting a goroutine of its own.
const maxInFlight = 64

// maxOpen bounds the files one connection may hold open.
const maxOpen = 4096

// Serve answers the requests that arrive on conn against fsys, until conn
// is closed. It sends the hello first, then serves up to maxInFlight requests
// at once. It never closes fsys -- that is its owner's -- but it closes every
// file its peer opened and did not close, and conn.
//
// The peer is not trusted: a frame that does not decode, an unknown op, a
// reply id the server never sent, or a handle that is not open ends the
// connection or is refused, and nothing a frame says is allocated before it
// is checked against the bounds above.
func Serve(conn io.ReadWriteCloser, fsys filesystem.Filesystem) error {
	s := &server{l: &link{rw: conn}, fsys: fsys, files: map[uint64]filesystem.File{}}
	defer conn.Close()
	defer s.closeAll()
	if err := s.l.send(0, opHello, s.hello()); err != nil {
		return err
	}
	slots := make(chan struct{}, maxInFlight)
	var wg sync.WaitGroup
	defer wg.Wait()
	for {
		id, op, payload, err := s.l.recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if op <= opHello || op >= opLast {
			return fmt.Errorf("%w: op %d", errMalformed, op)
		}
		slots <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-slots }()
			status, out := s.handle(op, payload)
			// A reply that cannot be written ends nothing here: the read
			// loop sees the broken connection and returns.
			_ = s.l.send(id, status, out)
		}()
	}
}

type server struct {
	l    *link
	fsys filesystem.Filesystem

	mu     sync.Mutex
	files  map[uint64]filesystem.File
	nextID uint64
}

// hello announces what fsys can do beyond Filesystem.
func (s *server) hello() []byte {
	var caps uint64
	if _, ok := s.fsys.(filesystem.LabelReader); ok {
		caps |= capLabel
	}
	if _, ok := s.fsys.(filesystem.Symlinker); ok {
		caps |= capSymlink
	}
	if _, ok := s.fsys.(filesystem.HardLinker); ok {
		caps |= capLink
	}
	if _, ok := s.fsys.(filesystem.Truncater); ok {
		caps |= capTruncate
	}
	if _, ok := s.fsys.(filesystem.MetadataSetter); ok {
		caps |= capMetadata
	}
	if _, ok := s.fsys.(filesystem.Opener); ok {
		caps |= capOpen
	}
	var e enc
	e.u64(caps)
	return e.b
}

func (s *server) closeAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, f := range s.files {
		_ = f.Close()
		delete(s.files, id)
	}
}

// handle answers one request.
func (s *server) handle(op byte, payload []byte) (byte, []byte) {
	d := &dec{b: payload}
	var e enc
	err := s.dispatch(op, d, &e)
	if err != nil {
		var out enc
		encodeErr(&out, err)
		return statusErr, out.b
	}
	return statusOK, e.b
}

// unsupported is the error for an op fsys has no interface for.
func unsupported(op string) error {
	return &fs.PathError{Op: op, Err: errors.ErrUnsupported}
}

func (s *server) dispatch(op byte, d *dec, e *enc) error {
	switch op {
	case opReadFile:
		p := d.str()
		if err := d.done(); err != nil {
			return err
		}
		b, err := s.fsys.ReadFile(p)
		if err != nil {
			return err
		}
		if len(b) > MaxFrame-8 {
			return &fs.PathError{Op: "read", Path: p, Err: fmt.Errorf("remote: %d bytes is over the frame limit; open the file instead", len(b))}
		}
		e.bytes(b)
	case opListDir:
		p := d.str()
		if err := d.done(); err != nil {
			return err
		}
		es, err := s.fsys.ListDir(p)
		if err != nil {
			return err
		}
		e.u64(uint64(len(es)))
		for _, de := range es {
			e.u64(de.Inode())
			e.str(de.Name())
			e.u64(uint64(de.FileType()))
		}
	case opStat:
		p := d.str()
		if err := d.done(); err != nil {
			return err
		}
		st, err := s.fsys.Stat(p)
		if err != nil {
			return err
		}
		encodeStat(e, st)
	case opWriteFile:
		p, data, perm := d.str(), d.bytes(MaxFrame), d.u64()
		if err := d.done(); err != nil {
			return err
		}
		return s.fsys.WriteFile(p, data, os.FileMode(perm))
	case opReadLink:
		p := d.str()
		if err := d.done(); err != nil {
			return err
		}
		t, err := s.fsys.ReadLink(p)
		if err != nil {
			return err
		}
		e.str(t)
	case opMkDir:
		p, perm := d.str(), d.u64()
		if err := d.done(); err != nil {
			return err
		}
		return s.fsys.MkDir(p, os.FileMode(perm))
	case opDeleteFile, opDeleteDir:
		p := d.str()
		if err := d.done(); err != nil {
			return err
		}
		if op == opDeleteFile {
			return s.fsys.DeleteFile(p)
		}
		return s.fsys.DeleteDir(p)
	case opRename:
		a, b := d.str(), d.str()
		if err := d.done(); err != nil {
			return err
		}
		return s.fsys.Rename(a, b)
	case opLabel:
		if err := d.done(); err != nil {
			return err
		}
		lr, ok := s.fsys.(filesystem.LabelReader)
		if !ok {
			return unsupported("label")
		}
		e.str(lr.Label())
	case opSymlink:
		target, link := d.str(), d.str()
		if err := d.done(); err != nil {
			return err
		}
		sl, ok := s.fsys.(filesystem.Symlinker)
		if !ok {
			return unsupported("symlink")
		}
		return sl.Symlink(target, link)
	case opLink:
		a, b := d.str(), d.str()
		if err := d.done(); err != nil {
			return err
		}
		hl, ok := s.fsys.(filesystem.HardLinker)
		if !ok {
			return unsupported("link")
		}
		return hl.Link(a, b)
	case opTruncate:
		p, size := d.str(), d.i64()
		if err := d.done(); err != nil {
			return err
		}
		t, ok := s.fsys.(filesystem.Truncater)
		if !ok {
			return unsupported("truncate")
		}
		return t.Truncate(p, size)
	case opChmod, opChown, opChtimes:
		return s.metadata(op, d)
	case opOpenFile:
		p := d.str()
		if err := d.done(); err != nil {
			return err
		}
		return s.open(p, e)
	default: // the file ops; Serve has refused every other code
		return s.fileOp(op, d, e)
	}
	return nil
}

func (s *server) metadata(op byte, d *dec) error {
	p := d.str()
	var perm, uid, gid uint64
	var atime, mtime int64
	switch op {
	case opChmod:
		perm = d.u64()
	case opChown:
		uid, gid = d.u64(), d.u64()
	default:
		atime, mtime = d.i64(), d.i64()
	}
	if err := d.done(); err != nil {
		return err
	}
	m, ok := s.fsys.(filesystem.MetadataSetter)
	if !ok {
		return unsupported("metadata")
	}
	switch op {
	case opChmod:
		return m.Chmod(p, os.FileMode(perm))
	case opChown:
		if uid > 1<<32-1 || gid > 1<<32-1 {
			return errMalformed
		}
		return m.Chown(p, uint32(uid), uint32(gid))
	}
	return m.Chtimes(p, time.Unix(0, atime), time.Unix(0, mtime))
}

func (s *server) open(p string, e *enc) error {
	o, ok := s.fsys.(filesystem.Opener)
	if !ok {
		return unsupported("open")
	}
	f, err := o.OpenFile(p)
	if err != nil {
		return err
	}
	s.mu.Lock()
	if len(s.files) >= maxOpen {
		s.mu.Unlock()
		_ = f.Close()
		return &fs.PathError{Op: "open", Path: p, Err: fmt.Errorf("remote: %d files open on this connection already", maxOpen)}
	}
	s.nextID++
	id := s.nextID
	s.files[id] = f
	s.mu.Unlock()
	var flags uint64
	if _, ok := f.(filesystem.WritableFile); ok {
		flags |= openWritable
	}
	e.u64(id)
	e.u64(flags)
	e.i64(f.Size())
	return nil
}

func (s *server) file(id uint64) (filesystem.File, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	f, ok := s.files[id]
	if !ok {
		return nil, fs.ErrClosed
	}
	return f, nil
}

func (s *server) fileOp(op byte, d *dec, e *enc) error {
	id := d.u64()
	var off, n int64
	var data []byte
	switch op {
	case opFileReadAt:
		off, n = d.i64(), int64(d.int(maxChunk))
	case opFileWriteAt:
		off, data = d.i64(), d.bytes(maxChunk)
	case opFileTruncate:
		n = d.i64()
	}
	if err := d.done(); err != nil {
		return err
	}
	if op == opFileClose {
		s.mu.Lock()
		f, ok := s.files[id]
		delete(s.files, id)
		s.mu.Unlock()
		if !ok {
			return fs.ErrClosed
		}
		return f.Close()
	}
	f, err := s.file(id)
	if err != nil {
		return err
	}
	switch op {
	case opFileReadAt:
		if off < 0 {
			return errMalformed
		}
		buf := make([]byte, n)
		k, err := f.ReadAt(buf, off)
		eof := uint64(0)
		if errors.Is(err, io.EOF) {
			eof, err = 1, nil
		}
		if err != nil {
			return err
		}
		e.bytes(buf[:k])
		e.u64(eof)
		return nil
	case opFileSize:
		e.i64(f.Size())
		return nil
	}
	w, ok := f.(filesystem.WritableFile)
	if !ok {
		return fs.ErrPermission
	}
	switch op {
	case opFileWriteAt:
		if off < 0 {
			return errMalformed
		}
		// The count goes back with the error, if there is one: a short
		// write is the one case where both mean something.
		k, err := w.WriteAt(data, off)
		e.u64(uint64(k))
		if err != nil {
			e.u64(1)
			encodeErr(e, err)
			return nil
		}
		e.u64(0)
		return nil
	case opFileTruncate:
		// Refused here, as a negative offset is for READ and WRITE: a peer
		// that sends one is not a client this package wrote, and a driver
		// handed a negative size can panic slicing with it (an in-memory one
		// does).
		if n < 0 {
			return errMalformed
		}
		return w.Truncate(n)
	}
	return w.Sync()
}
