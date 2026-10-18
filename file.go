// SPDX-License-Identifier: BSD-3-Clause

package remote

import (
	"io"
	"io/fs"
	"sync/atomic"

	filesystem "github.com/go-filesystems/interface"
)

// file is a file the server holds open; every operation is a call, and a
// read or a write larger than maxChunk is several.
type file struct {
	fs     *FS
	id     uint64
	size   int64 // at open, the answer Size gives until a write changes it
	closed atomic.Bool
}

// writableFile is a file the server opened writable.
type writableFile struct{ *file }

var (
	_ filesystem.File         = (*file)(nil)
	_ filesystem.WritableFile = (*writableFile)(nil)
)

func (h *file) call(op byte, e *enc) (*dec, error) {
	if h.closed.Load() {
		return nil, fs.ErrClosed
	}
	return h.fs.call(op, e.b)
}

func (h *file) enc() *enc {
	e := &enc{}
	e.u64(h.id)
	return e
}

// ReadAt follows io.ReaderAt to the letter: n < len(p) only with an error,
// io.EOF when the file ends first. The server's answers are pieces of at
// most maxChunk; this asks until p is full or the file ends.
func (h *file) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, &fs.PathError{Op: "read", Err: fs.ErrInvalid}
	}
	n := 0
	for n < len(p) {
		want := min(len(p)-n, maxChunk)
		e := h.enc()
		e.i64(off + int64(n))
		e.u64(uint64(want))
		d, err := h.call(opFileReadAt, e)
		if err != nil {
			return n, err
		}
		b := d.bytes(want)
		eof := d.u64()
		if err := d.done(); err != nil {
			return n, err
		}
		n += copy(p[n:], b)
		if eof != 0 {
			if n < len(p) {
				return n, io.EOF
			}
			break
		}
		if len(b) < want {
			// Short without EOF is not a reader's answer.
			return n, io.ErrUnexpectedEOF
		}
	}
	return n, nil
}

// Size asks the server: a write through another handle, or this one, may
// have changed it.
func (h *file) Size() int64 {
	d, err := h.call(opFileSize, h.enc())
	if err != nil {
		return h.size
	}
	s := d.i64()
	if d.done() != nil {
		return h.size
	}
	return s
}

// Close releases the server's file. A second Close is an error, as it is on
// an *os.File.
func (h *file) Close() error {
	if h.closed.Swap(true) {
		return fs.ErrClosed
	}
	d, err := h.fs.call(opFileClose, h.enc().b)
	if err != nil {
		return err
	}
	return d.done()
}

// WriteAt writes p at off, in pieces of at most maxChunk; n < len(p) only
// with an error.
func (h *writableFile) WriteAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, &fs.PathError{Op: "write", Err: fs.ErrInvalid}
	}
	n := 0
	for n < len(p) {
		piece := p[n:min(len(p), n+maxChunk)]
		e := h.enc()
		e.i64(off + int64(n))
		e.bytes(piece)
		d, err := h.call(opFileWriteAt, e)
		if err != nil {
			return n, err
		}
		k := d.int(len(piece))
		hasErr := d.u64()
		var werr error
		if hasErr != 0 {
			werr = decodeErr(d)
		} else if err := d.done(); err != nil {
			return n, err
		}
		n += k
		if werr != nil {
			return n, werr
		}
		if k < len(piece) {
			return n, io.ErrShortWrite
		}
	}
	return n, nil
}

func (h *writableFile) Truncate(size int64) error {
	e := h.enc()
	e.i64(size)
	d, err := h.call(opFileTruncate, e)
	if err != nil {
		return err
	}
	return d.done()
}

func (h *writableFile) Sync() error {
	d, err := h.call(opFileSync, h.enc())
	if err != nil {
		return err
	}
	return d.done()
}
