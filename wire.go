// SPDX-License-Identifier: BSD-3-Clause

package remote

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)

// The wire format, the same both ways: a frame is
//
//	u32 length of what follows | u64 request id | u8 code | payload
//
// where code is an op for a request and a status for a reply. A payload is a
// sequence of fields: u64 numbers, and byte strings as u32 length + bytes.
// Every length is checked against what is left of the frame, and a frame
// against MaxFrame, before anything is allocated: whatever sends it, the
// reader allocates no more than one frame's worth.

// MaxFrame bounds one frame: a ReadFile or WriteFile carries the whole file,
// so this is also the largest file those two move. Larger ones go through
// OpenFile, whose reads and writes are split into pieces of maxChunk.
const MaxFrame = 32 << 20

// maxChunk bounds one proxied ReadAt or WriteAt.
const maxChunk = 1 << 20

// maxPath bounds a path or a name.
const maxPath = 4096

const headerSize = 4 + 8 + 1

// errMalformed is a frame that does not decode.
var errMalformed = errors.New("remote: malformed frame")

// enc builds a frame's payload.
type enc struct{ b []byte }

func (e *enc) u64(v uint64) { e.b = binary.BigEndian.AppendUint64(e.b, v) }

func (e *enc) bytes(v []byte) {
	e.b = binary.BigEndian.AppendUint32(e.b, uint32(len(v)))
	e.b = append(e.b, v...)
}

func (e *enc) str(v string) { e.bytes([]byte(v)) }

func (e *enc) i64(v int64) { e.u64(uint64(v)) }

// frame is the whole frame: header and payload.
func frame(id uint64, code byte, payload []byte) []byte {
	b := make([]byte, headerSize, headerSize+len(payload))
	binary.BigEndian.PutUint32(b, uint32(8+1+len(payload)))
	binary.BigEndian.PutUint64(b[4:], id)
	b[12] = code
	return append(b, payload...)
}

// dec reads a payload, remembering the first error.
type dec struct {
	b   []byte
	err error
}

func (d *dec) fail() {
	if d.err == nil {
		d.err = errMalformed
	}
	d.b = nil
}

func (d *dec) u64() uint64 {
	if len(d.b) < 8 {
		d.fail()
		return 0
	}
	v := binary.BigEndian.Uint64(d.b)
	d.b = d.b[8:]
	return v
}

func (d *dec) i64() int64 { return int64(d.u64()) }

func (d *dec) bytes(limit int) []byte {
	if len(d.b) < 4 {
		d.fail()
		return nil
	}
	n := binary.BigEndian.Uint32(d.b)
	if uint64(n) > uint64(len(d.b)-4) || uint64(n) > uint64(limit) {
		d.fail()
		return nil
	}
	v := d.b[4 : 4+n]
	d.b = d.b[4+n:]
	return v
}

func (d *dec) str() string { return string(d.bytes(maxPath)) }

// int is a u64 that must fit an int, and be at most limit.
func (d *dec) int(limit int) int {
	v := d.u64()
	if v > uint64(limit) || v > math.MaxInt {
		d.fail()
		return 0
	}
	return int(v)
}

// done is the decoded payload's error, and an error too if anything is left.
func (d *dec) done() error {
	if d.err == nil && len(d.b) != 0 {
		return fmt.Errorf("%w: %d bytes left over", errMalformed, len(d.b))
	}
	return d.err
}
