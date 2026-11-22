// SPDX-License-Identifier: BSD-3-Clause

package remote

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"sync"

	"github.com/go-filesystems/inflight"
)

// link carries frames over one connection, in both directions: writes are
// serialised, reads are the caller's (one reader at a time).
type link struct {
	rw  io.ReadWriter
	wmu sync.Mutex
	hdr [headerSize]byte

	// budget, when set (the server's), bounds the request payloads held at
	// once; see recv.
	budget *inflight.Budget
}

// send writes one frame. A payload over MaxFrame is refused before anything
// is written, so a frame never leaves half-sent for being too large.
func (l *link) send(id uint64, code byte, payload []byte) error {
	if len(payload) > MaxFrame {
		return fmt.Errorf("remote: a frame of %d bytes is over the limit of %d", len(payload), MaxFrame)
	}
	b := frame(id, code, payload)
	l.wmu.Lock()
	defer l.wmu.Unlock()
	_, err := l.rw.Write(b)
	return err
}

// recv reads one frame. The length is checked before the payload is
// allocated: a peer that announces 4 GiB is refused, not believed.
func (l *link) recv() (id uint64, code byte, payload []byte, err error) {
	if _, err := io.ReadFull(l.rw, l.hdr[:]); err != nil {
		return 0, 0, nil, err
	}
	n := binary.BigEndian.Uint32(l.hdr[:4])
	if n < 9 || n-9 > MaxFrame {
		return 0, 0, nil, fmt.Errorf("%w: a frame of %d bytes", errMalformed, n)
	}
	id = binary.BigEndian.Uint64(l.hdr[4:12])
	code = l.hdr[12]
	// A server holds the payloads of every request in flight at once: the
	// bytes are taken from its budget BEFORE the payload is allocated, so the
	// reader waits (and the peer with it) instead of retaining 64 frames. The
	// caller gives them back when it has answered.
	if l.budget != nil {
		if err := l.budget.Acquire(context.Background(), int64(n-9)); err != nil {
			return 0, 0, nil, err
		}
	}
	payload = make([]byte, n-9)
	if _, err := io.ReadFull(l.rw, payload); err != nil {
		if l.budget != nil {
			l.budget.Release(int64(n - 9))
		}
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return 0, 0, nil, err
	}
	return id, code, payload, nil
}
