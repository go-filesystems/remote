// SPDX-License-Identifier: BSD-3-Clause

package remote

import (
	"time"

	filesystem "github.com/go-filesystems/interface"
)

// A Stat crosses with what it carried: mode, size and inode always, and a
// modification time only if the server's had one, in the form it had it --
// the protocol servers ask for ModTime() int64 (webdav, sftp, nfs) or
// ModTime() time.Time (s3), and a client must answer exactly when, and as,
// the server's driver would have. A time that was not there is not invented.

func encodeStat(e *enc, st filesystem.Stat) {
	var flags uint64
	var mtime int64
	switch t := st.(type) {
	case interface{ ModTime() int64 }:
		flags, mtime = statUnixModTime, t.ModTime()
	case interface{ ModTime() time.Time }:
		flags, mtime = statTimeModTime, t.ModTime().UnixNano()
	}
	e.u64(uint64(st.Mode()))
	e.u64(st.Size())
	e.u64(st.Inode())
	e.u64(flags)
	e.i64(mtime)
}

func decodeStat(d *dec) (filesystem.Stat, error) {
	mode := d.u64()
	size, inode, flags, mtime := d.u64(), d.u64(), d.u64(), d.i64()
	if err := d.done(); err != nil {
		return nil, err
	}
	if mode > 0xffff {
		return nil, errMalformed
	}
	base := filesystem.NewStat(uint16(mode), size, inode)
	switch flags {
	case 0:
		return base, nil
	case statUnixModTime:
		return unixTimeStat{base, mtime}, nil
	case statTimeModTime:
		return timeStat{base, time.Unix(0, mtime)}, nil
	}
	return nil, errMalformed
}

// unixTimeStat is a Stat whose driver gave ModTime() int64, in seconds.
type unixTimeStat struct {
	filesystem.Stat
	mtime int64
}

func (s unixTimeStat) ModTime() int64 { return s.mtime }

// timeStat is a Stat whose driver gave ModTime() time.Time.
type timeStat struct {
	filesystem.Stat
	mtime time.Time
}

func (s timeStat) ModTime() time.Time { return s.mtime }
