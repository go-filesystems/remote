// SPDX-License-Identifier: BSD-3-Clause

package remote

import (
	"errors"
	"io/fs"
	"testing"
	"time"

	filesystem "github.com/go-filesystems/interface"
)

// timesFS is plainFS plus the times capability.
type timesFS struct {
	plainFS
	ft  filesystem.FileTimes
	err error
}

func (f timesFS) Times(string) (filesystem.FileTimes, error) { return f.ft, f.err }

// What a driver reports comes out the other end as it went in: the instant,
// the resolution, and above all "not recorded".
func TestTimesCrossTheWireExactly(t *testing.T) {
	want := filesystem.FileTimes{
		Atime: filesystem.NewTimestamp(time.Date(1904, 1, 1, 0, 0, 5, 0, time.UTC), time.Second),         // before 1970, HFS+'s epoch
		Mtime: filesystem.NewTimestamp(time.Unix(1_700_000_000, 123_456_789), time.Nanosecond),           // sub-second
		Ctime: filesystem.Timestamp{},                                                                    // not recorded
		Btime: filesystem.NewTimestamp(time.Date(2446, 5, 1, 0, 0, 0, 0, time.UTC), 10*time.Millisecond), // past 2262: no UnixNano
	}
	f := pair(t, timesFS{ft: want})
	got, err := f.Times("/x")
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("got %+v\nwant %+v", got, want)
	}
	if got.Ctime.Recorded() {
		t.Fatal("not recorded became recorded")
	}
}

func TestTimesFromAHostDirectory(t *testing.T) {
	fsys, _ := openOSFS(t)
	if err := fsys.WriteFile("/f", []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	m := time.Date(2010, 11, 12, 13, 14, 15, 0, time.UTC)
	if err := fsys.Chtimes("/f", m, m); err != nil {
		t.Fatal(err)
	}
	local, err := fsys.Times("/f")
	if err != nil {
		t.Fatal(err)
	}
	remote, err := pair(t, fsys).Times("/f")
	if err != nil {
		t.Fatal(err)
	}
	if remote != local {
		t.Fatalf("remote %+v, local %+v", remote, local)
	}
}

func TestTimesErrorsAndMissingCapability(t *testing.T) {
	// The server's error comes back as itself.
	f := pair(t, timesFS{err: fs.ErrNotExist})
	if _, err := f.Times("/x"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("err = %v", err)
	}
	// A server whose filesystem has no times answers ErrUnsupported, which a
	// protocol server reads as "no times".
	if _, err := pair(t, plainFS{}).Times("/x"); !errors.Is(err, errors.ErrUnsupported) {
		t.Fatalf("err = %v", err)
	}
}

func TestClientRefusesBadTimesReplies(t *testing.T) {
	good := func(e *enc) {
		for range 4 {
			e.u64(0)
			e.i64(0)
			e.u64(0)
			e.i64(0)
		}
	}
	for name, build := range map[string]func(*enc){
		"a recorded flag of 2": func(e *enc) { e.u64(2); e.i64(0); e.u64(0); e.i64(0) },
		"a second of nanoseconds": func(e *enc) {
			e.u64(1)
			e.i64(5)
			e.u64(1_000_000_000)
			e.i64(1)
		},
		"truncated":      func(e *enc) { e.u64(0) },
		"trailing bytes": func(e *enc) { good(e); e.u64(0) },
	} {
		t.Run(name, func(t *testing.T) {
			e := &enc{}
			build(e)
			f := fakeServer(t, okHello(), func(l *link, id uint64, op byte, p []byte) { _ = l.send(id, statusOK, e.b) })
			if _, err := f.Times("/x"); err == nil {
				t.Fatal("the client believed it")
			}
		})
	}
	// And the good one is believed, so the cases above fail for their own reason.
	e := &enc{}
	good(e)
	f := fakeServer(t, okHello(), func(l *link, id uint64, op byte, p []byte) { _ = l.send(id, statusOK, e.b) })
	if ft, err := f.Times("/x"); err != nil || ft != (filesystem.FileTimes{}) {
		t.Fatalf("%+v, %v", ft, err)
	}
}
