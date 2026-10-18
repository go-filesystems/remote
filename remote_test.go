// SPDX-License-Identifier: BSD-3-Clause

package remote

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"syscall"
	"testing"
	"time"

	filesystem "github.com/go-filesystems/interface"
	"github.com/go-filesystems/osfs"
)

// pair serves fsys on one end of a pipe and returns the filesystem at the
// other; Serve's return is checked at cleanup.
func pair(t *testing.T, fsys filesystem.Filesystem) *FS {
	t.Helper()
	a, b := net.Pipe()
	done := make(chan error, 1)
	go func() { done <- Serve(b, fsys) }()
	f, err := New(a)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = f.Close()
		if err := <-done; err != nil && !errors.Is(err, io.ErrClosedPipe) {
			t.Errorf("Serve: %v", err)
		}
	})
	return f
}

func openOSFS(t *testing.T, opts ...osfs.Option) (*osfs.FS, string) {
	t.Helper()
	dir := t.TempDir()
	fsys, err := osfs.Open(dir, opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fsys.Close() })
	return fsys, dir
}

var sentinels = []error{fs.ErrNotExist, fs.ErrExist, fs.ErrPermission, fs.ErrInvalid, fs.ErrClosed, errors.ErrUnsupported}

// sameErr reports how two errors differ in what a caller can see of them:
// nil-ness, errors.Is for every sentinel, the errno, the text.
func sameErr(local, remote error) string {
	if (local == nil) != (remote == nil) {
		return fmt.Sprintf("local %v, remote %v", local, remote)
	}
	if local == nil {
		return ""
	}
	for _, s := range sentinels {
		if errors.Is(local, s) != errors.Is(remote, s) {
			return fmt.Sprintf("errors.Is(%v): local %v (%v), remote %v (%v)", s, errors.Is(local, s), local, errors.Is(remote, s), remote)
		}
	}
	if fmt.Sprintf("%T", local) != fmt.Sprintf("%T", remote) {
		return fmt.Sprintf("type: local %T, remote %T", local, remote)
	}
	var le, re syscall.Errno
	if errors.As(local, &le) != errors.As(remote, &re) || le != re {
		return fmt.Sprintf("errno: local %v, remote %v", le, re)
	}
	if local.Error() != remote.Error() {
		return fmt.Sprintf("text: local %q, remote %q", local, remote)
	}
	return ""
}

// The same operations on osfs directly and through remote give the same
// results and the same errors, in everything a caller can test.
func TestSameAsLocal(t *testing.T) {
	loc, ldir := openOSFS(t)
	remFS, rdir := openOSFS(t)
	rem := pair(t, remFS)
	if !rem.Can(CanOpen) || !rem.Can(CanSymlink) || !rem.Can(CanLink) || !rem.Can(CanTruncate) || !rem.Can(CanMetadata) {
		t.Fatalf("capabilities of osfs not announced: %b", rem.caps)
	}
	type step struct {
		name string
		run  func(filesystem.Filesystem) (any, error)
	}
	both := func(dirOf func(filesystem.Filesystem) string) []step {
		return []step{
			{"write", func(f filesystem.Filesystem) (any, error) { return nil, f.WriteFile("a.txt", []byte("hello"), 0o640) }},
			{"read", func(f filesystem.Filesystem) (any, error) { return f.ReadFile("a.txt") }},
			{"read missing", func(f filesystem.Filesystem) (any, error) { return f.ReadFile("nope") }},
			{"mkdir", func(f filesystem.Filesystem) (any, error) { return nil, f.MkDir("d", 0o750) }},
			{"mkdir again", func(f filesystem.Filesystem) (any, error) { return nil, f.MkDir("d", 0o750) }},
			{"stat", func(f filesystem.Filesystem) (any, error) {
				st, err := f.Stat("a.txt")
				if err != nil {
					return nil, err
				}
				return [2]uint64{uint64(st.Mode()), st.Size()}, nil
			}},
			{"stat missing", func(f filesystem.Filesystem) (any, error) { return f.Stat("missing/x") }},
			{"escape", func(f filesystem.Filesystem) (any, error) { return f.ReadFile("../../etc/passwd") }},
			{"symlink", func(f filesystem.Filesystem) (any, error) { return nil, f.(filesystem.Symlinker).Symlink("a.txt", "l") }},
			{"readlink", func(f filesystem.Filesystem) (any, error) { return f.ReadLink("l") }},
			{"readlink of a file", func(f filesystem.Filesystem) (any, error) { return f.ReadLink("a.txt") }},
			{"link", func(f filesystem.Filesystem) (any, error) { return nil, f.(filesystem.HardLinker).Link("a.txt", "h") }},
			{"truncate", func(f filesystem.Filesystem) (any, error) { return nil, f.(filesystem.Truncater).Truncate("h", 2) }},
			{"read truncated", func(f filesystem.Filesystem) (any, error) { return f.ReadFile("a.txt") }},
			{"chmod", func(f filesystem.Filesystem) (any, error) {
				return nil, f.(filesystem.MetadataSetter).Chmod("a.txt", 0o600)
			}},
			{"chown to self", func(f filesystem.Filesystem) (any, error) {
				return nil, f.(filesystem.MetadataSetter).Chown("a.txt", uint32(os.Getuid()), uint32(os.Getgid()))
			}},
			{"chtimes", func(f filesystem.Filesystem) (any, error) {
				when := time.Unix(1700000000, 123)
				return nil, f.(filesystem.MetadataSetter).Chtimes("a.txt", when, when)
			}},
			{"mtime on disk", func(f filesystem.Filesystem) (any, error) {
				fi, err := os.Stat(filepath.Join(dirOf(f), "a.txt"))
				if err != nil {
					return nil, err
				}
				return fi.ModTime().UnixNano(), nil
			}},
			{"rename", func(f filesystem.Filesystem) (any, error) { return nil, f.Rename("a.txt", "d/b.txt") }},
			{"rename missing", func(f filesystem.Filesystem) (any, error) { return nil, f.Rename("a.txt", "c") }},
			{"list", func(f filesystem.Filesystem) (any, error) {
				es, err := f.ListDir("d")
				var names []string
				for _, e := range es {
					names = append(names, fmt.Sprint(e.Name(), e.FileType()))
				}
				return names, err
			}},
			{"delete dir not empty", func(f filesystem.Filesystem) (any, error) { return nil, f.DeleteDir("d") }},
			{"delete file", func(f filesystem.Filesystem) (any, error) { return nil, f.DeleteFile("d/b.txt") }},
			{"delete dir", func(f filesystem.Filesystem) (any, error) { return nil, f.DeleteDir("d") }},
			{"delete missing", func(f filesystem.Filesystem) (any, error) { return nil, f.DeleteFile("d") }},
		}
	}
	dirOf := func(f filesystem.Filesystem) string {
		if f == filesystem.Filesystem(loc) {
			return ldir
		}
		return rdir
	}
	steps := both(dirOf)
	for _, s := range steps {
		lv, lerr := s.run(loc)
		rv, rerr := s.run(rem)
		if d := sameErr(lerr, rerr); d != "" {
			t.Errorf("%s: %s", s.name, d)
		}
		if fmt.Sprint(lv) != fmt.Sprint(rv) {
			t.Errorf("%s: local %v, remote %v", s.name, lv, rv)
		}
	}
}

// A file opened through remote: reads and writes larger than a piece, its
// size following the writes, truncate, sync, close -- the bytes on disk are
// the ones written.
func TestOpenFile(t *testing.T) {
	fsys, dir := openOSFS(t)
	rem := pair(t, fsys)
	big := bytes.Repeat([]byte("0123456789abcdef"), (3*maxChunk+maxChunk/2)/16)
	if err := os.WriteFile(filepath.Join(dir, "big"), big, 0o644); err != nil {
		t.Fatal(err)
	}
	f, err := rem.OpenFile("big")
	if err != nil {
		t.Fatal(err)
	}
	w, ok := f.(filesystem.WritableFile)
	if !ok {
		t.Fatal("a writable osfs file came back read-only")
	}
	if f.Size() != int64(len(big)) {
		t.Fatalf("size %d", f.Size())
	}
	got := make([]byte, len(big))
	if n, err := f.ReadAt(got, 0); n != len(big) || err != nil || !bytes.Equal(got, big) {
		t.Fatalf("read %d, %v", n, err)
	}
	// Past the end: what there is, and io.EOF.
	tail := make([]byte, 100)
	if n, err := f.ReadAt(tail, int64(len(big)-10)); n != 10 || err != io.EOF {
		t.Fatalf("tail: %d, %v", n, err)
	}
	if n, err := f.ReadAt(tail, int64(len(big)+5)); n != 0 || err != io.EOF {
		t.Fatalf("beyond: %d, %v", n, err)
	}
	if _, err := f.ReadAt(tail, -1); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("negative offset: %v", err)
	}
	more := bytes.Repeat([]byte("Z"), 2*maxChunk+7)
	if n, err := w.WriteAt(more, int64(len(big))); n != len(more) || err != nil {
		t.Fatalf("write: %d, %v", n, err)
	}
	if _, err := w.WriteAt(more, -1); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("negative write: %v", err)
	}
	if f.Size() != int64(len(big)+len(more)) {
		t.Fatalf("size after write %d", f.Size())
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := w.Truncate(5); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); !errors.Is(err, fs.ErrClosed) {
		t.Fatalf("second close: %v", err)
	}
	if _, err := f.ReadAt(tail, 0); !errors.Is(err, fs.ErrClosed) {
		t.Fatalf("read after close: %v", err)
	}
	if f.Size() != int64(len(big)) {
		t.Fatalf("size of a closed file is the size at open, %d", f.Size())
	}
	if b, _ := os.ReadFile(filepath.Join(dir, "big")); string(b) != "01234" {
		t.Fatalf("on disk: %q", b[:min(len(b), 20)])
	}
	if _, err := rem.OpenFile("nope"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("missing: %v", err)
	}
}

// A read-only osfs opens read-only files, and the type says so.
func TestReadOnly(t *testing.T) {
	fsys, dir := openOSFS(t, osfs.ReadOnly())
	_ = os.WriteFile(filepath.Join(dir, "a"), []byte("x"), 0o644)
	rem := pair(t, fsys)
	f, err := rem.OpenFile("a")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := f.(filesystem.WritableFile); ok {
		t.Fatal("a read-only file came back writable")
	}
	if err := rem.WriteFile("b", []byte("y"), 0o644); err == nil {
		t.Fatal("a read-only filesystem took a write")
	}
	_ = f.Close()
}

// Many calls at once, from many goroutines, each getting its own answer.
func TestConcurrent(t *testing.T) {
	fsys, dir := openOSFS(t)
	for i := range 50 {
		_ = os.WriteFile(filepath.Join(dir, fmt.Sprint(i)), []byte(fmt.Sprint(i)), 0o644)
	}
	rem := pair(t, fsys)
	var wg sync.WaitGroup
	errs := make(chan error, 500)
	for i := range 500 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			name := fmt.Sprint(i % 50)
			b, err := rem.ReadFile(name)
			if err == nil && string(b) != name {
				err = fmt.Errorf("%s read %q", name, b)
			}
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
}

// Over a real unix socketpair, the way a parent hands one end to a child.
func TestSocketpair(t *testing.T) {
	if runtime.GOOS == "windows" || runtime.GOOS == "plan9" || runtime.GOOS == "js" || runtime.GOOS == "wasip1" {
		t.Skip("no socketpair here")
	}
	fsys, dir := openOSFS(t)
	_ = os.WriteFile(filepath.Join(dir, "a"), []byte("over a socket"), 0o644)
	a, b := socketpair(t)
	done := make(chan error, 1)
	go func() { done <- Serve(b, fsys) }()
	rem, err := New(a)
	if err != nil {
		t.Fatal(err)
	}
	got, err := rem.ReadFile("a")
	if err != nil || string(got) != "over a socket" {
		t.Fatalf("%q %v", got, err)
	}
	_ = rem.Close()
	if err := <-done; err != nil {
		t.Fatalf("Serve after the client closed: %v", err)
	}
}
