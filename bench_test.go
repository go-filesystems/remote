// SPDX-License-Identifier: BSD-3-Clause

package remote

import (
	"os"
	"path/filepath"
	"testing"

	filesystem "github.com/go-filesystems/interface"
	"github.com/go-filesystems/osfs"
)

// What a read costs through remote, against the same read from osfs
// directly: 64 MiB in reads of 1 MiB, over a unix socketpair.
func benchRead(b *testing.B, through func(*testing.B, filesystem.Filesystem) filesystem.Filesystem) {
	dir := b.TempDir()
	const size = 64 << 20
	if err := os.WriteFile(filepath.Join(dir, "f"), make([]byte, size), 0o644); err != nil {
		b.Fatal(err)
	}
	fsys, err := osfs.Open(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer fsys.Close()
	f, err := through(b, fsys).(filesystem.Opener).OpenFile("f")
	if err != nil {
		b.Fatal(err)
	}
	defer f.Close()
	buf := make([]byte, 1<<20)
	b.SetBytes(size)
	b.ResetTimer()
	for range b.N {
		for off := int64(0); off < size; off += int64(len(buf)) {
			if _, err := f.ReadAt(buf, off); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkReadDirect(b *testing.B) {
	benchRead(b, func(_ *testing.B, f filesystem.Filesystem) filesystem.Filesystem { return f })
}

func BenchmarkReadRemote(b *testing.B) {
	benchRead(b, func(b *testing.B, fsys filesystem.Filesystem) filesystem.Filesystem {
		t := &testing.T{}
		x, y := socketpair(t)
		go func() { _ = Serve(y, fsys) }()
		rem, err := New(x)
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { _ = rem.Close() })
		return rem
	})
}
