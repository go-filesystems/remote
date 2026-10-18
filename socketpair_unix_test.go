// SPDX-License-Identifier: BSD-3-Clause

//go:build unix

package remote

import (
	"net"
	"os"
	"syscall"
	"testing"
)

// socketpair is a connected pair of unix stream sockets, as a parent makes
// one to hand an end to a child.
func socketpair(t *testing.T) (net.Conn, net.Conn) {
	t.Helper()
	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	conn := func(fd int) net.Conn {
		f := os.NewFile(uintptr(fd), "socketpair")
		defer f.Close()
		c, err := net.FileConn(f)
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	return conn(fds[0]), conn(fds[1])
}
