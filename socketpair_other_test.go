// SPDX-License-Identifier: BSD-3-Clause

//go:build !unix

package remote

import (
	"net"
	"testing"
)

func socketpair(t *testing.T) (net.Conn, net.Conn) {
	t.Skip("no socketpair here")
	return nil, nil
}
