// SPDX-License-Identifier: BSD-3-Clause

// Package remote is a go-filesystems filesystem served over a connection:
// Serve answers requests against any filesystem.Filesystem, and New is that
// filesystem at the other end, with the same interfaces and the same errors.
//
// It keeps the process that touches the files apart from the process that
// talks to the network -- for go-fileshare/fileshare, a worker running as the
// person whose files they are, behind a front that holds no privilege.
// Neither side trusts the other's bytes: every frame and every field in it is
// bounded before anything is allocated for it.
package remote
