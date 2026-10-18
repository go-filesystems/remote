// SPDX-License-Identifier: BSD-3-Clause

package remote

import (
	"errors"
	"io/fs"
	"os"
	"syscall"
)

// Operations. A request's code is one of these; a reply's is a status.
const (
	opHello byte = iota + 1 // capabilities, sent by the server first
	opReadFile
	opListDir
	opStat
	opWriteFile
	opReadLink
	opMkDir
	opDeleteFile
	opDeleteDir
	opRename
	opLabel
	opSymlink
	opLink
	opTruncate
	opChmod
	opChown
	opChtimes
	opOpenFile
	// On a file the server holds open.
	opFileReadAt
	opFileWriteAt
	opFileSize
	opFileTruncate
	opFileSync
	opFileClose
	opLast // not an op: the bound
)

const (
	statusOK byte = iota
	statusErr
)

// Capabilities the server's filesystem has, announced in the hello.
const (
	capLabel uint64 = 1 << iota
	capSymlink
	capLink
	capTruncate
	capMetadata
	capOpen
)

// What a Stat value carried besides mode, size and inode.
const (
	statUnixModTime uint64 = 1 << iota // ModTime() int64, seconds
	statTimeModTime                    // ModTime() time.Time
)

// How an opened file comes back.
const (
	openWritable uint64 = 1 << iota // the handle may be written
)

// Kinds an error is, beyond its text, so that errors.Is answers on the client
// as it would have on the server.
const (
	kindNotExist uint64 = 1 << iota
	kindExist
	kindPermission
	kindInvalid
	kindClosed
	kindUnsupported
)

var kinds = []struct {
	bit uint64
	err error
}{
	{kindNotExist, fs.ErrNotExist},
	{kindExist, fs.ErrExist},
	{kindPermission, fs.ErrPermission},
	{kindInvalid, fs.ErrInvalid},
	{kindClosed, fs.ErrClosed},
	{kindUnsupported, errors.ErrUnsupported},
}

// The forms an error takes on the wire: the standard wrappers a driver
// returns are rebuilt as themselves, so a caller can still errors.As them and
// their text reads the same.
const (
	formPlain   uint64 = iota // just the text
	formPath                  // *fs.PathError{Op, Path, Err}
	formLink                  // *os.LinkError{Op, Old, New, Err}
	formSyscall               // *os.SyscallError{Syscall, Err}
)

// encodeErr writes err: its kinds, its form and that form's fields, its
// errno if it has one, and the text of the innermost error.
func encodeErr(e *enc, err error) {
	var bits uint64
	for _, k := range kinds {
		if errors.Is(err, k.err) {
			bits |= k.bit
		}
	}
	var errno syscall.Errno
	errors.As(err, &errno)
	form, a, b, c := formPlain, "", "", ""
	inner := err
	var pe *fs.PathError
	var le *os.LinkError
	var se *os.SyscallError
	switch {
	case errors.As(err, &pe):
		form, a, b, inner = formPath, pe.Op, pe.Path, pe.Err
	case errors.As(err, &le):
		form, a, b, c, inner = formLink, le.Op, le.Old, le.New, le.Err
	case errors.As(err, &se):
		form, a, inner = formSyscall, se.Syscall, se.Err
	}
	e.u64(bits)
	e.u64(uint64(errno))
	e.u64(form)
	e.str(a)
	e.str(b)
	e.str(c)
	e.str(inner.Error())
}

// decodeErr rebuilds what encodeErr wrote. An errno comes back as itself,
// whose Is already knows fs.ErrNotExist and the rest; any other error as its
// text, with the kinds it had; and around it the wrapper it came in.
func decodeErr(d *dec) error {
	bits := d.u64()
	errno := syscall.Errno(d.u64())
	form := d.u64()
	a, b, c, msg := d.str(), d.str(), d.str(), d.str()
	if err := d.done(); err != nil {
		return err
	}
	var inner error = &Error{Msg: msg, kinds: bits}
	if errno != 0 {
		inner = errno
	}
	switch form {
	case formPlain:
		return inner
	case formPath:
		return &fs.PathError{Op: a, Path: b, Err: inner}
	case formLink:
		return &os.LinkError{Op: a, Old: b, New: c, Err: inner}
	case formSyscall:
		return &os.SyscallError{Syscall: a, Err: inner}
	}
	return errMalformed
}

// Error is an error the server's filesystem returned that was not an errno:
// its text, and the kinds it was (fs.ErrNotExist, fs.ErrPermission, ...),
// which errors.Is still recognises.
type Error struct {
	Msg   string
	kinds uint64
}

func (e *Error) Error() string { return e.Msg }

// Is reports the kinds the original error was.
func (e *Error) Is(target error) bool {
	for _, k := range kinds {
		if e.kinds&k.bit != 0 && target == k.err {
			return true
		}
	}
	return false
}
