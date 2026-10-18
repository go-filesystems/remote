# remote

A [go-filesystems](https://github.com/go-filesystems) filesystem **served over a
connection**: one process opens the files, another serves them. `Serve` answers
requests against any `filesystem.Filesystem`; `New` is that filesystem at the
other end of the connection, with the same interfaces and the same errors.

It exists so that a file server can keep the process that talks to the network
apart from the process that touches the files. go-fileshare/fileshare uses it
to run a person's file access **as that person**: a worker started under their
uid serves the directory, and the front that parses SMB, WebDAV or SFTP calls it
through `remote` (fileshare's `docs/identity.md`).

```go
// in the process that may open the files
go remote.Serve(conn, osfsTree)

// in the process that serves them
fsys, err := remote.New(conn) // a filesystem.Filesystem, an Opener, ...
```

`conn` is any `io.ReadWriteCloser`: a unix socketpair handed to a child process,
a pipe, a TCP connection. Pure Go, `CGO_ENABLED=0`, every platform Go builds for.

## What crosses, and what is kept

- **Every method of `filesystem.Filesystem`**, and of the optional interfaces a
  protocol server looks for: `Opener`, `Symlinker`, `HardLinker`, `Truncater`,
  `MetadataSetter` and `LabelReader`. The client always has them. Those the
  server's filesystem lacks answer `errors.ErrUnsupported` (`Label`: `""`), and
  `Can` says which are really there.
- **Errors keep their meaning.**
  - An errno comes back as itself, so `errors.Is(err, fs.ErrNotExist)` answers
    as it did on the server.
  - Any other error comes back as its text, with the kinds it was
    (`fs.ErrNotExist`, `ErrExist`, `ErrPermission`, `ErrInvalid`, `ErrClosed`,
    `errors.ErrUnsupported`).
  - A `*fs.PathError`, `*os.LinkError` or `*os.SyscallError` is rebuilt as
    itself, with the same text.
- **A `Stat` keeps the modification time its driver gave**, in the form it gave
  it: `ModTime() int64` (as webdav, sftp and nfs ask for it) or
  `ModTime() time.Time` (as s3 does). Where the driver gave none, none is
  invented.
- **An opened file is held by the server.** Reads and writes are calls, in
  pieces of at most 1 MiB. `ReadAt` keeps `io.ReaderAt`'s contract to the letter,
  and a file is a `filesystem.WritableFile` exactly when the server's was. Files
  the client leaves open are closed by the server when the connection ends.

## The peer is not trusted

Each side treats the other's bytes as hostile.

- **Frames:** a frame is at most `MaxFrame` (32 MiB). Its length is checked
  before anything is allocated for it, and every string and count inside it is
  checked against what is left.
- **Unknown ops and replies:** an unknown op, or a reply to a call never made,
  ends the connection.
- **Limits:** a server works on at most 64 requests at once and holds at most
  4096 files per connection.

Each of these is a test. Each was also checked by removing it: ten defences,
ten tests that fail without them. One of those tests used to pass for the wrong
reason. It checked only that a 4 GiB frame was refused, without checking *why*:
the server believed the length, allocated the 4 GiB, and failed later on the
closed pipe. It now checks the reason.

## What it costs

Reading 64 MiB in 1 MiB pieces over a unix socketpair, on an Apple M-series
laptop under heavy load (load average about 155), gave:

- through `remote`: **about 0.3 GB/s**;
- through `osfs` directly: **about 10 GB/s**, from the page cache.

Those figures are an order of magnitude, not a benchmark. 0.3 GB/s keeps up
with a 1 Gb/s network and would limit a 10 Gb/s one. **Passing the file
descriptor** across a unix socket (`SCM_RIGHTS`) would remove this cost: the
server opens the file under its own identity, and the client reads and writes
it directly. That is the next step, and it will be measured. The benchmark is
`go test -bench Read`.

## Tests

The client is tested **against the same operations done directly**: every step
runs on `osfs` locally and on `osfs` through `remote`. The results, the error
types, the errnos, the kinds `errors.Is` recognises, and the text must all be
equal. The first run of that test found that an `*os.LinkError` (from `Rename`)
had come back as plain text.

Coverage is 100% and enforced, under `-race`. CI runs the tests on Linux
(amd64, arm64), macOS and Windows, and emulated on riscv64, loong64, ppc64le and
s390x. It builds for FreeBSD, OpenBSD, NetBSD, Plan 9, js/wasm and wasip1.

## Licence

BSD 3-Clause.
