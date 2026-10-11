//go:build linux

// Package linux verifies per-record kernel credentials against a live process
// handle owned by the trusted launcher. Privileged identity impersonation,
// compromised launchers and same-process code injection are outside its trust
// boundary. It does not inspect command names or infer authority from UID alone.
package linux

import (
	"context"
	"errors"
	peer "github.com/Josepavese/videosluice/framepeer"
	"golang.org/x/sys/unix"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

func failure(stage string, cause error) error { return &peer.Failure{Stage: stage, Cause: cause} }

// Lease holds the exact execution supplied by the trusted launcher, either
// directly from Start or as an imported pinned capability. No PID-based reopen,
// signal or retry occurs. Generation comes from the authority, not a request.
type processHandle interface{ WithHandle(func(uintptr)) error }
type Lease struct {
	process    processHandle
	pid        int
	generation string
	uid, gid   uint32
	revoked    atomic.Bool
	closeOnce  sync.Once
	closeOwned func() error
	closeErr   error
}

func validGeneration(generation string) bool {
	if len(generation) < 1 || len(generation) > 64 {
		return false
	}
	for _, c := range []byte(generation) {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_' || c == '.') {
			return false
		}
	}
	return true
}

func BindStartedProcess(process *os.Process, generation string, uid, gid uint32) (*Lease, error) {
	if process == nil || process.Pid <= 0 || !validGeneration(generation) {
		return nil, peer.ErrInvalid
	}
	l := &Lease{process: process, pid: process.Pid, generation: generation, uid: uid, gid: gid}
	if _, err := l.CurrentGeneration(); err != nil {
		return nil, err
	}
	return l, nil
}

func (l *Lease) Revoke() {
	if l != nil {
		l.revoked.Store(true)
	}
}

// Close releases only an imported capability owned by this lease; it does not
// kill or Wait for a process, nor release the launcher's original os.Process.
func (l *Lease) Close() error {
	if l == nil {
		return nil
	}
	l.Revoke()
	l.closeOnce.Do(func() {
		if l.closeOwned != nil {
			l.closeErr = l.closeOwned()
		}
	})
	return l.closeErr
}
func (l *Lease) CurrentGeneration() (string, error) {
	if l == nil || l.process == nil {
		return "", peer.ErrInvalid
	}
	if l.revoked.Load() {
		return "", failure("execution_revoked", peer.ErrRetired)
	}
	check := peer.ErrUnavailable
	err := l.process.WithHandle(func(handle uintptr) {
		check = pollProcessHandle(handle, unix.Poll)
	})
	if errors.Is(err, os.ErrNoHandle) {
		l.Revoke()
		return "", failure("execution_handle", peer.ErrUnavailable)
	}
	if err != nil || check != nil || l.revoked.Load() {
		l.Revoke()
		var diagnostic *peer.Failure
		if err == nil && errors.As(check, &diagnostic) {
			return "", check
		}
		if check == peer.ErrUnavailable && err == nil {
			return "", failure("execution_poll", peer.ErrUnavailable)
		}
		return "", failure("execution_retired", peer.ErrRetired)
	}
	return l.generation, nil
}

func pollProcessHandle(handle uintptr, poll func([]unix.PollFd, int) (int, error)) error {
	fds := []unix.PollFd{{Fd: int32(handle), Events: unix.POLLIN}}
	// A runtime/OS signal can interrupt poll without indicating process exit.
	// Retry only EINTR against the same held pidfd; four zero-wait calls bound
	// CPU under a signal storm. No PID reopen, sleep, or retry on real errors.
	for attempt := 0; attempt < 4; attempt++ {
		fds[0].Revents = 0
		n, err := poll(fds, 0)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if err != nil {
			return failure("execution_poll_error", peer.ErrUnavailable)
		}
		if n != 0 || fds[0].Revents != 0 {
			return peer.ErrRetired
		}
		return nil
	}
	return failure("execution_poll_interrupted", peer.ErrUnavailable)
}

type Receiver struct {
	connection *net.UnixConn
	lease      *Lease
	read       sync.Mutex
	write      sync.Mutex
	maxBytes   int
	maxWait    time.Duration
}

var _ peer.Receiver = (*Receiver)(nil)

// NewReceiver consumes an exclusively owned connected Unix packet socket on
// success only. One in-flight read; no queue, buffer copies or read goroutines.
// Per-connection admission and socket/kernel budgets belong to the caller.
func NewReceiver(connection *net.UnixConn, lease *Lease, maxBytes int, maxWait time.Duration) (*Receiver, error) {
	if connection == nil || lease == nil || maxBytes < 1 || maxBytes > 65536 || maxWait <= 0 || maxWait > 5*time.Second {
		return nil, peer.ErrInvalid
	}
	if _, err := lease.CurrentGeneration(); err != nil {
		return nil, err
	}
	raw, err := connection.SyscallConn()
	if err != nil {
		return nil, peer.ErrUnavailable
	}
	var setupErr error
	err = raw.Control(func(fd uintptr) {
		kind, e := unix.GetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_TYPE)
		if e != nil || kind != unix.SOCK_SEQPACKET {
			setupErr = peer.ErrInvalid
			return
		}
		setupErr = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_PASSCRED, 1)
	})
	if err != nil {
		return nil, failure("transport_descriptor", peer.ErrUnavailable)
	}
	if errors.Is(setupErr, peer.ErrInvalid) {
		return nil, failure("transport_type", peer.ErrInvalid)
	}
	if setupErr != nil {
		return nil, failure("credentials_setup", peer.ErrUnavailable)
	}
	return &Receiver{connection: connection, lease: lease, maxBytes: maxBytes, maxWait: maxWait}, nil
}

func (r *Receiver) Close() error { return r.connection.Close() }
func (r *Receiver) Receive(ctx context.Context, buffer []byte) (int, peer.Principal, error) {
	record, err := r.receive(ctx, buffer, false)
	return record.bytes, record.principal, err
}

type receivedRecord struct {
	bytes      int
	principal  peer.Principal
	capability *os.File
}

func (r *Receiver) receive(ctx context.Context, buffer []byte, allowCapability bool) (receivedRecord, error) {
	if ctx == nil || len(buffer) < 1 || len(buffer) > r.maxBytes {
		return receivedRecord{}, failure("receive_contract", peer.ErrInvalid)
	}
	if !r.read.TryLock() {
		return receivedRecord{}, failure("receive_busy", peer.ErrBusy)
	}
	defer r.read.Unlock()
	if err := ctx.Err(); err != nil {
		return receivedRecord{}, failure("receive_cancelled_before_read", errors.Join(err, peer.ErrNoRecord))
	}
	if _, err := r.lease.CurrentGeneration(); err != nil {
		return receivedRecord{}, err
	}
	deadline := time.Now().Add(r.maxWait)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	if err := r.connection.SetReadDeadline(deadline); err != nil {
		return receivedRecord{}, failure("receive_deadline_setup", peer.ErrUnavailable)
	}
	done := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { _ = r.connection.SetReadDeadline(time.Now()); close(done) })
	defer func() {
		if !stop() {
			<-done
		}
	}()
	// Fixed control space: credentials plus at most one intended capability on
	// the separate control channel. Kernel closes excess truncated descriptors;
	// every descriptor that does arrive is disposed unless ownership transfers.
	space := unix.CmsgSpace(unix.SizeofUcred)
	if allowCapability {
		space += unix.CmsgSpace(4)
	}
	control := make([]byte, space)
	n, oob, flags, _, readErr := r.connection.ReadMsgUnix(buffer, control)
	messages, parseErr := unix.ParseSocketControlMessage(control[:oob])
	var cred *unix.Ucred
	var descriptors []int
	unexpectedRights := false
	defer func() {
		for _, fd := range descriptors {
			_ = unix.Close(fd)
		}
	}()
	valid := parseErr == nil
	for _, m := range messages {
		switch {
		case m.Header.Level == unix.SOL_SOCKET && m.Header.Type == unix.SCM_RIGHTS:
			fds, e := unix.ParseUnixRights(&m)
			if e != nil {
				valid = false
			}
			descriptors = append(descriptors, fds...)
			if !allowCapability {
				unexpectedRights = true
			}
		case m.Header.Level == unix.SOL_SOCKET && m.Header.Type == unix.SCM_CREDENTIALS && cred == nil:
			var e error
			cred, e = unix.ParseUnixCredentials(&m)
			if e != nil {
				valid = false
			}
		default:
			valid = false
		}
	}
	// Go's poller preserves recvmsg's -1 on EAGAIN followed by a poll deadline;
	// a deadline before recvmsg yields 0. Neither consumes a sequenced record.
	if readErr != nil && os.IsTimeout(readErr) && (n == 0 || n == -1) && oob == 0 && flags == 0 {
		if _, err := r.lease.CurrentGeneration(); err != nil {
			return receivedRecord{}, err
		}
		return receivedRecord{}, failure("receive_idle_deadline", errors.Join(peer.ErrDeadline, peer.ErrNoRecord, ctx.Err()))
	}
	if err := ctx.Err(); err != nil {
		return receivedRecord{}, failure("receive_cancelled_after_read", err)
	}
	if readErr != nil {
		if os.IsTimeout(readErr) {
			return receivedRecord{}, failure("receive_deadline", peer.ErrDeadline)
		}
		return receivedRecord{}, failure("receive", peer.ErrUnavailable)
	}
	if flags&unix.MSG_CTRUNC != 0 {
		return receivedRecord{}, failure("receive_ancillary_truncated", peer.ErrRecord)
	}
	if flags&unix.MSG_TRUNC != 0 {
		return receivedRecord{}, failure("receive_payload_truncated", peer.ErrRecord)
	}
	if !valid {
		return receivedRecord{}, failure("receive_ancillary_invalid", peer.ErrRecord)
	}
	if cred == nil {
		return receivedRecord{}, failure("receive_credentials_missing", peer.ErrRecord)
	}
	if unexpectedRights {
		return receivedRecord{}, failure("receive_capability_forbidden", peer.ErrRecord)
	}
	if len(descriptors) > 1 {
		return receivedRecord{}, failure("receive_capability_cardinality", peer.ErrRecord)
	}
	if n == 0 {
		return receivedRecord{}, failure("receive_empty_record", peer.ErrRecord)
	}
	if cred.Pid != int32(r.lease.pid) || cred.Uid != r.lease.uid || cred.Gid != r.lease.gid {
		return receivedRecord{}, failure("receive_sender_mismatch", peer.ErrUnauthorized)
	}
	generation, err := r.lease.CurrentGeneration()
	if err != nil {
		return receivedRecord{}, err
	}
	if err = ctx.Err(); err != nil {
		return receivedRecord{}, failure("receive_cancelled_before_transfer", err)
	}
	record := receivedRecord{bytes: n, principal: peer.Principal{Generation: generation}}
	if len(descriptors) == 1 {
		record.capability = os.NewFile(uintptr(descriptors[0]), "framepeer-received-capability")
		descriptors = nil // exclusive ownership transfers only on authenticated success
	}
	return record, nil
}
