//go:build linux

package linux

import (
	"context"
	"errors"
	peer "github.com/Josepavese/videosluice/framepeer"
	"golang.org/x/sys/unix"
	"net"
	"os"
	"sync"
	"time"
)

// Launch owns one private launch channel and the two descriptors to inherit.
// The caller must admit its provider footprint BEFORE PrepareLaunch (including
// creation-time duplicates). Files are borrowed only for exec.ExtraFiles;
// CloseInherited follows successful Start, Close follows failed Start/teardown.
// A nonnil result on error still owns cleanup obligations; never discard it.
type Launch struct {
	connection        *net.UnixConn
	files             [2]*os.File
	metadata          []byte
	mu                sync.Mutex
	delivered, closed bool
	closeErr          error
	child             *Lease
	ctx               context.Context
	cancel            context.CancelFunc
}

func PrepareLaunch(controller *Lease) (*Launch, error) {
	if controller == nil {
		return nil, peer.ErrInvalid
	}
	if _, err := controller.CurrentGeneration(); err != nil {
		return nil, err
	}
	life, cancel := context.WithCancel(context.Background())
	l := &Launch{ctx: life, cancel: cancel}
	fail := func(err error) (*Launch, error) {
		if closeErr := l.Close(); closeErr != nil {
			return l, errors.Join(err, closeErr)
		}
		return nil, err
	}
	file, identity, err := controller.Export()
	if err != nil {
		return fail(err)
	}
	l.files[1] = file
	l.metadata, err = EncodeProcessIdentity(identity)
	if err != nil {
		return fail(err)
	}
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return fail(failure("launch_pair", peer.ErrUnavailable))
	}
	parent := os.NewFile(uintptr(fds[0]), "framepeer-launch-parent")
	l.files[0] = os.NewFile(uintptr(fds[1]), "framepeer-launch-child")
	// Enable credentials before either side can queue the first record.
	for _, fd := range fds {
		if err = unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_PASSCRED, 1); err != nil {
			l.closeErr = errors.Join(l.closeErr, parent.Close())
			return fail(failure("launch_credentials", peer.ErrUnavailable))
		}
	}
	connection, err := net.FileConn(parent)
	closeErr := parent.Close()
	l.closeErr = errors.Join(l.closeErr, closeErr)
	if err != nil {
		return fail(errors.Join(failure("launch_connection", peer.ErrUnavailable), closeErr))
	}
	l.connection = connection.(*net.UnixConn)
	if closeErr != nil {
		return fail(closeErr)
	}
	if _, err = ConfigurePacketSocket(l.connection, 32768); err != nil {
		return fail(err)
	}
	return l, nil
}

// Inherited returns exclusively borrowed handles and owned public identity
// metadata. Descriptor positions are chosen by the launcher after other inputs.
func (l *Launch) Inherited() ([2]*os.File, []byte, error) {
	if l == nil {
		return [2]*os.File{}, nil, peer.ErrInvalid
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed || l.files[0] == nil || l.files[1] == nil {
		return [2]*os.File{}, nil, peer.ErrRetired
	}
	return l.files, append([]byte(nil), l.metadata...), nil
}
func (l *Launch) CloseInherited() error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	for i, file := range l.files {
		if file != nil {
			l.closeErr = errors.Join(l.closeErr, file.Close())
			l.files[i] = nil
		}
	}
	return l.closeErr
}

// Deliver binds the exact os.Process from successful Start. A send error is
// ambiguous: no second bootstrap is sent. The caller retires this launch.
// The returned endpoint is BORROWED; Launch remains its sole close owner.
func (l *Launch) Deliver(ctx context.Context, process *os.Process, generation string, uid, gid uint32, maxWait time.Duration) (*ControlConnection, error) {
	if l == nil || ctx == nil {
		return nil, peer.ErrInvalid
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed || l.delivered {
		return nil, peer.ErrRetired
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if l.connection == nil || l.ctx == nil {
		return nil, peer.ErrInvalid
	}
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(l.ctx, cancel)
	defer func() { stop(); cancel() }()
	child, err := BindStartedProcess(process, generation, uid, gid)
	if err != nil {
		return nil, err
	}
	l.child = child
	endpoint, err := NewControlConnection(l.connection, child, maxWait)
	if err != nil {
		return nil, err
	}
	packet, err := EncodeProcessIdentity(ProcessIdentity{PID: process.Pid, UID: uid, GID: gid, Generation: generation})
	if err != nil {
		return nil, err
	}
	l.delivered = true
	if err = endpoint.WriteCommand(ctx, packet, child); err != nil {
		return nil, err
	}
	return endpoint, nil
}
func (l *Launch) Close() error {
	if l == nil {
		return nil
	}
	if l.cancel != nil {
		l.cancel()
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return l.closeErr
	}
	l.closed = true
	for i, file := range l.files {
		if file != nil {
			l.closeErr = errors.Join(l.closeErr, file.Close())
			l.files[i] = nil
		}
	}
	if l.connection != nil {
		l.closeErr = errors.Join(l.closeErr, l.connection.Close())
	}
	if l.child != nil {
		l.closeErr = errors.Join(l.closeErr, l.child.Close())
	}
	return l.closeErr
}

// AcceptedLaunch owns the channel, inherited controller handle and exact self
// handle sent by that authenticated controller. Domain construction can use
// Self as ProcessAuthority; it must not substitute an environment/PID label.
type AcceptedLaunch struct {
	Controller, Self *Lease
	Control          *ControlReceiver
	once             sync.Once
	controlOnce      sync.Once
	controlCloseErr  error
	closeErr         error
	raw              [3]*os.File
	connection       net.Conn
}

// CloseControl retires the owned channel without closing process authorities.
// A session may call it while borrowing AcceptedLaunch; final Close reuses the
// same result and never closes a potentially reused descriptor a second time.
func (a *AcceptedLaunch) CloseControl() error {
	if a == nil {
		return nil
	}
	a.controlOnce.Do(func() {
		if a.connection != nil {
			a.controlCloseErr = a.connection.Close()
		}
	})
	return a.controlCloseErr
}

func (a *AcceptedLaunch) Close() error {
	if a == nil {
		return nil
	}
	a.once.Do(func() {
		a.closeErr = errors.Join(a.closeErr, a.CloseControl())
		for i, file := range a.raw {
			if file != nil {
				a.closeErr = errors.Join(a.closeErr, file.Close())
				a.raw[i] = nil
			}
		}
		if a.Self != nil {
			a.closeErr = errors.Join(a.closeErr, a.Self.Close())
		}
		if a.Controller != nil {
			a.closeErr = errors.Join(a.closeErr, a.Controller.Close())
		}
	})
	return a.closeErr
}

// AcceptLaunch consumes the two inherited descriptors on all paths (the caller
// must supply distinct nonnil handles). Metadata is trusted launcher input;
// it is validated against the inherited pidfd, not accepted as authority alone.
// The caller admits receive storage and complete native footprint in advance.
func AcceptLaunch(ctx context.Context, socket, controllerFile *os.File, metadata []byte, maxWait time.Duration) (*AcceptedLaunch, error) {
	if socket == nil || controllerFile == nil || socket == controllerFile || socket.Fd() == controllerFile.Fd() {
		return nil, peer.ErrInvalid
	}
	a := &AcceptedLaunch{raw: [3]*os.File{socket, controllerFile, nil}}
	fail := func(err error) (*AcceptedLaunch, error) {
		if closeErr := a.Close(); closeErr != nil {
			return a, errors.Join(err, closeErr)
		}
		return nil, err
	}
	if ctx == nil {
		return fail(peer.ErrInvalid)
	}
	if err := ctx.Err(); err != nil {
		return fail(err)
	}
	identity, err := DecodeProcessIdentity(metadata)
	if err != nil {
		return fail(err)
	}
	controller, err := ImportProcessCapability(controllerFile, identity)
	if err != nil {
		return fail(err)
	}
	a.Controller = controller
	a.raw[1] = nil
	conn, err := net.FileConn(socket)
	closeErr := socket.Close()
	a.raw[0] = nil
	a.closeErr = errors.Join(a.closeErr, closeErr)
	if err != nil {
		return fail(errors.Join(failure("launch_accept_connection", peer.ErrUnavailable), closeErr))
	}
	a.connection = conn
	r, err := NewControlReceiver(conn.(*net.UnixConn), controller, 4096, maxWait)
	if err != nil {
		return fail(err)
	}
	a.Control = r
	if closeErr != nil {
		return fail(closeErr)
	}
	var buffer [256]byte
	n, _, selfFile, err := r.Receive(ctx, buffer[:])
	if err != nil {
		return fail(err)
	}
	if selfFile == nil {
		return fail(failure("launch_self_missing", peer.ErrInvalid))
	}
	a.raw[2] = selfFile
	selfIdentity, err := DecodeProcessIdentity(buffer[:n])
	if err != nil {
		return fail(err)
	}
	if selfIdentity.PID != os.Getpid() || selfIdentity.UID != uint32(os.Getuid()) || selfIdentity.GID != uint32(os.Getgid()) {
		return fail(failure("launch_self_identity", peer.ErrUnauthorized))
	}
	a.Self, err = ImportProcessCapability(selfFile, selfIdentity)
	if err != nil {
		return fail(err)
	}
	a.raw[2] = nil
	return a, nil
}
