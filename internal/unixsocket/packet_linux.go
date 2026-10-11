//go:build linux

package unixsocket

import (
	"errors"
	"net"
	"time"

	"golang.org/x/sys/unix"
)

// ListenPacketSingleOwner uses the existing inode-bound owner. SO_PASSCRED
// and backlog1 are configured before readiness is exposed. Provider admission
// must include the kernel's actual backlog/socket accounting, not just this hint.
// The caller must reserve native/listener/backlog costs before calling.
func ListenPacketSingleOwner(path string) (*Publication, error) {
	return listenSingleOwner(path, "unixpacket", func(listener *net.UnixListener) error {
		raw, err := listener.SyscallConn()
		if err != nil {
			return err
		}
		var setupErr error
		err = raw.Control(func(handle uintptr) {
			setupErr = unix.SetsockoptInt(int(handle), unix.SOL_SOCKET, unix.SO_PASSCRED, 1)
			if setupErr == nil {
				setupErr = unix.Listen(int(handle), 1)
			}
		})
		return errors.Join(err, setupErr)
	}, true)
}

// AcceptPacket returns a connection even on a partial failure; its caller
// retains cleanup ownership. One endpoint owner serializes calls.
func (publication *Publication) AcceptPacket(deadline time.Time) (*net.UnixConn, error) {
	if publication == nil {
		return nil, net.ErrClosed
	}
	listener, ok := publication.listener.(*net.UnixListener)
	if !ok || listener.Addr().Network() != "unixpacket" {
		return nil, errors.New("endpoint is not a packet listener")
	}
	if err := listener.SetDeadline(deadline); err != nil {
		return nil, err
	}
	return listener.AcceptUnix()
}
