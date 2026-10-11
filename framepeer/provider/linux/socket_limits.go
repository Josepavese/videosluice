//go:build linux

package linux

import (
	peer "github.com/Josepavese/videosluice/framepeer"
	"golang.org/x/sys/unix"
	"net"
)

// PacketSocketLimits describes verified socket settings, NOT an allocation
// footprint or an absolute RAM bound. AF_UNIX ignores SO_RCVBUF; an ingress
// admission must also constrain the actual sender endpoint. SO_SNDBUF is
// reported by Linux with doubled accounting and may be clamped at setup.
type PacketSocketLimits struct{ SendAccountingBytes int }

func packetSocketSettings(connection *net.UnixConn, requested int) (PacketSocketLimits, error) {
	if connection == nil {
		return PacketSocketLimits{}, peer.ErrInvalid
	}
	raw, err := connection.SyscallConn()
	if err != nil {
		return PacketSocketLimits{}, failure("socket_limit_descriptor", peer.ErrUnavailable)
	}
	var result PacketSocketLimits
	var operationErr error
	err = raw.Control(func(fd uintptr) {
		kind, e := unix.GetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_TYPE)
		if e != nil || kind != unix.SOCK_SEQPACKET {
			operationErr = failure("socket_limit_type", peer.ErrInvalid)
			return
		}
		if requested != 0 {
			if e = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_SNDBUF, requested); e != nil {
				operationErr = failure("socket_limit_setup", peer.ErrUnavailable)
				return
			}
		}
		result.SendAccountingBytes, e = unix.GetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_SNDBUF)
		if e != nil || result.SendAccountingBytes <= 0 {
			operationErr = failure("socket_limit_readback", peer.ErrUnavailable)
		}
	})
	if err != nil {
		return PacketSocketLimits{}, failure("socket_limit_descriptor", peer.ErrUnavailable)
	}
	if operationErr != nil {
		return PacketSocketLimits{}, operationErr
	}
	return result, nil
}

// ConfigurePacketSocket mutates only the exclusively owned supplied socket.
// The caller must have admitted native creation separately and still owns the
// connection on error. No sysctl, global rlimit, busy-poll or receive setting
// is changed. Unsupported/clamped settings fail explicitly, not by fallback.
func ConfigurePacketSocket(connection *net.UnixConn, requestedSendBytes int) (PacketSocketLimits, error) {
	if requestedSendBytes < 8192 || requestedSendBytes > 131072 {
		return PacketSocketLimits{}, peer.ErrInvalid
	}
	actual, err := packetSocketSettings(connection, requestedSendBytes)
	if err != nil {
		return PacketSocketLimits{}, err
	}
	if actual.SendAccountingBytes != 2*requestedSendBytes {
		return PacketSocketLimits{}, failure("socket_limit_clamped", peer.ErrUnavailable)
	}
	return actual, nil
}

func ValidatePacketSocket(connection *net.UnixConn, expected PacketSocketLimits) error {
	if expected.SendAccountingBytes < 16384 || expected.SendAccountingBytes > 262144 {
		return peer.ErrInvalid
	}
	actual, err := packetSocketSettings(connection, 0)
	if err != nil {
		return err
	}
	if actual != expected {
		return failure("socket_limit_changed", peer.ErrUnavailable)
	}
	return nil
}
