//go:build linux

package linux

import (
	"context"
	peer "github.com/Josepavese/videosluice/framepeer"
	"net"
	"os"
	"time"
)

// ControlReceiver is a separate, exclusively owned control connection from the
// pinned launch authority. It admits zero or one raw FD only after authenticating
// the record sender. It does NOT validate command schema/sequence or the FD type.
// Those checks must precede installing a grant; failure must close the returned
// file. A frame-request Receiver continues to reject every received descriptor.
type ControlReceiver struct {
	reader *Receiver
	limits PacketSocketLimits
}

// Caller must admit connection, control-buffer and transient FD capacity before
// receiving: fixed OOB capacity is56B on64-bit Linux (at most10 unsolicited FDs
// can transiently fit without credentials; all rejects are closed). Payload is
// borrowed and limited to4096B. No pending capability queue is created here.
func NewControlReceiver(connection *net.UnixConn, controller *Lease, maxBytes int, maxWait time.Duration) (*ControlReceiver, error) {
	if maxBytes > 4096 {
		return nil, peer.ErrInvalid
	}
	r, err := NewReceiver(connection, controller, maxBytes, maxWait)
	if err != nil {
		return nil, err
	}
	limits, err := ConfigurePacketSocket(connection, 32768)
	if err != nil {
		return nil, err
	}
	return &ControlReceiver{reader: r, limits: limits}, nil
}
func (r *ControlReceiver) Close() error { return r.reader.Close() }

var _ peer.ControlServerConnection = (*ControlReceiver)(nil)

func (r *ControlReceiver) CurrentGeneration() (string, error) {
	if r == nil || r.reader == nil || r.reader.lease == nil {
		return "", peer.ErrInvalid
	}
	return r.reader.lease.CurrentGeneration()
}

// ReceiveCommand keeps raw descriptor ownership inside the Linux provider.
// Wrapping a nonnil file cannot fail or duplicate the descriptor; it preserves
// even a partial/error result for the caller's single cleanup path.
func (r *ControlReceiver) ReceiveCommand(ctx context.Context, buffer []byte) (int, peer.Principal, peer.PendingCapability, error) {
	if r == nil {
		return 0, peer.Principal{}, nil, peer.ErrInvalid
	}
	n, principal, file, err := r.Receive(ctx, buffer)
	if file == nil {
		return n, principal, nil, err
	}
	return n, principal, &pendingCapability{file: file}, err
}
func (r *ControlReceiver) Receive(ctx context.Context, buffer []byte) (int, peer.Principal, *os.File, error) {
	if ctx == nil {
		return 0, peer.Principal{}, nil, peer.ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return 0, peer.Principal{}, nil, err
	}
	if err := ValidatePacketSocket(r.reader.connection, r.limits); err != nil {
		return 0, peer.Principal{}, nil, err
	}
	record, err := r.reader.receive(ctx, buffer, true)
	if err == nil {
		if changed := ValidatePacketSocket(r.reader.connection, r.limits); changed != nil {
			if record.capability != nil {
				record.capability.Close()
			}
			return 0, peer.Principal{}, nil, changed
		}
		if cancelled := ctx.Err(); cancelled != nil {
			if record.capability != nil {
				record.capability.Close()
			}
			return 0, peer.Principal{}, nil, cancelled
		}
	}
	return record.bytes, record.principal, record.capability, err
}
