//go:build linux

package linux

import (
	"context"
	peer "github.com/Josepavese/videosluice/framepeer"
	"net"
	"os"
	"time"
)

// ControlConnection owns one socket on constructor success only. Its complete
// provider footprint must be admitted before creation. The broker lease is
// borrowed, not closed; no process IDs are reopened from command metadata.
type ControlConnection struct {
	receiver *Receiver
	limits   PacketSocketLimits
}

var _ peer.ControlConnection = (*ControlConnection)(nil)

func NewControlConnection(conn *net.UnixConn, broker *Lease, maxWait time.Duration) (*ControlConnection, error) {
	r, err := NewReceiver(conn, broker, 16384, maxWait)
	if err != nil {
		return nil, err
	}
	limits, err := ConfigurePacketSocket(conn, 32768)
	if err != nil {
		return nil, err
	}
	return &ControlConnection{receiver: r, limits: limits}, nil
}
func (c *ControlConnection) CurrentGeneration() (string, error) {
	return c.receiver.lease.CurrentGeneration()
}
func (c *ControlConnection) Receive(ctx context.Context, p []byte) (int, peer.Principal, error) {
	return c.receiver.Receive(ctx, p)
}
func (c *ControlConnection) Close() error { return c.receiver.Close() }

// ExportPeer duplicates the exact pinned process for trusted launcher exec
// inputs. The caller admits/owns the duplicate; the original lease and socket
// remain with their current owner. No PID lookup or authority from arguments.
func (c *ControlConnection) ExportPeer() (*os.File, ProcessIdentity, error) {
	if c == nil || c.receiver == nil || c.receiver.lease == nil {
		return nil, ProcessIdentity{}, peer.ErrInvalid
	}
	return c.receiver.lease.Export()
}

// FrameWriter borrows the already authenticated launch transport. Launch keeps
// sole close ownership; control/bootstrap and frame writes share the receiver's
// serialization and exact peer capability. The caller admits sender buffers.
func (c *ControlConnection) FrameWriter() (*FrameWriter, error) {
	if c == nil {
		return nil, peer.ErrInvalid
	}
	return NewFrameWriter(c.receiver)
}

func (c *ControlConnection) WriteCommand(ctx context.Context, p []byte, capability peer.ProcessCapability) error {
	if c == nil || len(p) == 0 || len(p) > 4096 {
		return failure("control_send_contract", peer.ErrInvalid)
	}
	var lease *Lease
	if capability != nil {
		var ok bool
		lease, ok = capability.(*Lease)
		if !ok || lease == nil {
			return failure("control_send_provider", peer.ErrUnavailable)
		}
	}
	return writeRecord(ctx, c.receiver, c.limits, p, lease)
}
