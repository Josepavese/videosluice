//go:build linux

package linux

import (
	"context"
	peer "github.com/Josepavese/videosluice/framepeer"
	"net"
	"time"
)

type FrameConnection struct {
	receiver *Receiver
	writer   *FrameWriter
}

var _ peer.Connection = (*FrameConnection)(nil)

// NewFrameConnection consumes the exclusively owned socket on success only.
// The caller must reserve its verified provider footprint before this call.
func NewFrameConnection(conn *net.UnixConn, lease *Lease, maxBytes int, maxWait time.Duration) (*FrameConnection, error) {
	r, err := NewReceiver(conn, lease, maxBytes, maxWait)
	if err != nil {
		return nil, err
	}
	w, err := NewFrameWriter(r)
	if err != nil {
		return nil, err
	}
	return &FrameConnection{receiver: r, writer: w}, nil
}
func (c *FrameConnection) Receive(ctx context.Context, p []byte) (int, peer.Principal, error) {
	return c.receiver.Receive(ctx, p)
}
func (c *FrameConnection) WritePacket(ctx context.Context, p []byte) error {
	return c.writer.WritePacket(ctx, p)
}
func (c *FrameConnection) CurrentGeneration() (string, error) { return c.writer.CurrentGeneration() }
func (c *FrameConnection) Close() error                       { return c.receiver.Close() }
