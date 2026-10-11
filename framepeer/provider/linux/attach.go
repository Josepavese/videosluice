//go:build linux

package linux

import (
	"bytes"
	"context"
	"net"
	"time"

	peer "github.com/Josepavese/videosluice/framepeer"
)

var attachHello = []byte{'F', 'A', 'P', 'H', 1, 0, 0, 0}

// AttachedConnection owns only the accepted socket. The capability remains
// owned by the controller's grant. This is attachment to an already running
// consumer, NOT process launch. No PID is reopened or inferred from its name.
type AttachedConnection struct {
	control *ControlConnection
	writer  *FrameWriter
}

var _ peer.Connection = (*AttachedConnection)(nil)

// AttachStartedConsumer consumes conn on SUCCESS only; a failure can follow a
// partial bootstrap send, so the caller MUST close it and never retry on it.
// Both socket sides must have SO_PASSCRED enabled before queuing records. Native
// socket/FD/Go/kernel footprint must already be admitted by the endpoint owner.
// The only unauthenticated data is a fixed hello with no identity or selectors.
// It is verified with kernel credentials against the capability BEFORE the
// broker releases the consumer's pinned self handle used by the existing C SDK.
func AttachStartedConsumer(ctx context.Context, conn *net.UnixConn, consumer *Lease, maxWait time.Duration) (*AttachedConnection, error) {
	if ctx == nil || conn == nil || consumer == nil {
		return nil, peer.ErrInvalid
	}
	control, err := NewControlConnection(conn, consumer, maxWait)
	if err != nil {
		return nil, err
	}
	var hello [8]byte
	n, principal, err := control.Receive(ctx, hello[:])
	if err != nil {
		return nil, failure("attach_hello_receive", err)
	}
	if n != len(hello) || !bytes.Equal(hello[:], attachHello) || principal.Generation != consumer.generation {
		return nil, failure("attach_hello", peer.ErrUnauthorized)
	}
	metadata, err := EncodeProcessIdentity(ProcessIdentity{PID: consumer.pid, UID: consumer.uid, GID: consumer.gid, Generation: consumer.generation})
	if err != nil {
		return nil, err
	}
	if err = control.WriteCommand(ctx, metadata, consumer); err != nil {
		return nil, failure("attach_bootstrap", err)
	}
	writer, err := control.FrameWriter()
	if err != nil {
		return nil, err
	}
	return &AttachedConnection{control: control, writer: writer}, nil
}
func (c *AttachedConnection) CurrentGeneration() (string, error) { return c.writer.CurrentGeneration() }
func (c *AttachedConnection) Receive(ctx context.Context, p []byte) (int, peer.Principal, error) {
	return c.control.Receive(ctx, p)
}
func (c *AttachedConnection) WritePacket(ctx context.Context, p []byte) error {
	return c.writer.WritePacket(ctx, p)
}
func (c *AttachedConnection) Close() error { return c.control.Close() }
