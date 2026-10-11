//go:build linux

package linux

import (
	"context"
	peer "github.com/Josepavese/videosluice/framepeer"
)

// FrameWriter shares the execution-bound frame receiver's exclusive transport
// ownership and write lock. Construct once before starting I/O. It borrows the
// receiver: the session owner closes the Receiver after sender teardown.
type FrameWriter struct {
	reader *Receiver
	limits PacketSocketLimits
}

var _ peer.PacketWriter = (*FrameWriter)(nil)

func NewFrameWriter(receiver *Receiver) (*FrameWriter, error) {
	if receiver == nil || receiver.connection == nil || receiver.lease == nil {
		return nil, peer.ErrInvalid
	}
	if _, err := receiver.lease.CurrentGeneration(); err != nil {
		return nil, err
	}
	limits, err := ConfigurePacketSocket(receiver.connection, 32768)
	if err != nil {
		return nil, err
	}
	return &FrameWriter{reader: receiver, limits: limits}, nil
}

func (w *FrameWriter) WritePacket(ctx context.Context, packet []byte) error {
	if w == nil {
		return peer.ErrInvalid
	}
	return writePacket(ctx, w.reader, w.limits, packet)
}

func (w *FrameWriter) CurrentGeneration() (string, error) {
	if w == nil || w.reader == nil || w.reader.lease == nil {
		return "", peer.ErrInvalid
	}
	return w.reader.lease.CurrentGeneration()
}
