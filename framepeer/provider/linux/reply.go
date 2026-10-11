//go:build linux

package linux

import (
	"context"
	peer "github.com/Josepavese/videosluice/framepeer"
	"golang.org/x/sys/unix"
	"os"
	"time"
)

// Reply borrows one admitted packet on this control connection. Single-flight
// write, no capability transfer, no queue or retry. A write error/cancellation
// can follow delivery: it MUST NOT undo a committed command. The controller
// reconciles authenticated receipts/status rather than assuming non-delivery.
// Its receiving endpoint must authenticate the broker separately; a connected
// socket or the reply body alone is not proof of the sender's identity.
func (r *ControlReceiver) Reply(ctx context.Context, packet []byte) error {
	if r == nil {
		return failure("reply_contract", peer.ErrInvalid)
	}
	return writePacket(ctx, r.reader, r.limits, packet)
}

func writePacket(ctx context.Context, w *Receiver, limits PacketSocketLimits, packet []byte) error {
	return writeRecord(ctx, w, limits, packet, nil)
}

func writeRecord(ctx context.Context, w *Receiver, limits PacketSocketLimits, packet []byte, capability *Lease) error {
	if ctx == nil || w == nil || w.connection == nil || w.lease == nil || len(packet) == 0 || len(packet) > 16384 {
		return failure("reply_contract", peer.ErrInvalid)
	}
	if !w.write.TryLock() {
		return failure("reply_busy", peer.ErrBusy)
	}
	defer w.write.Unlock()
	if err := ctx.Err(); err != nil {
		return failure("reply_cancelled_before_write", err)
	}
	if err := ValidatePacketSocket(w.connection, limits); err != nil {
		return err
	}
	if _, err := w.lease.CurrentGeneration(); err != nil {
		return err
	}
	if capability != nil {
		if _, err := capability.CurrentGeneration(); err != nil {
			return err
		}
	}
	deadline := time.Now().Add(w.maxWait)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	if err := w.connection.SetWriteDeadline(deadline); err != nil {
		return failure("reply_deadline_setup", peer.ErrUnavailable)
	}
	done := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { _ = w.connection.SetWriteDeadline(time.Now()); close(done) })
	defer func() {
		if !stop() {
			<-done
		}
	}()
	var n int
	var err error
	if capability == nil {
		n, err = w.connection.Write(packet)
	} else {
		// WithHandle pins the existing admitted handle across sendmsg. The kernel
		// duplicates for the receiver; no unaccounted local duplicate is created.
		var sendErr error
		err = capability.process.WithHandle(func(handle uintptr) {
			rights := unix.UnixRights(int(handle))
			var oob int
			n, oob, sendErr = w.connection.WriteMsgUnix(packet, rights, nil)
			if sendErr == nil && oob != len(rights) {
				sendErr = peer.ErrUnavailable
			}
		})
		if err == nil {
			err = sendErr
		}
	}
	if cancelled := ctx.Err(); cancelled != nil {
		return failure("reply_cancelled_after_write", cancelled)
	}
	if err != nil {
		if os.IsTimeout(err) {
			return failure("reply_deadline", peer.ErrDeadline)
		}
		return failure("reply_write", peer.ErrUnavailable)
	}
	if n != len(packet) {
		return failure("reply_short_write", peer.ErrUnavailable)
	}
	if err := ValidatePacketSocket(w.connection, limits); err != nil {
		return err
	}
	if _, err := w.lease.CurrentGeneration(); err != nil {
		return err
	}
	if capability != nil {
		if _, err := capability.CurrentGeneration(); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return failure("reply_cancelled_before_return", err)
	}
	return nil
}
