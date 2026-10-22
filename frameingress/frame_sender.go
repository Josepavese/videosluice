package ingress

import (
	"context"
	"crypto/sha256"
	"sync"
	"sync/atomic"
	"time"

	fa "github.com/Josepavese/videosluice/frameaccess"
	fd "github.com/Josepavese/videosluice/framedomain"
	peer "github.com/Josepavese/videosluice/framepeer"
)

// FrameSender owns one globally accounted packet buffer. The transport itself
// has separate admission/ownership; this class does not quote kernel memory.
// A stalled writer holds the slot and buffer charge until it actually returns.
type FrameSender struct {
	buffer    *fd.Resource[[]byte]
	writer    peer.PacketWriter
	maxBytes  int
	maxSend   time.Duration
	operation sync.Mutex
	closed    atomic.Bool
}

func NewFrameSender(ctx context.Context, runtime *fd.Runtime, writer peer.PacketWriter, maxBytes int, maxSendNS uint64) (*FrameSender, error) {
	if ctx == nil || runtime == nil || writer == nil || maxBytes < 1 || maxBytes > 1<<28 || maxSendNS == 0 || maxSendNS > MaxFrameRequestBudgetNS {
		return nil, fail("frame_sender_admission", peer.ErrInvalid)
	}
	buffer, err := fd.OwnRuntimeResource(ctx, runtime, fd.ResourcePlan[[]byte]{Bytes: MaxFrameDataBytes,
		Create: func(ctx context.Context) (fd.Created[[]byte], error) {
			if err := ctx.Err(); err != nil {
				return fd.Created[[]byte]{}, err
			}
			return fd.Created[[]byte]{Value: make([]byte, MaxFrameDataBytes), Owned: true}, nil
		}, Release: func([]byte) error { return nil },
	})
	if err != nil {
		return nil, fail("frame_sender_buffer", err)
	}
	return &FrameSender{buffer: buffer, writer: writer, maxBytes: maxBytes, maxSend: time.Duration(maxSendNS)}, nil
}

func (s *FrameSender) Close() {
	if s != nil && !s.closed.Swap(true) {
		s.buffer.Close()
	}
}
func (s *FrameSender) WaitClosed(ctx context.Context) error {
	if s == nil {
		return peer.ErrInvalid
	}
	return s.buffer.WaitClosed(ctx)
}

// Send borrows (does not consume) one extracted result. It performs no transport
// I/O inside Visit: each packet is committed only AFTER Visit's final consumer
// generation check. Driver/source leases are not retained, only extracted output.
// One deadline covers the entire artifact, not a fresh timeout for each chunk.
// Any failure after attempting the first write retires this sender: no ambiguous
// partial transfer is silently followed by a new artifact on the same channel.
func (s *FrameSender) Send(ctx context.Context, q FrameRequest, result *AccessResult, index int) (err error) {
	if s == nil || ctx == nil || result == nil || index < 0 || index >= len(q.Regions) {
		return fail("frame_send_contract", peer.ErrInvalid)
	}
	if err = q.validate(); err != nil {
		return err
	}
	if s.closed.Load() {
		return fail("frame_send_closed", peer.ErrRetired)
	}
	if !s.operation.TryLock() {
		return fail("frame_send_busy", peer.ErrBusy)
	}
	defer s.operation.Unlock()
	if s.closed.Load() {
		return fail("frame_send_closed", peer.ErrRetired)
	}
	ctx, cancel := context.WithTimeout(ctx, s.maxSend)
	defer cancel()
	started := false
	defer func() {
		if started && err != nil {
			s.Close()
		}
	}()
	return s.buffer.Borrow(ctx, func(ctx context.Context, buffer []byte) error {
		artifact, err := result.Describe(index)
		if err != nil {
			return fail("frame_send_describe", err)
		}
		size := 0
		if artifact.State == fa.Complete {
			size, err = artifact.Proof.Core.Geometry.Bytes()
			if err != nil {
				return err
			}
		}
		manifest := FrameManifest{ID: q.ID, Fence: q.Frame.Fence, Index: index, Bytes: size, Artifact: artifact}
		if err := ValidateFrameManifest(manifest, q, s.maxBytes); err != nil {
			return err
		}
		n, err := EncodeFrameManifest(buffer, manifest)
		if err != nil {
			return fail("frame_send_manifest", err)
		}
		manifestID := sha256.Sum256(buffer[:n])
		write := func(n int) error {
			return s.writeAuthorized(ctx, result.access, buffer[:n], &started)
		}
		if err := write(n); err != nil {
			return err
		}
		for offset := 0; offset < manifest.Bytes; {
			if err := ctx.Err(); err != nil {
				return err
			}
			end := min(offset+MaxFrameDataBytes-frameDataHeaderBytes, manifest.Bytes)
			if err := result.Visit(index, func(artifact fd.Artifact, p []byte) error {
				if artifact != manifest.Artifact || len(p) != manifest.Bytes {
					return fail("frame_send_artifact_changed", fa.InvalidContract)
				}
				var err error
				n, err = EncodeFrameData(buffer, manifestID, offset, p[offset:end])
				return err
			}); err != nil {
				return fail("frame_send_chunk", err)
			}
			if err := write(n); err != nil {
				return err
			}
			offset = end
		}
		return nil
	})
}

func (s *FrameSender) writeAuthorized(ctx context.Context, access *Access, packet []byte, started *bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := access.current(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return fail("frame_send_consumer_authority_deadline", err)
	}
	generation, err := s.writer.CurrentGeneration()
	if err != nil {
		return fail("frame_send_peer_unavailable", err)
	}
	if generation != access.generation {
		return fail("frame_send_peer_mismatch", peer.ErrUnauthorized)
	}
	if err := ctx.Err(); err != nil {
		return fail("frame_send_peer_authority_deadline", err)
	}
	*started = true
	if err := s.writer.WritePacket(ctx, packet); err != nil {
		return fail("frame_send_write", err)
	}
	if err := ctx.Err(); err != nil {
		return fail("frame_send_write_deadline", err)
	}
	generation, err = s.writer.CurrentGeneration()
	if err != nil || generation != access.generation {
		return fail("frame_send_peer_changed", peer.ErrRetired)
	}
	if err := ctx.Err(); err != nil {
		return fail("frame_send_peer_confirmation_deadline", err)
	}
	if err := access.current(); err != nil {
		return err
	}
	return fail("frame_send_consumer_confirmation_deadline", ctx.Err())
}

// Only FrameSession calls this after authenticated, authorized extraction has
// returned a recognized semantic negative outcome. It never emits raw errors.
func (s *FrameSender) sendNegative(ctx context.Context, q FrameRequest, access *Access, state fa.Code) (err error) {
	if ctx == nil || access == nil || state == fa.Complete || frameStateCode(state) == 0 {
		return peer.ErrInvalid
	}
	if err = q.validate(); err != nil {
		return err
	}
	if s.closed.Load() {
		return peer.ErrRetired
	}
	if !s.operation.TryLock() {
		return peer.ErrBusy
	}
	defer s.operation.Unlock()
	if s.closed.Load() {
		return peer.ErrRetired
	}
	ctx, cancel := context.WithTimeout(ctx, s.maxSend)
	defer cancel()
	started := false
	defer func() {
		if started && err != nil {
			s.Close()
		}
	}()
	return s.buffer.Borrow(ctx, func(ctx context.Context, buffer []byte) error {
		for i, region := range q.Regions {
			manifest := FrameManifest{ID: q.ID, Fence: q.Frame.Fence, Index: i, Artifact: fd.Artifact{Region: region.ID, State: state}}
			n, err := EncodeFrameManifest(buffer, manifest)
			if err != nil {
				return err
			}
			if err = s.writeAuthorized(ctx, access, buffer[:n], &started); err != nil {
				return err
			}
		}
		return nil
	})
}
