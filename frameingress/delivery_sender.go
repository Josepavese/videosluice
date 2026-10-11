package ingress

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"time"

	peer "github.com/Josepavese/videosluice/framepeer"
	wire "github.com/Josepavese/videosluice/framewire"
)

// A delivery channel is granted to one existing publication. Requests cannot
// select a source or acquire a new publication after restart/switch. FAPN is a
// 24-byte next request: version/reserved, publication epoch, last delivery ID.
// Only the first request uses epoch=0,last=0; its first reply is FAPB metadata
// from the admitted binding, followed by VBCF/FAPD or explicit FAPI idle.
const DeliveryRequestBytes = 24
const PublicationMetadataBytes = 512

func deliveryRequest(packet []byte, publication *DeliveryPublication, first bool) (uint64, error) {
	if len(packet) != DeliveryRequestBytes || string(packet[:8]) != "FAPN\x01\x00\x00\x00" {
		return 0, fail("delivery_request_record", peer.ErrRecord)
	}
	epoch, last := binary.BigEndian.Uint64(packet[8:16]), binary.BigEndian.Uint64(packet[16:24])
	if first {
		if epoch != 0 || last != 0 {
			return 0, fail("delivery_request_initial", peer.ErrInvalid)
		}
	} else if epoch != publication.scope.Streams[publication.stream] {
		return 0, fail("delivery_request_epoch", peer.ErrUnauthorized)
	}
	return last, nil
}

func (a *Access) authorizePublication(p *DeliveryPublication) error {
	if err := a.current(); err != nil {
		return err
	}
	if p == nil || p.binding == nil || a.runtime.Fence() != p.fence || a.fence != p.fence ||
		a.source != p.scope.Source || a.surface != p.scope.Surface || a.graph != p.scope.Graph ||
		a.streams[p.stream] != p.scope.Streams[p.stream] {
		return fail("delivery_grant_publication", peer.ErrUnauthorized)
	}
	return p.binding.ValidateCurrent()
}

// encodeMetadata uses the sender's existing bounded buffer, with exact uint64
// identities and no JSON/float conversion or per-frame metadata allocation.
func (p *DeliveryPublication) encodeMetadata(out []byte) (int, error) {
	if len(out) < PublicationMetadataBytes {
		return 0, peer.ErrInvalid
	}
	clear(out[:PublicationMetadataBytes])
	copy(out, "FAPB\x01\x00")
	copy(out[8:24], p.fence.Namespace[:])
	binary.BigEndian.PutUint64(out[24:32], p.scope.MediaEpoch)
	binary.BigEndian.PutUint64(out[32:40], p.scope.Streams[p.stream])
	copy(out[40:72], p.scope.Graph[:])
	t := p.layout.Transform
	mirror := 0
	if t.Mirror {
		mirror = 1
	}
	values := [...]int{t.Full.Width, t.Full.Height, t.Full.Stride, t.Crop.X1, t.Crop.Y1,
		t.Crop.X2, t.Crop.Y2, t.Rotation, mirror, t.ResizedWidth, t.ResizedHeight,
		t.DeliveryWidth, t.DeliveryHeight, t.PadLeft, t.PadTop}
	for i, v := range values {
		binary.BigEndian.PutUint32(out[72+i*4:76+i*4], uint32(v))
	}
	copy(out[132:164], p.copy.Evidence[:])
	n := 164
	for _, value := range [...]string{p.fence.ProcessGeneration, p.scope.Source, p.stream, p.scope.Surface, p.copy.Provider} {
		if !controlToken(value) {
			return 0, peer.ErrInvalid
		}
		out[n] = byte(len(value))
		n++
		n += copy(out[n:], value)
	}
	binary.BigEndian.PutUint16(out[6:8], uint16(n))
	return n, nil
}

// SendDelivery reuses the authenticated packet writer, global buffer ledger and
// total deadline of ROI delivery. One latest snapshot only; no driver lease or
// queue is held while a slow consumer receives its copy. The connection owner
// must close on any error, including a partial descriptor/frame transfer.
func (s *FrameSender) SendDelivery(ctx context.Context, access *Access, p *DeliveryPublication, request []byte, first bool) (err error) {
	if s == nil || ctx == nil || access == nil || p == nil {
		return peer.ErrInvalid
	}
	if s.closed.Load() {
		return peer.ErrRetired
	}
	last, err := deliveryRequest(request, p, first)
	if err != nil {
		return err
	}
	if err = access.authorizePublication(p); err != nil {
		return err
	}
	if int(p.layout.MaxPayload) > s.maxBytes {
		return peer.ErrInvalid
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
	progress := deliverySendProgress{phase: "workspace", step: "borrow", started: time.Now(),
		lastDelivery: last, sourceEpoch: p.scope.MediaEpoch, publication: p.scope.Streams[p.stream]}
	if deadline, ok := ctx.Deadline(); ok {
		progress.budget = max(0, deadline.Sub(progress.started))
	}
	started := false
	defer func() {
		if err != nil {
			err = progress.failure(err)
		}
		if started && err != nil {
			s.Close()
		}
	}()
	return s.buffer.Borrow(ctx, func(ctx context.Context, buffer []byte) error {
		write := func(n int) (err error) {
			start := time.Now()
			defer func() {
				elapsed := time.Since(start)
				progress.transactionTime += elapsed
				progress.maxTransactionTime = max(progress.maxTransactionTime, elapsed)
			}()
			progress.step = "pre_authorize"
			if err := access.authorizePublication(p); err != nil {
				return err
			}
			progress.step = "authorized_write"
			if err := s.writeAuthorized(ctx, access, buffer[:n], &started); err != nil {
				return err
			}
			progress.step = "post_authorize"
			if err := access.authorizePublication(p); err != nil {
				return err
			}
			progress.confirmedRecords++
			progress.confirmedWire += uint64(n)
			return nil
		}
		if first {
			progress.phase, progress.step = "metadata", "encode"
			n, err := p.encodeMetadata(buffer)
			if err != nil {
				return err
			}
			if err = write(n); err != nil {
				return err
			}
		}
		progress.phase, progress.step, progress.snapshotStarted = "snapshot", "copy", time.Now()
		err := p.Snapshot(ctx, last, func(ctx context.Context, h wire.Header, pixels []byte) error {
			progress.snapshotFinished = time.Now()
			progress.delivery, progress.origin = h.DeliveryID, h.OriginSequence
			progress.phase, progress.step = "header", "encode"
			header, err := wire.MarshalHeader(h)
			if err != nil {
				return err
			}
			copy(buffer, header[:])
			if err = write(wire.HeaderSize); err != nil {
				return err
			}
			digest := sha256.Sum256(header[:])
			for offset := 0; offset < len(pixels); {
				progress.phase, progress.step = "payload", "encode"
				end := min(offset+MaxFrameDataBytes-frameDataHeaderBytes, len(pixels))
				n, err := EncodeFrameData(buffer, digest, offset, pixels[offset:end])
				if err != nil {
					return err
				}
				if err = write(n); err != nil {
					return err
				}
				offset = end
				progress.confirmedPayload = uint64(offset)
			}
			return nil
		})
		if !errors.Is(err, ErrDeliveryIdle) {
			return err
		}
		progress.snapshotFinished = time.Now()
		progress.phase, progress.step = "idle", "encode"
		copy(buffer[:8], "FAPI\x01\x00\x00\x00")
		binary.BigEndian.PutUint64(buffer[8:16], p.scope.Streams[p.stream])
		binary.BigEndian.PutUint64(buffer[16:24], last)
		return write(DeliveryRequestBytes)
	})
}
