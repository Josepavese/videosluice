package ingress

import (
	"context"
	"encoding/binary"
	"errors"
	fd "github.com/Josepavese/videosluice/framedomain"
	peer "github.com/Josepavese/videosluice/framepeer"
)

// A complete bounded status snapshot can contain64 grant revisions. Its
// maximum wire size is < 10000B; transport admission must reserve16384B.
const MaxControlReplyBytes = 16384

type ControlReplyCode uint8

const (
	ReplyOK ControlReplyCode = iota
	ReplyInvalid
	ReplyUnauthorized
	ReplyRetired
	ReplySequence
	ReplyUnavailable
	ReplyBusy
	ReplyDeadline
	ReplyCanceled
	ReplyInternal
)

type ControlReply struct {
	Broker  fd.Fence
	ID      [16]byte
	Kind    ControlKind
	Code    ControlReplyCode
	Stage   string
	Receipt ControlReceipt
	Stats   *ControlStats
}

// BuildControlReply uses the server-owned broker fence, not a request-provided
// identity. A malformed request with no usable ID cannot produce a reply.
// Errors never serialize arbitrary provider messages or request content.
func BuildControlReply(broker fd.Fence, result ControlResult, cause error) (ControlReply, error) {
	r := ControlReply{Broker: broker, ID: result.ID, Kind: result.Kind, Receipt: result.Receipt}
	if cause != nil {
		switch {
		case errors.Is(cause, context.Canceled):
			r.Code = ReplyCanceled
		case errors.Is(cause, context.DeadlineExceeded), errors.Is(cause, peer.ErrDeadline):
			r.Code = ReplyDeadline
		case errors.Is(cause, peer.ErrUnauthorized):
			r.Code = ReplyUnauthorized
		case errors.Is(cause, peer.ErrRetired), errors.Is(cause, fd.ErrRetired), errors.Is(cause, fd.ErrProcessChanged):
			r.Code = ReplyRetired
		case errors.Is(cause, ErrControlSequence):
			r.Code = ReplySequence
		case errors.Is(cause, peer.ErrInvalid), errors.Is(cause, fd.ErrInvalid), errors.Is(cause, fd.ErrDomainMismatch):
			r.Code = ReplyInvalid
		case errors.Is(cause, peer.ErrBusy):
			r.Code = ReplyBusy
		case errors.Is(cause, peer.ErrUnavailable):
			r.Code = ReplyUnavailable
		default:
			r.Code = ReplyInternal
		}
		r.Stage = "control_operation_failed"
		var diagnostic *Failure
		var provider *peer.Failure
		if errors.As(cause, &diagnostic) && controlToken(diagnostic.Stage) {
			r.Stage = diagnostic.Stage
		} else if errors.As(cause, &provider) && controlToken(provider.Stage) {
			r.Stage = provider.Stage
		}
	} else if result.Kind == ControlStatus {
		stats := result.Stats
		if len(stats.Grants) > 64 {
			return ControlReply{}, fail("control_reply_limits", peer.ErrInvalid)
		}
		stats.Grants = append([]GrantRevision(nil), stats.Grants...)
		r.Stats = &stats
	}
	if err := r.validate(); err != nil {
		return ControlReply{}, err
	}
	return r, nil
}

func validReceipt(r ControlReceipt) bool {
	if r.Sequence == 0 {
		return r == (ControlReceipt{})
	}
	if r.ID == ([16]byte{}) || !controlToken(r.Grant) || !controlToken(r.Stage) {
		return false
	}
	switch r.Outcome {
	case "installed", "revoked", "absent", "conflict", "capacity", "invalid":
		return true
	}
	return false
}
func validStats(s ControlStats) bool {
	if !validReceipt(s.Receipt) || s.Active < 0 || s.Active > 64 || s.Retiring < 0 || s.Retiring > 64-s.Active || s.ReleaseFailures < 0 || s.ReleaseFailures > s.Retiring || len(s.Grants) != s.Active || s.Closed && s.Active != 0 {
		return false
	}
	for i, g := range s.Grants {
		if !controlToken(g.Name) || !controlToken(g.Consumer) || g.Sequence == 0 || g.Sequence > s.Receipt.Sequence || i > 0 && g.Name <= s.Grants[i-1].Name {
			return false
		}
	}
	return true
}
func (r ControlReply) validate() error {
	invalid := func() error { return fail("control_reply_contract", peer.ErrInvalid) }
	if r.Broker.Namespace == ([16]byte{}) || !controlToken(r.Broker.ProcessGeneration) || r.ID == ([16]byte{}) || r.Kind < ControlGrant || r.Kind > ControlStatus || r.Code > ReplyInternal {
		return invalid()
	}
	if r.Code == ReplyOK && r.Stage != "" || r.Code != ReplyOK && !controlToken(r.Stage) {
		return invalid()
	}
	if !validReceipt(r.Receipt) {
		return invalid()
	}
	if r.Kind == ControlStatus {
		if r.Receipt != (ControlReceipt{}) || r.Code == ReplyOK && r.Stats == nil {
			return invalid()
		}
		if r.Stats != nil && !validStats(*r.Stats) {
			return invalid()
		}
	} else {
		if r.Stats != nil || r.Code == ReplyOK && r.Receipt.Sequence == 0 {
			return invalid()
		}
		if r.Receipt.Sequence != 0 && (r.Receipt.ID != r.ID || r.Kind == ControlGrant && (r.Receipt.Outcome == "revoked" || r.Receipt.Outcome == "absent") || r.Kind == ControlRevoke && (r.Receipt.Outcome == "installed" || r.Receipt.Outcome == "capacity")) {
			return invalid()
		}
	}
	return nil
}

// ValidateControlReply is the client correlation gate AFTER transport sender
// authentication. The body alone never proves it came from the real broker.
func ValidateControlReply(reply ControlReply, command ControlCommand) error {
	if err := command.validate(); err != nil {
		return err
	}
	if err := reply.validate(); err != nil {
		return err
	}
	if reply.Broker != command.Header.Target || reply.ID != command.Header.ID || reply.Kind != command.Kind {
		return fail("control_reply_correlation", peer.ErrUnauthorized)
	}
	if command.Kind != ControlStatus && reply.Receipt.Sequence != 0 && (reply.Receipt.Sequence != command.Header.Sequence || reply.Receipt.Grant != command.Header.Grant) {
		return fail("control_reply_receipt", peer.ErrUnauthorized)
	}
	return nil
}

type replyWriter struct {
	data []byte
	n    int
}

func (w *replyWriter) bytes(p []byte) { w.n += copy(w.data[w.n:], p) }
func (w *replyWriter) byte(b byte)    { w.data[w.n] = b; w.n++ }
func (w *replyWriter) text(s string)  { w.byte(byte(len(s))); w.bytes([]byte(s)) }
func (w *replyWriter) u64(n uint64)   { binary.BigEndian.PutUint64(w.data[w.n:], n); w.n += 8 }
func (w *replyWriter) receipt(r ControlReceipt) {
	w.u64(r.Sequence)
	w.bytes(r.ID[:])
	w.text(r.Grant)
	w.text(r.Outcome)
	w.text(r.Stage)
}
func readControlReceipt(r *controlReader) ControlReceipt {
	receipt := ControlReceipt{Sequence: r.u64()}
	copy(receipt.ID[:], r.bytes(16))
	receipt.Grant = r.text()
	receipt.Outcome = r.text()
	receipt.Stage = r.text()
	return receipt
}
func EncodeControlReply(reply ControlReply) ([]byte, error) {
	buffer := make([]byte, MaxControlReplyBytes)
	n, err := EncodeControlReplyInto(buffer, reply)
	if err != nil {
		return nil, err
	}
	return buffer[:n], nil
}

// EncodeControlReplyInto writes into already admitted storage. Invalid input
// leaves the buffer unchanged. Full capacity avoids partial encodes or growth.
func EncodeControlReplyInto(buffer []byte, reply ControlReply) (int, error) {
	if len(buffer) < MaxControlReplyBytes {
		return 0, fail("control_reply_buffer", peer.ErrInvalid)
	}
	if err := reply.validate(); err != nil {
		return 0, err
	}
	w := replyWriter{data: buffer[:MaxControlReplyBytes], n: 8}
	w.bytes(reply.Broker.Namespace[:])
	w.text(reply.Broker.ProcessGeneration)
	w.bytes(reply.ID[:])
	w.byte(byte(reply.Code))
	w.text(reply.Stage)
	w.receipt(reply.Receipt)
	if reply.Stats == nil {
		w.byte(0)
	} else {
		w.byte(1)
		s := reply.Stats
		w.byte(byte(s.Active))
		w.byte(byte(s.Retiring))
		w.byte(byte(s.ReleaseFailures))
		if s.Closed {
			w.byte(1)
		} else {
			w.byte(0)
		}
		w.receipt(s.Receipt)
		for _, g := range s.Grants {
			w.text(g.Name)
			w.text(g.Consumer)
			w.u64(g.Sequence)
		}
	}
	copy(w.data[:4], "FAPR")
	w.data[4] = 1
	w.data[5] = byte(reply.Kind)
	binary.BigEndian.PutUint16(w.data[6:8], uint16(w.n))
	return w.n, nil
}
func DecodeControlReply(data []byte) (ControlReply, error) {
	invalid := func() (ControlReply, error) { return ControlReply{}, fail("control_reply_wire", peer.ErrInvalid) }
	if len(data) < 8 || len(data) > MaxControlReplyBytes || string(data[:4]) != "FAPR" || data[4] != 1 || int(binary.BigEndian.Uint16(data[6:8])) != len(data) {
		return invalid()
	}
	r := controlReader{data: data, n: 8}
	reply := ControlReply{Kind: ControlKind(data[5])}
	copy(reply.Broker.Namespace[:], r.bytes(16))
	reply.Broker.ProcessGeneration = r.text()
	copy(reply.ID[:], r.bytes(16))
	reply.Code = ControlReplyCode(r.byte())
	reply.Stage = r.text()
	reply.Receipt = readControlReceipt(&r)
	present := r.byte()
	if present > 1 {
		return invalid()
	}
	if present == 1 {
		s := &ControlStats{Active: int(r.byte()), Retiring: int(r.byte()), ReleaseFailures: int(r.byte())}
		closed := r.byte()
		if closed > 1 || s.Active > 64 {
			return invalid()
		}
		s.Closed = closed == 1
		s.Receipt = readControlReceipt(&r)
		s.Grants = make([]GrantRevision, s.Active)
		for i := range s.Grants {
			s.Grants[i] = GrantRevision{Name: r.text(), Consumer: r.text(), Sequence: r.u64()}
		}
		reply.Stats = s
	}
	if r.bad || r.n != len(data) {
		return invalid()
	}
	if err := reply.validate(); err != nil {
		return ControlReply{}, err
	}
	return reply, nil
}
