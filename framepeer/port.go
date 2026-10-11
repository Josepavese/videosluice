// Package framepeer defines authenticated record transport, not source ACLs.
// A Principal is produced by a trusted provider, never decoded from a request.
package framepeer

import (
	"context"
	"errors"
)

var (
	// ErrNoRecord accompanies a deadline/cancellation ONLY when the provider can prove no
	// record/payload/control bytes were consumed. It is not permission to resume
	// after a partial frame; that decision belongs to the framing owner.
	ErrNoRecord     = errors.New("peer deadline consumed no record")
	ErrUnavailable  = errors.New("peer authentication unavailable")
	ErrUnauthorized = errors.New("record sender is not the authorized process")
	ErrRetired      = errors.New("authorized process generation retired")
	ErrInvalid      = errors.New("invalid peer transport contract")
	ErrBusy         = errors.New("peer operation already active")
	ErrRecord       = errors.New("invalid or truncated peer record")
	ErrDeadline     = errors.New("peer receive deadline exceeded")
)

// Failure carries a bounded stage without credentials or record contents.
type Failure struct {
	Stage string
	Cause error
}

func (f *Failure) Error() string { return "peer failure stage=" + f.Stage }
func (f *Failure) Unwrap() error { return f.Cause }

type Principal struct{ Generation string }

// ProcessCapability is an owned, already validated execution handle. Closing
// it releases that handle, never terminates the referenced process.
type ProcessCapability interface {
	CurrentGeneration() (string, error)
	Close() error
}

// PendingCapability owns an admitted raw handle received from authenticated
// control transport. Import transfers ownership only on success. On failure
// the raw handle remains owned here and must be closed; no provider fallback.
// Neither metadata nor the interface itself authenticates the message sender.
type PendingCapability interface {
	Import(kind string, metadata []byte) (ProcessCapability, error)
	Close() error
}

// Receiver owns its transport. Receive borrows an exclusively owned, admitted
// buffer. An error returns zero usable bytes. It never accepts FDs as input.
// The caller must apply source/operation ACLs before executing a request;
// authentication alone does not authorize any frame. Close does not kill peers.
type Receiver interface {
	Receive(context.Context, []byte) (int, Principal, error)
	Close() error
}

// PacketWriter borrows an admitted packet synchronously. It must enforce the
// bound peer execution, total caller deadline/cancellation and record size.
// Returning an error does not prove non-delivery. No retained packet or queue.
type PacketWriter interface {
	CurrentGeneration() (string, error)
	WritePacket(context.Context, []byte) error
}

// Connection is a single owned bidirectional frame channel; receiving never
// accepts capabilities. Its physical close has one owner, not one per direction.
type Connection interface {
	Receiver
	PacketWriter
}

// ControlConnection is the controller endpoint: it can send one already
// admitted execution capability and receives only authenticated, FD-free replies.
// WriteCommand borrows capability (nil for capability-free commands) and packet
// until return. It never closes/transfers the caller's original handle, queues
// or retries. An error does not establish non-delivery, including for handles.
type ControlConnection interface {
	Receiver
	CurrentGeneration() (string, error)
	WriteCommand(context.Context, []byte, ProcessCapability) error
}

// ControlServerConnection is the broker endpoint of an admitted controller
// channel. ReceiveCommand transfers zero or one pending capability; if nonnil
// it is caller-owned EVEN on error and must be released or passed to dispatch.
// Provider admission includes transient handle capacity before receiving.
// Reply never carries capabilities, queues packets or retries uncertain writes.
type ControlServerConnection interface {
	CurrentGeneration() (string, error)
	ReceiveCommand(context.Context, []byte) (int, Principal, PendingCapability, error)
	Reply(context.Context, []byte) error
	Close() error
}
