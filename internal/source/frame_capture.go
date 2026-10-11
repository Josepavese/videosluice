package source

import (
	"context"
	"errors"
	wire "github.com/Josepavese/videosluice/framewire"
	"os"
	"os/exec"
)

// FrameDeliveryConsumer borrows exact immutable delivery bytes synchronously.
// Implementations must not retain the slice or infer another frame identity.
type FrameDeliveryConsumer func(context.Context, wire.Header, []byte) error

// FrameCaptureFactory is a trusted provider-composition hook, not a camera
// configuration or RPC assertion. It admits a complete source generation before
// the existing mux process starts. Failure must not silently disable frame access.
type FrameCaptureFactory func(context.Context, BrokerMuxPlan) (FrameCapture, error)

// ErrFrameCaptureNotRequested is a trusted composition decision: this source
// generation has no frame-plane consumer. Encoded-only sources must not acquire
// an extra decoder or a dummy capture identity. It is NOT a provider/admission
// failure and must never replace an unavailable requested frame path.
// A factory returning it must return no owned FrameCapture.
var ErrFrameCaptureNotRequested = errors.New("frame capture not requested")

// FrameExecution is supplied by trusted platform admission and composes the
// process boundary with native SDK/driver ownership. An existing admitted
// capture can add a checked receipt without acquiring another process or
// decoder; an independent launcher can also enforce a delegated cgroup.
// Attach binds the exact executable before exec. Release runs
// after the sole process owner's Wait and succeeds ONLY with a native-resource
// release receipt. An empty cgroup alone is not a DMA release receipt.
// This object never starts, kills or reaps a process itself. A nonnil value
// returned with a construction error still owns its cleanup obligations.
type FrameExecution interface {
	Attach(*exec.Cmd) error
	Started(*os.Process) error
	Release() error
}

// FrameCapture extends the existing process runner; it never starts, waits for,
// kills or reaps that process itself. These OS-facing methods belong to the mux
// provider boundary, not the frame-domain logic. Retire is called only when Start
// has not succeeded or the process owner's Wait has returned AND Run has joined.
// Failed cleanup remains an error; the next generation must not claim release.
type FrameCapture interface {
	SourceEpoch() uint64
	Attach(*exec.Cmd) error
	Started(context.Context, *os.Process) error
	Run(context.Context, FrameDeliveryConsumer) error
	Snapshot(context.Context, string, uint64, FrameDeliveryConsumer) error
	Retire(context.Context) error
}
