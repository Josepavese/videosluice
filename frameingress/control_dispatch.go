package ingress

import (
	"context"
	peer "github.com/Josepavese/videosluice/framepeer"
	"sync"
)

type ControlResult struct {
	ID      [16]byte
	Kind    ControlKind
	Receipt ControlReceipt
	Stats   ControlStats
}

type ControlDispatchState struct {
	Closed             bool
	ReleaseFailures    int
	Busy               bool
	Stage              string
	OwnershipUncertain bool
}

// ControlDispatcher owns one admitted processing slot, with no input queue.
// It never writes ACKs: a committed result must survive a later send failure.
// The transport owner must bound connections/kernel resources and enforce its
// operation deadline/fail-stop policy for a provider that cannot return.
type ControlDispatcher struct {
	operation          sync.Mutex
	mu                 sync.Mutex
	control            *GrantControl
	closed             bool
	busy               bool
	stage              string
	quarantined        []interface{ Close() error }
	ownershipUncertain bool
}

func NewControlDispatcher(control *GrantControl) (*ControlDispatcher, error) {
	if control == nil {
		return nil, peer.ErrInvalid
	}
	return &ControlDispatcher{control: control, stage: "idle"}, nil
}

func (d *ControlDispatcher) State() ControlDispatchState {
	d.mu.Lock()
	defer d.mu.Unlock()
	return ControlDispatchState{Closed: d.closed, ReleaseFailures: len(d.quarantined), Busy: d.busy, Stage: d.stage, OwnershipUncertain: d.ownershipUncertain}
}

// A provider panic can leave an unreported partial acquisition. Keep the
// enclosing admission charged even if every known handle closes successfully.
// Never serialize the panic payload or guess that cleanup recovered ownership.
func (d *ControlDispatcher) uncertainOperation() {
	d.mu.Lock()
	d.ownershipUncertain = true
	d.closed = true
	d.stage = "ownership_uncertain"
	d.mu.Unlock()
	d.control.Close()
}

func (d *ControlDispatcher) phase(stage string) {
	d.mu.Lock()
	d.stage = stage
	d.mu.Unlock()
}

func (d *ControlDispatcher) Close() {
	d.mu.Lock()
	d.closed = true
	d.mu.Unlock()
	d.control.Close()
}

func (d *ControlDispatcher) release(owned interface{ Close() error }) error {
	var err error
	func() {
		defer func() {
			if recover() != nil {
				err = peer.ErrUnavailable
			}
		}()
		err = owned.Close()
	}()
	if err == nil {
		return nil
	}
	// At most the raw handle and imported capability of the one active command.
	// A failed release permanently closes admission; keep the resource charged
	// and referenced, do not retry against a potentially reused FD.
	d.mu.Lock()
	d.quarantined = append(d.quarantined, owned)
	d.closed = true
	d.stage = "release_failed"
	d.mu.Unlock()
	d.control.Close()
	return fail("control_dispatch_release", peer.ErrUnavailable)
}

// Apply takes ownership of pending ONLY if accepted=true. Invalid context,
// busy or already closed leaves ownership with the caller. Once admitted,
// every path closes the raw capability or transfers its validated owner to
// GrantControl. The caller must not close/reuse it independently afterward.
// An ACK failure after this returns must not undo a committed grant/revoke.
func (d *ControlDispatcher) Apply(ctx context.Context, principal peer.Principal, packet []byte, pending peer.PendingCapability) (result ControlResult, accepted bool, err error) {
	if d == nil {
		return result, false, peer.ErrInvalid
	}
	if ctx == nil {
		return result, false, peer.ErrInvalid
	}
	if err = ctx.Err(); err != nil {
		return result, false, err
	}
	if !d.operation.TryLock() {
		return result, false, peer.ErrBusy
	}
	defer d.operation.Unlock()
	defer func() {
		if recover() != nil {
			d.uncertainOperation()
			result = ControlResult{}
			err = fail("control_dispatch_ownership_uncertain", peer.ErrUnavailable)
		}
	}()
	d.mu.Lock()
	closed := d.closed
	d.mu.Unlock()
	if closed {
		return result, false, peer.ErrRetired
	}
	accepted = true
	d.mu.Lock()
	d.busy = true
	d.stage = "decode"
	d.mu.Unlock()
	var imported peer.ProcessCapability
	defer func() {
		if imported != nil || pending != nil {
			d.phase("release")
		}
		if imported != nil {
			if releaseErr := d.release(imported); releaseErr != nil {
				err = releaseErr
			}
		}
		if pending != nil {
			if releaseErr := d.release(pending); releaseErr != nil {
				err = releaseErr
			}
		}
		d.mu.Lock()
		d.busy = false
		if len(d.quarantined) == 0 {
			d.stage = "idle"
		}
		d.mu.Unlock()
	}()
	command, err := DecodeControl(packet)
	if err != nil {
		return result, true, err
	}
	result.ID = command.Header.ID
	result.Kind = command.Kind
	d.phase("preflight")
	if err = d.control.Preflight(principal, command); err != nil {
		return result, true, err
	}
	if command.Kind != ControlGrant && pending != nil {
		return result, true, fail("control_dispatch_unexpected_capability", peer.ErrInvalid)
	}
	if command.Kind == ControlGrant {
		if pending == nil {
			return result, true, fail("control_dispatch_missing_capability", peer.ErrInvalid)
		}
		d.phase("capability_import")
		imported, err = pending.Import(command.CapabilityType, command.CapabilityMetadata)
		if err != nil {
			return result, true, err
		}
		if imported == nil {
			return result, true, fail("control_dispatch_invalid_provider", peer.ErrUnavailable)
		}
	}
	if err = ctx.Err(); err != nil {
		return result, true, err
	}
	d.mu.Lock()
	closed = d.closed
	d.mu.Unlock()
	if closed {
		return result, true, peer.ErrRetired
	}
	d.phase("commit")
	switch command.Kind {
	case ControlGrant:
		result.Receipt, err = d.control.Grant(principal, command.Header, command.Binding, imported, command.Policy)
		if err == nil && result.Receipt.Outcome == "installed" {
			imported = nil
		}
	case ControlRevoke:
		result.Receipt, err = d.control.Revoke(principal, command.Header)
	case ControlStatus:
		result.Stats, err = d.control.Status(principal)
	}
	return result, true, err
}
