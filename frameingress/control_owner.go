package ingress

import (
	"context"
	"errors"
	fd "github.com/Josepavese/videosluice/framedomain"
	peer "github.com/Josepavese/videosluice/framepeer"
)

// OwnControlDispatcher reserves the shared control plane before construction.
// bytes is a trusted provider/composition quote for all control maps, snapshots,
// maximum active/retiring grants and admitted transient/imported capabilities,
// including platform overhead. It is NOT a packet size or an untrusted setting.
// The controller authority is borrowed from an enclosing bootstrap owner whose
// lifetime must extend through this resource's WaitClosed.
//
// Channels borrow this resource across dispatch/reply and never close it when
// a connection ends: receipts and grants survive a lost ACK. Runtime shutdown
// closes this owner, prevents new borrows, drains existing operations, then joins
// all physical releases. Failed/stuck release retains the entire admitted quote.
func OwnControlDispatcher(ctx context.Context, runtime *fd.Runtime, authority ConsumerAuthority, generation string, maxGrants, bytes int) (*fd.Resource[*ControlDispatcher], error) {
	if ctx == nil || runtime == nil || authority == nil || !controlToken(generation) || maxGrants < 1 || maxGrants > 64 || bytes < 1 {
		return nil, fail("control_owner_admission", peer.ErrInvalid)
	}
	return fd.OwnRuntimeResource(ctx, runtime, fd.ResourcePlan[*ControlDispatcher]{
		Bytes: bytes,
		Create: func(ctx context.Context) (fd.Created[*ControlDispatcher], error) {
			if err := ctx.Err(); err != nil {
				return fd.Created[*ControlDispatcher]{}, err
			}
			control, err := NewGrantControl(runtime, authority, generation, maxGrants)
			if err != nil {
				return fd.Created[*ControlDispatcher]{}, err
			}
			dispatcher, err := NewControlDispatcher(control)
			if err != nil {
				control.Close()
				return fd.Created[*ControlDispatcher]{}, err
			}
			return fd.Created[*ControlDispatcher]{Value: dispatcher, Owned: true}, nil
		},
		Release: func(dispatcher *ControlDispatcher) error {
			dispatcher.Close()
			err := dispatcher.control.WaitClosed(context.Background())
			state := dispatcher.State()
			if state.ReleaseFailures != 0 || state.OwnershipUncertain {
				err = errors.Join(err, fail("control_dispatch_release", peer.ErrUnavailable))
			}
			return err
		},
	})
}
