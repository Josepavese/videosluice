package ingress

import (
	"context"
	fd "github.com/Josepavese/videosluice/framedomain"
	peer "github.com/Josepavese/videosluice/framepeer"
)

// Discover shares the dispatcher slot and its failed-release quarantine with
// mutation operations. On accepted=true every pending handle is consumed;
// discovery NEVER imports it or turns it into a grant. Caller owns admitted
// response slots and packet storage through reply transmission.
func (d *ControlDispatcher) Discover(ctx context.Context, principal peer.Principal, packet []byte, storage []fd.BindingRef, pending peer.PendingCapability) (catalog DiscoveryCatalog, accepted bool, err error) {
	if d == nil || ctx == nil {
		return catalog, false, peer.ErrInvalid
	}
	if err = ctx.Err(); err != nil {
		return catalog, false, err
	}
	if !d.operation.TryLock() {
		return catalog, false, peer.ErrBusy
	}
	defer d.operation.Unlock()
	defer func() {
		if recover() != nil {
			d.uncertainOperation()
			clear(storage[:min(len(storage), fd.MaxBindingReferences)])
			catalog = DiscoveryCatalog{}
			err = fail("discovery_dispatch_ownership_uncertain", peer.ErrUnavailable)
		}
	}()
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return catalog, false, peer.ErrRetired
	}
	d.busy = true
	d.stage = "discovery"
	d.mu.Unlock()
	accepted = true
	defer func() {
		if pending != nil {
			d.phase("release")
			if releaseErr := d.release(pending); releaseErr != nil {
				err = releaseErr
			}
		}
		d.mu.Lock()
		closed := d.closed
		d.busy = false
		if len(d.quarantined) == 0 {
			d.stage = "idle"
		}
		d.mu.Unlock()
		if err == nil && closed {
			err = peer.ErrRetired
		}
		if err == nil {
			err = ctx.Err()
		}
		if err != nil {
			clear(storage[:min(len(storage), fd.MaxBindingReferences)])
			catalog = DiscoveryCatalog{}
		}
	}()
	if pending != nil {
		return catalog, true, fail("discovery_unexpected_capability", peer.ErrInvalid)
	}
	catalog, err = BuildDiscoveryCatalog(d.control, principal, packet, storage)
	return catalog, true, err
}
