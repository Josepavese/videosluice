//go:build linux

package main

import (
	"context"
	"errors"
	"fmt"
	fa "github.com/Josepavese/videosluice/frameaccess"
	fd "github.com/Josepavese/videosluice/framedomain"
	"github.com/Josepavese/videosluice/framedomain/entropy"
	"github.com/Josepavese/videosluice/framedomain/monotime"
	in "github.com/Josepavese/videosluice/frameingress"
	peer "github.com/Josepavese/videosluice/framepeer"
	linux "github.com/Josepavese/videosluice/framepeer/provider/linux"
	"github.com/Josepavese/videosluice/internal/source"
	"log"
	"os"
	"sync"
	"time"
)

// The product composition must obtain this admission from a qualified native
// provider. CLI arguments/configuration cannot assert a native upper bound.
type nativeFrameAdmission struct {
	limits                                                  fd.Limits
	bytes                                                   int
	controlBytes, workspaceBytes, transportBytes, maxGrants int
	source                                                  func(context.Context, source.BrokerMuxPlan) (nativeFrameSourceAdmission, error)
	readers                                                 []nativeFrameReaderPlan
}

type nativeFrameBootstrap struct {
	runtime          *fd.Runtime
	resource         *fd.Resource[*linux.AcceptedLaunch]
	once             sync.Once
	closeErr         error
	admission        nativeFrameAdmission
	run              sync.Mutex
	started          bool
	configurationSet bool
	clock            fa.Clock
	capturesMu       sync.Mutex
	captures         map[string]*nativeFrameCapture
}

func nativeFrameOpener(admission nativeFrameAdmission) frameBootstrapOpener {
	return func(ctx context.Context, input frameLaunchInput) (frameBootstrapOwner, error) {
		clock := monotime.New()
		r, resource, err := fd.NewFromBootstrap(ctx, entropy.System{}, clock, admission.limits,
			fd.BootstrapPlan[*linux.AcceptedLaunch]{Bytes: admission.bytes,
				Create: func(ctx context.Context) (fd.Created[*linux.AcceptedLaunch], fd.ProcessAuthority, error) {
					launch, err := linux.AcceptLaunch(ctx, os.NewFile(uintptr(input.SocketFD), "frame-launch"), os.NewFile(uintptr(input.ControllerFD), "frame-controller"), input.ControllerMetadata, time.Second)
					var authority fd.ProcessAuthority
					if launch != nil {
						authority = launch.Self
					}
					return fd.Created[*linux.AcceptedLaunch]{Value: launch, Owned: launch != nil}, authority, err
				}, Release: func(launch *linux.AcceptedLaunch) error { return launch.Close() },
			})
		if r == nil {
			return nil, err
		}
		return &nativeFrameBootstrap{runtime: r, resource: resource, admission: admission, clock: clock}, err
	}
}

// This wrapper borrows the native channel; AcceptedLaunch remains its sole
// physical owner. Its separately admitted quote is wrapper/session overhead,
// while the enclosing bootstrap quote continues to cover the native transport.
type launchControlTransport struct {
	peer.ControlServerConnection
	owner *linux.AcceptedLaunch
}

func (c *launchControlTransport) Close() error { return c.owner.CloseControl() }

func (b *nativeFrameBootstrap) Run(ctx context.Context) error {
	if b == nil || ctx == nil || b.resource == nil {
		return peer.ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !b.run.TryLock() {
		return peer.ErrBusy
	}
	defer b.run.Unlock()
	if b.started {
		return peer.ErrRetired
	}
	b.started = true
	a := b.admission
	if a.controlBytes < 1 || a.workspaceBytes < 1 || a.transportBytes < 1 || a.maxGrants < 1 || a.maxGrants > 64 {
		return errFrameProviderUnavailable
	}
	// This Borrow outlives every dependent resource, including a stuck release.
	// Close may report its bounded wait unconfirmed, but cannot free the leases
	// underneath this operation or fake native-resource reclamation.
	return b.resource.Borrow(ctx, func(ctx context.Context, launch *linux.AcceptedLaunch) (result error) {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		generation, err := launch.Controller.CurrentGeneration()
		if err != nil {
			return err
		}
		control, err := in.OwnControlDispatcher(ctx, b.runtime, launch.Controller, generation, a.maxGrants, a.controlBytes)
		if err != nil {
			return err
		}
		defer func() { control.Close(); result = errors.Join(result, control.WaitClosed(context.Background())) }()
		workspace, err := fd.OwnRuntimeResource(ctx, b.runtime, fd.ResourcePlan[*in.DiscoveryWorkspace]{Bytes: a.workspaceBytes,
			Create: func(context.Context) (fd.Created[*in.DiscoveryWorkspace], error) {
				return fd.Created[*in.DiscoveryWorkspace]{Value: &in.DiscoveryWorkspace{}, Owned: true}, nil
			}, Release: func(*in.DiscoveryWorkspace) error { return nil }})
		if err != nil {
			return err
		}
		defer func() { workspace.Close(); result = errors.Join(result, workspace.WaitClosed(context.Background())) }()
		transport, err := fd.OwnRuntimeResource(ctx, b.runtime, fd.ResourcePlan[peer.ControlServerConnection]{Bytes: a.transportBytes,
			Create: func(context.Context) (fd.Created[peer.ControlServerConnection], error) {
				return fd.Created[peer.ControlServerConnection]{Value: &launchControlTransport{ControlServerConnection: launch.Control, owner: launch}, Owned: true}, nil
			},
			Release: func(c peer.ControlServerConnection) error { return c.Close() }})
		if err != nil {
			return err
		}
		defer func() { transport.Close(); result = errors.Join(result, transport.WaitClosed(context.Background())) }()
		session, err := in.NewControlSession(ctx, b.runtime, control, transport, workspace, uint64(time.Second))
		if err != nil {
			return err
		}
		defer func() { session.Close(); result = errors.Join(result, session.WaitClosed(context.Background())) }()
		readerReady, readerDone := make(chan struct{}), make(chan error, 1)
		go func() {
			readerDone <- b.serveFrameReaders(ctx, control, readerReady)
			cancel()
		}()
		defer func() { cancel(); result = errors.Join(result, <-readerDone) }()
		select {
		case <-readerReady:
		case <-ctx.Done():
			return ctx.Err()
		}
		if err := transport.Borrow(ctx, func(ctx context.Context, c peer.ControlServerConnection) error {
			return in.SendBootstrapReady(ctx, c, generation)
		}); err != nil {
			return fmt.Errorf("frame startup stage=ready_send: %w", err)
		}
		log.Printf("[video-broker] frame_control stage=ready")
		for {
			if err := ctx.Err(); err != nil {
				return err
			}
			_, err := session.Step(ctx)
			if err != nil {
				stats := session.Stats()
				log.Printf("[video-broker] frame_control stage=stopped last_stage=%s received=%d completed=%d negative=%d uncertain=%d", stats.LastStage, stats.Received, stats.Completed, stats.Negative, stats.Uncertain)
				return err
			}
		}
	})
}

func (b *nativeFrameBootstrap) Close() error {
	if b == nil {
		return nil
	}
	b.once.Do(func() {
		b.runtime.Revoke()
		if b.resource != nil {
			b.resource.Close()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			b.closeErr = b.resource.WaitClosed(ctx)
		} else if b.runtime.Stats().LiveResources != 0 {
			// Failed construction retains its quarantined/closing owner inside
			// the revoked Runtime. Do not claim release or poll/retry descriptor close.
			b.closeErr = errors.New("frame bootstrap cleanup unconfirmed")
		}
		if b.runtime.Stats().LiveResources != 0 {
			b.closeErr = errors.Join(b.closeErr, errors.New("frame dependent resource cleanup unconfirmed"))
		}
	})
	return b.closeErr
}
