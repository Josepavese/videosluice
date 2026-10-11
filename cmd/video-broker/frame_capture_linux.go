//go:build linux

package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	fa "github.com/Josepavese/videosluice/frameaccess"
	fd "github.com/Josepavese/videosluice/framedomain"
	in "github.com/Josepavese/videosluice/frameingress"
	peer "github.com/Josepavese/videosluice/framepeer"
	linux "github.com/Josepavese/videosluice/framepeer/provider/linux"
	wire "github.com/Josepavese/videosluice/framewire"
	"github.com/Josepavese/videosluice/framewire/surfaceport"
	"github.com/Josepavese/videosluice/internal/framehash"
	"github.com/Josepavese/videosluice/internal/source"
)

// Selected by trusted native admission, NOT decoded from CLI/RPC booleans.
// CaptureBytes covers the incremental Frame Access native state, detached
// input copy, bounded asynchronous publisher and launch transport. It does NOT claim a total process/SDK/DMA
// ceiling: the existing source owner admits and supervises the same decoder.
type nativeFrameSourceAdmission struct {
	Layout                       fd.Layout
	Limits                       fa.Limits
	Copier                       fa.Copier
	Admission                    fd.Admission
	Policy                       fa.CopyPolicy
	SurfaceDigest                fa.Digest
	Deliveries                   map[string]in.DeliveryLayout
	DeliveryAllocator            surfaceport.Allocator
	CaptureBytes                 int
	StartupNS, ReceiveNS, CopyNS uint64
	Execution                    func(context.Context) (source.FrameExecution, error)
	MailboxBytes                 int
	OutputMailboxBytes           int
	OutputCapacity               int
	Mailbox                      func(wire.Mapping) (nativeSurfaceMailbox, error)
}

type nativeSurfaceMailbox interface {
	in.SourceSnapshot
	Inherited() (*os.File, error)
	CloseInherited() error
	ConfirmWriter() error
	Close() error
}

// The broker process owns this composition. Supervisor only supplies the
// authenticated broker bootstrap; camera workers are launched by the broker.
func (b *nativeFrameBootstrap) FrameCaptures() source.FrameCaptureFactory {
	return func(ctx context.Context, plan source.BrokerMuxPlan) (source.FrameCapture, error) {
		if b.admission.source == nil || b.resource == nil {
			return nil, errFrameProviderUnavailable
		}
		a, err := b.admission.source(ctx, plan)
		if err != nil {
			return nil, err
		}
		if a.Layout.Source != plan.SourceID || a.Layout.Surface != "source_decoded" || a.Layout.Geometry.Width != plan.CaptureWidth || a.Layout.Geometry.Height != plan.CaptureHeight || a.Layout.Geometry.Stride != plan.CaptureWidth || a.CaptureBytes < 1 {
			return nil, fmt.Errorf("frame source admitted layout differs from capture: %w", peer.ErrInvalid)
		}
		integrity := fa.PayloadIntegrity{Sum256: framehash.Sum256, WorkspaceBytes: framehash.MaxWorkspaceBytes(max(a.Limits.MaxOutputPerRequest, a.Limits.MaxFrameBytes))}
		binding, err := b.runtime.Open(ctx, a.Layout, a.Limits, a.Copier, a.Admission, a.Policy, integrity)
		if err != nil {
			return nil, err
		}
		var capture *nativeFrameCapture
		err = b.resource.Borrow(ctx, func(ctx context.Context, bootstrap *linux.AcceptedLaunch) error {
			var err error
			capture, err = prepareNativeFrameCapture(ctx, binding, bootstrap.Self, b.clock, a)
			return err
		})
		if err != nil {
			binding.Close()
			if capture == nil {
				return nil, err // do not publish a typed-nil FrameCapture
			}
			return capture, err
		}
		if err := b.registerFrameCapture(capture); err != nil {
			return capture, err
		}
		return capture, nil
	}
}

type nativeFrameCapture struct {
	binding                                             *fd.Binding
	resource                                            *fd.Resource[*nativeCaptureResources]
	ingress                                             *in.Ingress
	clock                                               fa.Clock
	admission                                           nativeFrameSourceAdmission
	mu                                                  sync.Mutex
	command                                             *exec.Cmd // borrowed from the only process owner
	attached, attempted, started, running, ran, retired bool
	receiver                                            *in.SourceOutputReceiver
	surfaces                                            *in.SourceSurfaceReceiver
	publications                                        map[string]*in.DeliveryPublication
	generation                                          string
	reaped                                              chan struct{}
	reapOnce                                            sync.Once
	retireDone                                          chan struct{}
	closeErr                                            error
	owner                                               *nativeFrameBootstrap
}

type nativeCaptureResources struct {
	launch        *linux.Launch
	execution     source.FrameExecution
	mailbox       nativeSurfaceMailbox
	output        nativeSurfaceMailbox
	mapping       wire.Mapping
	outputMapping wire.Mapping
}

func prepareNativeFrameCapture(ctx context.Context, binding *fd.Binding, controller *linux.Lease, clock fa.Clock, a nativeFrameSourceAdmission) (*nativeFrameCapture, error) {
	if binding == nil || controller == nil || clock == nil || a.Execution == nil || a.Mailbox == nil || a.MailboxBytes < 1 || a.MailboxBytes > 1<<28 || a.OutputMailboxBytes < 1 || a.OutputMailboxBytes > 1<<28 || a.OutputCapacity <= wire.HeaderSize || a.OutputCapacity > wire.HeaderSize+wire.MaxPayload || a.DeliveryAllocator == nil || a.CaptureBytes < 1 || a.CaptureBytes > 1<<28 || a.StartupNS == 0 || a.StartupNS > 5_000_000_000 || a.ReceiveNS == 0 || a.ReceiveNS > 5_000_000_000 || a.CopyNS == 0 || a.CopyNS > 5_000_000_000 {
		return nil, peer.ErrInvalid
	}
	i, err := in.New(binding, a.SurfaceDigest, a.Deliveries)
	if err != nil {
		return nil, err
	}
	var nonce [16]byte
	if _, err = rand.Read(nonce[:]); err != nil {
		return nil, err
	}
	c := &nativeFrameCapture{binding: binding, ingress: i, clock: clock, admission: a, generation: hex.EncodeToString(nonce[:]), reaped: make(chan struct{}), publications: make(map[string]*in.DeliveryPublication, len(a.Deliveries))}
	serial, err := wire.NextMappingGeneration()
	if err != nil {
		return nil, err
	}
	mapping := wire.Mapping{Namespace: binding.Fence().Namespace, Generation: serial, Capacity: uint64(wire.HeaderSize + a.Limits.MaxFrameBytes)}
	outputSerial, err := wire.NextMappingGeneration()
	if err != nil {
		return nil, err
	}
	outputMapping := wire.Mapping{Namespace: binding.Fence().Namespace, Generation: outputSerial, Capacity: uint64(a.OutputCapacity)}
	c.resource, err = fd.OwnResource(ctx, binding, fd.ResourcePlan[*nativeCaptureResources]{Bytes: a.CaptureBytes + a.MailboxBytes + a.OutputMailboxBytes,
		Create: func(ctx context.Context) (fd.Created[*nativeCaptureResources], error) {
			native := &nativeCaptureResources{mapping: mapping, outputMapping: outputMapping}
			var createErr error
			native.execution, createErr = a.Execution(ctx)
			if createErr == nil && native.execution == nil {
				createErr = errFrameProviderUnavailable
			}
			if createErr == nil {
				native.launch, createErr = linux.PrepareLaunch(controller)
			}
			if createErr == nil {
				native.mailbox, createErr = a.Mailbox(mapping)
				if createErr == nil && native.mailbox == nil {
					createErr = errFrameProviderUnavailable
				}
			}
			if createErr == nil {
				native.output, createErr = a.Mailbox(outputMapping)
				if createErr == nil && native.output == nil {
					createErr = errFrameProviderUnavailable
				}
			}
			return fd.Created[*nativeCaptureResources]{Value: native, Owned: native.execution != nil || native.launch != nil || native.mailbox != nil || native.output != nil}, createErr
		}, Release: func(native *nativeCaptureResources) error {
			// Domain retirement alone is not proof that the camera stopped. The
			// process owner's reaping receipt must precede physical resource release.
			<-c.reaped
			var releaseErr error
			if native.execution != nil {
				releaseErr = native.execution.Release()
			}
			if native.launch != nil {
				releaseErr = errors.Join(releaseErr, native.launch.Close())
			}
			if native.mailbox != nil {
				releaseErr = errors.Join(releaseErr, native.mailbox.Close())
			}
			if native.output != nil {
				releaseErr = errors.Join(releaseErr, native.output.Close())
			}
			return releaseErr
		}})
	if err != nil {
		c.reapOnce.Do(func() { close(c.reaped) }) // no Start was possible
		binding.Close()
		return nil, err
	}
	streams := make([]string, 0, len(a.Deliveries))
	for stream := range a.Deliveries {
		streams = append(streams, stream)
	}
	sort.Strings(streams)
	for _, stream := range streams {
		publication, err := in.NewDeliveryPublication(ctx, binding, stream, a.Deliveries[stream], a.DeliveryAllocator)
		if publication != nil {
			c.publications[stream] = publication
		}
		if err != nil {
			return c, fmt.Errorf("frame source publication stream=%s: %w", stream, err)
		}
	}
	return c, nil
}

func (c *nativeFrameCapture) SourceEpoch() uint64 { return c.binding.Scope().MediaEpoch }

func (c *nativeFrameCapture) Attach(cmd *exec.Cmd) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if cmd == nil || cmd.Process != nil || c.attached || c.retired {
		return peer.ErrInvalid
	}
	if len(c.admission.Layout.Streams) != 1 {
		return peer.ErrInvalid
	}
	for _, arg := range cmd.Args {
		if strings.HasPrefix(arg, "--frame-launch-") || strings.HasPrefix(arg, "--frame-output-") || strings.HasPrefix(arg, "--frame-native-mailbox-") {
			return fmt.Errorf("frame bootstrap arguments already present: %w", peer.ErrInvalid)
		}
	}
	// Source identity must come from this binding and already be reflected in
	// the mux plan, not be silently overwritten after command construction.
	expected := map[string]string{"--source-id": c.binding.Scope().Source, "--media-epoch": strconv.FormatUint(c.SourceEpoch(), 10)}
	seen := make(map[string]bool, 2)
	for index, arg := range cmd.Args {
		if value, ok := expected[arg]; ok {
			if seen[arg] || index+1 >= len(cmd.Args) || cmd.Args[index+1] != value {
				return peer.ErrInvalid
			}
			seen[arg] = true
		}
		if strings.HasPrefix(arg, "--source-id=") || strings.HasPrefix(arg, "--media-epoch=") {
			return peer.ErrInvalid
		}
	}
	if len(seen) != len(expected) {
		return peer.ErrInvalid
	}
	err := c.resource.Borrow(context.Background(), func(_ context.Context, native *nativeCaptureResources) error {
		if err := native.execution.Attach(cmd); err != nil {
			return err
		}
		launch := native.launch
		files, metadata, err := launch.Inherited()
		if err != nil {
			return err
		}
		first := 3 + len(cmd.ExtraFiles)
		if first > 999996 {
			return peer.ErrInvalid
		}
		mailbox, err := native.mailbox.Inherited()
		if err != nil {
			return err
		}
		cmd.ExtraFiles = append(cmd.ExtraFiles, files[:]...)
		cmd.ExtraFiles = append(cmd.ExtraFiles, mailbox)
		output, err := native.output.Inherited()
		if err != nil {
			return err
		}
		cmd.ExtraFiles = append(cmd.ExtraFiles, output)
		cmd.Args = append(cmd.Args, "--frame-launch-socket-fd", strconv.Itoa(first), "--frame-launch-controller-fd", strconv.Itoa(first+1), "--frame-launch-controller-metadata", hex.EncodeToString(metadata))
		cmd.Args = append(cmd.Args, "--frame-output-stream", c.admission.Layout.Streams[0])
		cmd.Args = append(cmd.Args, "--frame-native-mailbox-fd", strconv.Itoa(first+2), "--frame-native-mailbox-generation", strconv.FormatUint(native.mapping.Generation, 10))
		cmd.Args = append(cmd.Args, "--frame-output-mailbox-fd", strconv.Itoa(first+3), "--frame-output-mailbox-generation", strconv.FormatUint(native.outputMapping.Generation, 10), "--frame-output-mailbox-capacity", strconv.Itoa(c.admission.OutputCapacity))
		return nil
	})
	if err == nil {
		c.attached = true
		c.command = cmd
	}
	return err
}

func (c *nativeFrameCapture) Started(ctx context.Context, process *os.Process) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if ctx == nil || process == nil || c.command == nil || c.command.Process != process || !c.attached || c.attempted || c.retired {
		return peer.ErrInvalid
	}
	c.attempted = true // a lost bootstrap response must never be replayed
	// Process loading/authentication is not a frame receive operation. Use one
	// absolute startup deadline; SourceReceiver keeps its independent per-frame
	// deadline, including on this same transport after the handshake succeeds.
	ctx, cancel := context.WithTimeout(ctx, time.Duration(c.admission.StartupNS))
	defer cancel()
	started := time.Now()
	log.Printf("[video-broker] frame_source stage=bootstrap_begin source=%s source_epoch=%d startup_budget_ms=%d frame_budget_ms=%d", c.binding.Scope().Source, c.SourceEpoch(), c.admission.StartupNS/1_000_000, c.admission.ReceiveNS/1_000_000)
	uid, gid := uint32(os.Getuid()), uint32(os.Getgid())
	if c.command.SysProcAttr != nil && c.command.SysProcAttr.Credential != nil {
		uid, gid = c.command.SysProcAttr.Credential.Uid, c.command.SysProcAttr.Credential.Gid
	}
	return c.resource.Borrow(ctx, func(ctx context.Context, native *nativeCaptureResources) error {
		if err := native.execution.Started(process); err != nil {
			return err
		}
		launch := native.launch
		if err := launch.CloseInherited(); err != nil {
			return err
		}
		if err := native.mailbox.CloseInherited(); err != nil {
			return err
		}
		if err := native.output.CloseInherited(); err != nil {
			return err
		}
		endpoint, err := launch.Deliver(ctx, process, c.generation, uid, gid, time.Duration(c.admission.StartupNS))
		if err != nil {
			return err
		}
		payloadBytes := c.admission.Limits.MaxFrameBytes
		for _, delivery := range c.admission.Deliveries {
			payloadBytes = max(payloadBytes, int(delivery.MaxPayload))
		}
		r, err := in.NewSourceOutputReceiver(ctx, c.ingress, endpoint, c.generation, c.admission.ReceiveNS, framehash.Sum256, framehash.WorkspaceBytes(payloadBytes))
		if err != nil {
			return err
		}
		c.receiver = r
		descriptor, err := c.ingress.SourceDescriptor()
		if err == nil {
			err = endpoint.WriteCommand(ctx, descriptor[:], nil)
		}
		if err != nil {
			return err
		}
		// One authenticated startup marker proves the exact launched writer has
		// mapped and sealed its allocation. It is never a per-frame ACK.
		var ready [8]byte
		n, principal, err := endpoint.Receive(ctx, ready[:])
		if err != nil {
			return fmt.Errorf("source mailbox ready: %w", err)
		}
		if n != len(ready) || principal.Generation != c.generation || !bytes.Equal(ready[:], []byte{'V', 'B', 'N', 'S', 2, 2, 2, 0}) {
			return peer.ErrUnauthorized
		}
		if err := native.mailbox.ConfirmWriter(); err != nil {
			return err
		}
		if err := native.output.ConfirmWriter(); err != nil {
			return err
		}
		c.surfaces, err = in.NewSourceSurfaceReceiver(ctx, c.ingress, endpoint, c.generation, c.clock, c.admission.CopyNS)
		if err != nil {
			return err
		}
		c.started = true
		log.Printf("[video-broker] frame_source stage=bound source=%s source_epoch=%d width=%d height=%d startup_elapsed_ms=%d", c.binding.Scope().Source, c.SourceEpoch(), c.admission.Layout.Geometry.Width, c.admission.Layout.Geometry.Height, time.Since(started).Milliseconds())
		return nil
	})
}

func (c *nativeFrameCapture) Run(ctx context.Context, observe source.FrameDeliveryConsumer) error {
	c.mu.Lock()
	if ctx == nil || observe == nil || !c.started || c.ran || c.retired {
		c.mu.Unlock()
		return peer.ErrInvalid
	}
	c.running, c.ran = true, true
	c.mu.Unlock()
	defer func() { c.mu.Lock(); c.running = false; c.mu.Unlock() }()
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(context.Canceled)
	// ONE independent pump per admitted source. An outstanding provider call
	// retains both resource borrows after cancellation; no timeout frees memory
	// still in use, nor can a slow snapshot block analysis receive or shutdown.
	go func() {
		err := c.resource.Borrow(ctx, func(ctx context.Context, native *nativeCaptureResources) error {
			timer := time.NewTicker(10 * time.Millisecond)
			defer timer.Stop()
			for {
				if err := c.surfaces.Poll(ctx, native.mailbox); err != nil {
					return err
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-timer.C:
				}
			}
		})
		if err != nil && ctx.Err() == nil {
			log.Printf("[video-broker] frame_source stage=surface_pump_failed source=%s source_epoch=%d error=%v", c.binding.Scope().Source, c.SourceEpoch(), err)
			cancel(err)
		}
	}()
	return c.resource.Borrow(ctx, func(ctx context.Context, native *nativeCaptureResources) error {
		timer := time.NewTicker(10 * time.Millisecond)
		defer timer.Stop()
		for {
			if err := ctx.Err(); err != nil {
				return context.Cause(ctx)
			}
			before := c.receiver.Stats()
			err := c.receiver.Poll(ctx, native.output, func(ctx context.Context, header wire.Header, pixels []byte) error {
				publication := c.publications[header.Stream]
				if publication == nil {
					return peer.ErrUnauthorized
				}
				return publishObservedDelivery(ctx, header, pixels, publication.Publish, observe)
			})
			stats := c.receiver.Stats()
			if stats.Drops > before.Drops && (stats.Drops == 1 || stats.Drops&(stats.Drops-1) == 0) {
				log.Printf("[video-broker] frame_source stage=verified_frame_dropped source=%s source_epoch=%d drops=%d reason=complete_snapshot_budget_or_store_pressure action=continue_next_frame err=%v", c.binding.Scope().Source, c.SourceEpoch(), stats.Drops, err)
			}
			if stats.Superseded > before.Superseded && (stats.Superseded == 1 || stats.Superseded&(stats.Superseded-1) == 0) {
				log.Printf("[video-broker] frame_source stage=output_snapshot_superseded source=%s source_epoch=%d superseded=%d reason=older_verified_slot_after_newer_output action=continue_latest_snapshot", c.binding.Scope().Source, c.SourceEpoch(), stats.Superseded)
			}
			if err != nil {
				if ctx.Err() != nil {
					return context.Cause(ctx)
				}
				if !errors.Is(err, in.ErrOutputPollBudget) {
					return err
				}
				if current := c.binding.ValidateCurrent(); current != nil {
					return current
				}
				log.Printf("[video-broker] frame_source stage=output_poll_deadline source=%s source_epoch=%d action=continue_latest_snapshot err=%v", c.binding.Scope().Source, c.SourceEpoch(), err)
			}
			select {
			case <-ctx.Done():
				return context.Cause(ctx)
			case <-timer.C:
			}
		}
	})
}

func (c *nativeFrameCapture) Snapshot(ctx context.Context, stream string, last uint64, consume source.FrameDeliveryConsumer) error {
	c.mu.Lock()
	publication, retired := c.publications[stream], c.retired
	c.mu.Unlock()
	if retired {
		return peer.ErrRetired
	}
	if publication == nil {
		return peer.ErrUnauthorized
	}
	return publication.Snapshot(ctx, last, consume)
}

func (c *nativeFrameCapture) Retire(ctx context.Context) error {
	if c == nil || ctx == nil {
		return peer.ErrInvalid
	}
	c.mu.Lock()
	if c.running {
		c.mu.Unlock()
		return fmt.Errorf("frame receive has not joined: %w", peer.ErrBusy)
	}
	// ProcessState is assigned only by the owner after Cmd.Wait; PID liveness
	// tests alone cannot establish that its wait/release obligations completed.
	if c.command != nil && c.command.Process != nil && c.command.ProcessState == nil {
		c.mu.Unlock()
		return fmt.Errorf("capture process not reaped: %w", peer.ErrBusy)
	}
	if !c.retired {
		c.retired = true
		c.retireDone = make(chan struct{})
		go c.finishRetire()
	}
	done := c.retireDone
	c.mu.Unlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.closeErr
	}
}

// Retirement is one owned operation. A caller's deadline bounds only its wait:
// the resource release continues, and later callers observe that same result.
// No second SDK release or unverified generation is started after a timeout.
func (c *nativeFrameCapture) finishRetire() {
	var closeErr error
	if c.owner != nil {
		c.owner.unregisterFrameCapture(c)
	}
	c.reapOnce.Do(func() { close(c.reaped) })
	if c.receiver != nil {
		c.receiver.Close()
		closeErr = c.receiver.WaitClosed(context.Background())
		stats := c.receiver.Stats()
		log.Printf("[video-broker] frame_source stage=retired source=%s source_epoch=%d outputs=%d drops=%d superseded=%d failures=%d transport=immutable_ram_mailbox", c.binding.Scope().Source, c.SourceEpoch(), stats.Outputs, stats.Drops, stats.Superseded, stats.Failures)
	}
	if c.surfaces != nil {
		c.surfaces.Close()
		closeErr = errors.Join(closeErr, c.surfaces.WaitClosed(context.Background()))
		stats := c.surfaces.Stats()
		log.Printf("[video-broker] frame_source stage=native_surfaces_retired source=%s source_epoch=%d staged_frames=%d slot_busy_or_unchanged=%d drops=%d integrity=verify_before_pixel_extraction", c.binding.Scope().Source, c.SourceEpoch(), stats.StagedFrames, stats.Busy, stats.Drops)
	}
	for _, publication := range c.publications {
		publication.Close()
	}
	for _, publication := range c.publications {
		closeErr = errors.Join(closeErr, publication.WaitClosed(context.Background()))
	}
	if c.resource != nil {
		c.resource.Close()
		closeErr = errors.Join(closeErr, c.resource.WaitClosed(context.Background()))
	}
	if c.binding != nil {
		c.binding.Close()
	}
	c.mu.Lock()
	c.closeErr = closeErr
	close(c.retireDone)
	c.mu.Unlock()
}
