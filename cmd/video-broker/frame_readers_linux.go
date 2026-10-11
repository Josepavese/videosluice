//go:build linux

package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"path/filepath"
	"strings"
	"time"

	in "github.com/Josepavese/videosluice/frameingress"
	fd "github.com/Josepavese/videosluice/framedomain"
	peer "github.com/Josepavese/videosluice/framepeer"
	"github.com/Josepavese/videosluice/internal/unixsocket"
)

// Trusted native composition, not consumer RPC/config assertions. Source is
// deliberately absent: each connection resolves the controller's exact grant,
// including changes of physical source, graph and publication after a switch.
type nativeFrameReaderPlan struct {
	Path, Grant, Stream                  string
	Delivery                             bool
	ListenerBytes, AcceptBytes, MaxBytes int
	MaxWorkNS                            uint64
}

func frameReaderToken(s string) bool {
	if len(s) < 1 || len(s) > 64 {
		return false
	}
	for _, c := range []byte(s) {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '.' || c == '-') {
			return false
		}
	}
	return true
}

func (p nativeFrameReaderPlan) validate() error {
	if !filepath.IsAbs(p.Path) || filepath.Clean(p.Path) != p.Path || len(p.Path) > 107 || strings.IndexByte(p.Path, 0) >= 0 ||
		!frameReaderToken(p.Grant) || p.Delivery && !frameReaderToken(p.Stream) || !p.Delivery && p.Stream != "" ||
		p.ListenerBytes < 1 || p.AcceptBytes < 1 || p.MaxBytes < 1 || p.MaxBytes > 1<<28 || p.MaxWorkNS == 0 || p.MaxWorkNS > in.MaxFrameRequestBudgetNS {
		return peer.ErrInvalid
	}
	return nil
}

func (b *nativeFrameBootstrap) registerFrameCapture(c *nativeFrameCapture) error {
	if c == nil || c.binding == nil {
		return peer.ErrInvalid
	}
	ref := c.binding.Reference()
	if _, err := b.runtime.ResolveBinding(ref); err != nil {
		return err
	}
	b.capturesMu.Lock()
	defer b.capturesMu.Unlock()
	if b.captures == nil {
		b.captures = make(map[string]*nativeFrameCapture)
	}
	if b.captures[ref.Source] == nil && len(b.captures) >= fd.MaxBindingReferences {
		return peer.ErrUnavailable
	}
	b.captures[ref.Source], c.owner = c, b
	return nil
}

func (b *nativeFrameBootstrap) unregisterFrameCapture(c *nativeFrameCapture) {
	b.capturesMu.Lock()
	defer b.capturesMu.Unlock()
	if b.captures[c.binding.Reference().Source] == c {
		delete(b.captures, c.binding.Reference().Source)
	}
}

func (b *nativeFrameBootstrap) readerPublication(ref fd.BindingRef, stream string) (*in.DeliveryPublication, error) {
	if _, err := b.runtime.ResolveBinding(ref); err != nil {
		return nil, err
	}
	b.capturesMu.Lock()
	c := b.captures[ref.Source]
	b.capturesMu.Unlock()
	if c == nil {
		return nil, peer.ErrUnavailable
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.retired || c.binding.Reference() != ref {
		return nil, peer.ErrRetired
	}
	p := c.publications[stream]
	if p == nil {
		return nil, peer.ErrUnavailable
	}
	return p, nil
}

// The same bootstrap/control owner outlives every reader. One goroutine and
// one active connection per admitted endpoint; no goroutine per accept and no
// reconnect queue. A failed client's transport retires before accepting another.
func (b *nativeFrameBootstrap) serveFrameReaders(ctx context.Context, control *fd.Resource[*in.ControlDispatcher], ready chan<- struct{}) (result error) {
	if b == nil || ctx == nil || ready == nil {
		return peer.ErrInvalid
	}
	if err := control.ValidateRuntimeScope(b.runtime); err != nil {
		return err
	}
	plans := b.admission.readers
	if len(plans) > 128 || len(plans) > 2*b.admission.maxGrants {
		return peer.ErrInvalid
	}
	for n, p := range plans {
		if err := p.validate(); err != nil {
			return fmt.Errorf("frame reader plan index=%d: %w", n, err)
		}
		for _, earlier := range plans[:n] {
			if earlier.Path == p.Path {
				return peer.ErrInvalid
			}
		}
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	listeners := make([]*fd.Resource[*unixsocket.Publication], 0, len(plans))
	defer func() {
		for _, resource := range listeners {
			resource.Close()
		}
		wait, stop := context.WithTimeout(context.Background(), time.Second)
		defer stop()
		for _, resource := range listeners {
			result = errors.Join(result, resource.WaitClosed(wait))
		}
	}()
	for _, p := range plans {
		listener, err := fd.OwnRuntimeResource(ctx, b.runtime, fd.ResourcePlan[*unixsocket.Publication]{Bytes: p.ListenerBytes,
			Create: func(context.Context) (fd.Created[*unixsocket.Publication], error) {
				owner, err := unixsocket.ListenPacketSingleOwner(p.Path)
				return fd.Created[*unixsocket.Publication]{Value: owner, Owned: owner != nil}, err
			}, Release: func(owner *unixsocket.Publication) error { return owner.CleanupChecked() },
		})
		if listener != nil {
			listeners = append(listeners, listener)
		}
		if err != nil {
			return fmt.Errorf("frame reader listen grant=%s: %w", p.Grant, err)
		}
	}
	// All sockets have credentials enabled before the controller can release a
	// consumer. This is readiness, not a claim that a camera frame is available.
	done := make(chan error, len(plans))
	for n, resource := range listeners {
		plan := plans[n]
		go func() {
			done <- resource.Borrow(ctx, func(ctx context.Context, listener *unixsocket.Publication) error {
				return b.serveFrameReader(ctx, control, listener, plan)
			})
			cancel()
		}()
	}
	close(ready)
	log.Printf("[video-broker] frame_readers stage=ready endpoints=%d", len(plans))
	<-ctx.Done()
	for range plans {
		result = errors.Join(result, <-done)
	}
	return errors.Join(result, ctx.Err())
}

func (b *nativeFrameBootstrap) serveFrameReader(ctx context.Context, control *fd.Resource[*in.ControlDispatcher], listener *unixsocket.Publication, plan nativeFrameReaderPlan) error {
	var selectDelivery func(fd.BindingRef) (*in.DeliveryPublication, error)
	var lastSelected fd.BindingRef
	lastSelectionLog := time.Time{}
	if plan.Delivery {
		selectDelivery = func(ref fd.BindingRef) (*in.DeliveryPublication, error) {
			p, err := b.readerPublication(ref, plan.Stream)
			if err == nil && (ref != lastSelected || time.Since(lastSelectionLog) >= 5*time.Second) {
				log.Printf("[video-broker] frame_reader stage=publication_selected grant=%s source=%s source_epoch=%d stream=%s", plan.Grant, ref.Source, ref.Epoch, plan.Stream)
				lastSelected, lastSelectionLog = ref, time.Now()
			}
			return p, err
		}
	}
	lastLog := time.Time{}
	var rejected uint64
	for ctx.Err() == nil {
		var session *in.FrameSession
		var partial *fd.Resource[peer.Connection]
		var acceptErr error
		err := control.Borrow(ctx, func(ctx context.Context, dispatch *in.ControlDispatcher) error {
			var err error
			session, partial, err = dispatch.AcceptReaderSession(ctx, plan.Grant, selectDelivery, in.ReaderAcceptPlan{Bytes: plan.AcceptBytes,
				Accept: func(context.Context) (*net.UnixConn, error) {
					var conn *net.UnixConn
					conn, acceptErr = listener.AcceptPacket(time.Now().Add(250 * time.Millisecond))
					return conn, acceptErr
				}}, plan.MaxBytes, plan.MaxWorkNS)
			return err
		})
		if partial != nil {
			partial.Close()
			wait, cancel := context.WithTimeout(context.Background(), time.Second)
			closeErr := partial.WaitClosed(wait)
			cancel()
			if closeErr != nil {
				return errors.Join(err, closeErr)
			}
		}
		if acceptErr != nil {
			var timeout net.Error
			if errors.As(acceptErr, &timeout) && timeout.Timeout() && ctx.Err() == nil {
				continue
			}
			return err // structural listener failure, not an idle client
		}
		var sessionStats in.FrameSessionStats
		if session != nil {
			for ctx.Err() == nil {
				if _, err = session.Step(ctx); err != nil {
					break
				}
			}
			session.Close()
			sessionStats = session.Stats()
			wait, cancel := context.WithTimeout(context.Background(), time.Second)
			closeErr := session.WaitClosed(wait)
			cancel()
			if closeErr != nil {
				return errors.Join(err, closeErr)
			}
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil {
			if rejected != ^uint64(0) {
				rejected++
			}
			if time.Since(lastLog) >= 5*time.Second {
				log.Printf("[video-broker] frame_reader stage=connection_retired grant=%s delivery=%t retired=%d last_stage=%s received=%d completed=%d idle=%d negative=%d cause=%v", plan.Grant, plan.Delivery, rejected, sessionStats.LastStage, sessionStats.Received, sessionStats.Completed, sessionStats.Idle, sessionStats.Negative, err)
				lastLog = time.Now()
			}
		}
		// Bounds rejected/repeated connects too; no busy loop on missing grants,
		// retired publications or clients repeatedly disconnecting without work.
		timer := time.NewTimer(50 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return ctx.Err()
}
