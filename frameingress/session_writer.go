package ingress

import (
	"context"
	"errors"
	"sync"

	fd "github.com/Josepavese/videosluice/framedomain"
	peer "github.com/Josepavese/videosluice/framepeer"
)

// sessionWriter is usable only inside one admitted transport Borrow. Pinning
// the transport once per request avoids three new resource operations per
// packet, but does not cache authority, generation or resource validity.
type sessionWriter struct {
	mu         sync.RWMutex
	resource   *fd.Resource[peer.Connection]
	runtime    *fd.Runtime
	ctx        context.Context
	connection peer.Connection
}

// with does not own or close c. Its caller holds the transport Borrow until
// this callback and every in-flight writer method have actually returned.
func (w *sessionWriter) with(ctx context.Context, c peer.Connection, visit func() error) error {
	if ctx == nil || c == nil || visit == nil {
		return peer.ErrInvalid
	}
	w.mu.Lock()
	if w.connection != nil {
		w.mu.Unlock()
		return peer.ErrBusy
	}
	w.ctx, w.connection = ctx, c
	w.mu.Unlock()
	defer func() {
		w.mu.Lock()
		w.ctx, w.connection = nil, nil
		w.mu.Unlock()
	}()
	if err := w.check(); err != nil {
		return err
	}
	err := visit()
	return errors.Join(err, w.check())
}

func (w *sessionWriter) check() error {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.checkLocked()
}

func (w *sessionWriter) checkLocked() error {
	if w.ctx == nil || w.connection == nil {
		return peer.ErrRetired
	}
	if err := w.ctx.Err(); err != nil {
		return err
	}
	// Synchronous validation is required even if resource cancellation has
	// not yet propagated through context.AfterFunc. It also checks the
	// controller process fence; per-packet consumer checks stay in the sender.
	return w.resource.ValidateRuntimeScope(w.runtime)
}

func (w *sessionWriter) CurrentGeneration() (string, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if err := w.checkLocked(); err != nil {
		return "", err
	}
	generation, err := w.connection.CurrentGeneration()
	return generation, errors.Join(err, w.checkLocked())
}

func (w *sessionWriter) WritePacket(ctx context.Context, p []byte) error {
	if ctx == nil {
		return peer.ErrInvalid
	}
	w.mu.RLock()
	defer w.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := w.checkLocked(); err != nil {
		return err
	}
	err := w.connection.WritePacket(ctx, p)
	return errors.Join(err, ctx.Err(), w.checkLocked())
}
