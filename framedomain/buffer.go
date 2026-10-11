package framedomain

import (
	"context"
	"errors"
	fa "github.com/Josepavese/videosluice/frameaccess"
)

// Buffer is exclusive runtime-owned input/snapshot/transport staging memory.
// Borrow is for trusted in-process adapters: bytes MUST NOT escape its callback.
// It is not an RPC capability or an authorization assertion. Driver leases must
// be released before entering this plane. Retired active callbacks remain fully
// charged until they return; neither deadline nor Close fakes physical release.
type Buffer struct {
	binding      *Binding
	data         []byte
	busy, closed bool
}

func (r *Runtime) canChargeLocked(source string, n int) bool {
	// Subtraction avoids overflow, and includes all retired pinned generations.
	return n > 0 && n <= r.limits.MaxMemoryBytes-r.stores-r.bufferBytes-r.resourceBytes &&
		(source == "" || n <= r.limits.MaxSourceBytes-r.sourceBytes[source])
}

// AllocateBuffer admits bytes and cardinality BEFORE allocation. There is no
// waiting queue or software fallback. Close or source retirement reclaims an
// idle buffer; a stalled callback instead consumes its bounded reservation.
func (b *Binding) AllocateBuffer(ctx context.Context, n int) (*Buffer, error) {
	if ctx == nil || n <= 0 || n > b.runtime.limits.MaxBufferBytes {
		return nil, ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	_, opCtx, finish, err := b.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer finish()
	r := b.runtime
	r.mu.Lock()
	defer r.mu.Unlock()
	if b.closed || r.closed {
		return nil, ErrRetired
	}
	if err := opCtx.Err(); err != nil {
		return nil, err
	}
	if r.buffers >= r.limits.MaxBuffers || !r.canChargeLocked(b.scope.Source, n) {
		return nil, fa.CapacityRejected
	}
	r.bufferBytes += n
	r.buffers++
	r.sourceBytes[b.scope.Source] += n
	buffer := &Buffer{binding: b, data: make([]byte, n)}
	b.buffers[buffer] = struct{}{}
	return buffer, nil
}

// Borrow allows only one callback, cancels it when its source retires and
// rejects late results. It cannot roll back side effects inside a callback;
// callers must retain the commit semantics of any operation they invoke.
func (buffer *Buffer) Borrow(ctx context.Context, visit func(context.Context, []byte) error) error {
	if buffer == nil || buffer.binding == nil || ctx == nil || visit == nil {
		return ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	b := buffer.binding
	_, opCtx, finish, err := b.begin(ctx)
	if err != nil {
		return err
	}
	defer finish()
	r := b.runtime
	r.mu.Lock()
	if buffer.closed || b.closed || r.closed {
		r.mu.Unlock()
		return ErrRetired
	}
	if buffer.busy {
		r.mu.Unlock()
		return fa.CapacityRejected
	}
	buffer.busy = true
	data := buffer.data
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		buffer.busy = false
		if buffer.closed {
			buffer.releaseLocked()
		}
	}()
	if err = opCtx.Err(); err != nil {
		return err
	}
	err = visit(opCtx, data)
	r.mu.Lock()
	retired := buffer.closed
	r.mu.Unlock()
	if retired {
		return retainBorrowCause(ErrRetired, err)
	}
	if current := b.current(); current != nil {
		return retainBorrowCause(current, err)
	}
	if cancel := opCtx.Err(); cancel != nil {
		// Cancellation rejects the late result, but must not erase the
		// callback's original failure/checkpoint during unwinding.
		if err != nil && err != cancel {
			return errors.Join(cancel, err)
		}
		return cancel
	}
	return err
}

func (buffer *Buffer) releaseLocked() {
	if buffer.data == nil {
		return
	}
	r := buffer.binding.runtime
	n := len(buffer.data)
	buffer.data = nil
	r.bufferBytes -= n
	r.buffers--
	r.sourceBytes[buffer.binding.scope.Source] -= n
	delete(buffer.binding.buffers, buffer)
}
func (buffer *Buffer) closeLocked() {
	buffer.closed = true
	if !buffer.busy {
		buffer.releaseLocked()
	}
}
func (buffer *Buffer) Close() {
	if buffer == nil || buffer.binding == nil {
		return
	}
	r := buffer.binding.runtime
	r.mu.Lock()
	defer r.mu.Unlock()
	buffer.closeLocked()
}
