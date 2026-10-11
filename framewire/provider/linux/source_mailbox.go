//go:build linux && cgo

package linux

/*
#include "native/source_mailbox.h"
int vbsm_go_stage(vbsm_source_mailbox *,unsigned,uint64_t,uint8_t *,size_t,size_t *,uint64_t *);
*/
import "C"

import (
	"context"
	"fmt"
	"os"
	"sync"
	"unsafe"

	frame "github.com/Josepavese/videosluice/framewire"
	"github.com/Josepavese/videosluice/framewire/surfaceport"
)

// SourceMailbox owns two source slots and one exclusively borrowed exec file.
// Authentication and admission belong to the trusted launcher. It must charge
// SourceMailboxBytes plus the caller's private snapshot/native buffers
// BEFORE creating this allocation. No per-frame descriptors or queues exist.
type SourceMailbox struct {
	mu                sync.Mutex
	ptr               *C.vbsm_source_mailbox
	file              *os.File
	active            int
	closed, finalized bool
	closeErr          error
}

func SourceMailboxBytes(capacity uint64) (uint64, error) {
	n := uint64(C.vbsm_source_mailbox_bytes(C.uint64_t(capacity)))
	if n == 0 {
		return 0, frame.ErrInvalid
	}
	return pageCharge(n), nil
}

func NewSourceMailbox(mapping frame.Mapping) (*SourceMailbox, error) {
	if mapping.Namespace == [16]byte{} || mapping.Generation == 0 {
		return nil, frame.ErrInvalid
	}
	if _, err := SourceMailboxBytes(mapping.Capacity); err != nil {
		return nil, err
	}
	var native C.vbsc_mapping
	for i, b := range mapping.Namespace {
		native.namespace_id[i] = C.uint8_t(b)
	}
	native.generation, native.capacity = C.uint64_t(mapping.Generation), C.uint64_t(mapping.Capacity)
	var code C.int
	ptr := C.vbsm_source_mailbox_create(&native, &code)
	if ptr == nil {
		return nil, fmt.Errorf("%w: source-mailbox-create", surfaceport.ErrUnavailable)
	}
	m := &SourceMailbox{ptr: ptr}
	if code != 0 {
		m.closed = true
		return m, fmt.Errorf("%w: source-mailbox-create-cleanup", surfaceport.ErrRelease)
	}
	fd := int(C.vbsm_source_mailbox_export(ptr))
	if fd < 0 {
		if err := m.Close(); err != nil {
			return m, err
		}
		return nil, fmt.Errorf("%w: source-mailbox-export", surfaceport.ErrUnavailable)
	}
	m.file = os.NewFile(uintptr(fd), "source-mailbox-writer")
	return m, nil
}

func (m *SourceMailbox) Inherited() (*os.File, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed || m.file == nil {
		return nil, surfaceport.ErrClosed
	}
	return m.file, nil
}
func (m *SourceMailbox) CloseInherited() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.file != nil {
		m.closeErr = m.file.Close()
		m.file = nil
	}
	return m.closeErr
}
func (m *SourceMailbox) ConfirmWriter() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed || m.ptr == nil || m.active != 0 {
		return surfaceport.ErrClosed
	}
	return result(C.vbsm_source_mailbox_confirm(m.ptr))
}

// StageInto authenticates no process. Its caller fences the pinned producer
// before and after this single native attempt, and never exposes bytes on error.
// after/serial are per-slot publication counters, NOT capture or delivery IDs.
// The payload SHA is pending and preserved in the header; only an immutable
// private Store with verification-before-extraction may consume this output.
func (m *SourceMailbox) StageInto(ctx context.Context, slot int, after uint64, out []byte) (int, uint64, error) {
	if ctx == nil || slot < 0 || slot >= 2 || len(out) < frame.HeaderSize {
		return 0, 0, frame.ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return 0, 0, err
	}
	m.mu.Lock()
	if m.closed || m.ptr == nil {
		m.mu.Unlock()
		return 0, 0, surfaceport.ErrClosed
	}
	if m.active != 0 {
		m.mu.Unlock()
		return 0, 0, frame.ErrBusy
	}
	ptr := m.ptr
	m.active++
	m.mu.Unlock()
	var used C.size_t
	var serial C.uint64_t
	err := result(C.vbsm_go_stage(ptr, C.uint(slot), C.uint64_t(after), (*C.uint8_t)(unsafe.Pointer(&out[0])), C.size_t(len(out)), &used, &serial))
	m.mu.Lock()
	m.active--
	retired := m.closed
	m.mu.Unlock()
	if cancel := ctx.Err(); cancel != nil {
		return 0, 0, cancel
	}
	if retired {
		return 0, 0, surfaceport.ErrClosed
	}
	if err != nil {
		return 0, 0, err
	}
	return int(used), uint64(serial), nil
}

// Caller retries Close only to observe outstanding operations; a failed native
// unmap/close is permanent and remains charged in its owning resource ledger.
func (m *SourceMailbox) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	if m.finalized {
		return m.closeErr
	}
	if m.active != 0 {
		return frame.ErrBusy
	}
	if m.file != nil {
		m.closeErr = m.file.Close()
		m.file = nil
	}
	if C.vbsm_source_mailbox_close(m.ptr) != 0 {
		m.closeErr = fmt.Errorf("%w: source-mailbox-close", surfaceport.ErrRelease)
	} else {
		m.ptr = nil
	}
	m.finalized = true
	return m.closeErr
}
