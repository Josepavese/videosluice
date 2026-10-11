//go:build linux && cgo

// Package linux implements the native Linux mapping provider. It does not
// authenticate clients, expose descriptors, choose sources or certify HW.
package linux

/*
#cgo CFLAGS: -std=c11 -Wall -Wextra -Werror -Wconversion
#cgo LDFLAGS: -lcrypto -lpthread
#include "bridge.h"
*/
import "C"

import (
	"context"
	"fmt"
	frame "github.com/Josepavese/videosluice/framewire"
	"github.com/Josepavese/videosluice/framewire/surfaceport"
	"github.com/Josepavese/videosluice/internal/framehash"
	"sync"
	"unsafe"
)

// Limits includes pending, active and retired-but-pinned mappings. Bytes is
// page-rounded mapping plus bounded hash workspace for every admitted operation,
// not total process RSS or external private copy budgets.
// Operations is per allocation; total pinned operations are bounded by
// Allocations*Operations. Slow callers do not retain a camera/driver lease.
type Limits struct {
	Bytes                   uint64
	Allocations, Operations int
}
type Usage struct {
	Bytes       uint64
	Allocations int
}
type Manager struct {
	mu    sync.Mutex
	limit Limits
	usage Usage
}
type allocation struct {
	mu                 sync.Mutex
	writer             sync.Mutex
	manager            *Manager
	ptr                *C.vbsc_owned
	charge             uint64
	capacity           uint64
	active             int
	closed             bool
	closing, finalized bool
	closeErr           error
}

var _ surfaceport.Allocator = (*Manager)(nil)
var _ surfaceport.Allocation = (*allocation)(nil)

func New(limit Limits) (*Manager, error) {
	if limit.Bytes == 0 || limit.Allocations <= 0 || limit.Operations <= 0 {
		return nil, frame.ErrInvalid
	}
	return &Manager{limit: limit}, nil
}

func (m *Manager) Usage() Usage { m.mu.Lock(); defer m.mu.Unlock(); return m.usage }
func (m *Manager) release(n uint64) {
	m.mu.Lock()
	m.usage.Bytes -= n
	m.usage.Allocations--
	m.mu.Unlock()
}

func (m *Manager) Footprint(mapping frame.Mapping) (uint64, error) {
	if mapping.Namespace == [16]byte{} || mapping.Generation == 0 || mapping.Capacity < frame.HeaderSize || mapping.Capacity > frame.HeaderSize+frame.MaxPayload {
		return 0, frame.ErrInvalid
	}
	// The OS page size is obtained in this provider only, not in the port.
	base := pageCharge(uint64(frame.ControlSize) + (mapping.Capacity+7)/8*8)
	workspace := uint64(C.vbsc_hash_workspace_bytes(C.size_t(mapping.Capacity - frame.HeaderSize)))
	if workspace > uint64(framehash.NativeWorkspaceLimit()) {
		return 0, frame.ErrInvalid
	}
	if workspace != 0 && uint64(m.limit.Operations) > (^uint64(0)-base)/workspace {
		return 0, frame.ErrInvalid
	}
	return base + workspace*uint64(m.limit.Operations), nil
}
func (m *Manager) Create(mapping frame.Mapping) (surfaceport.Allocation, error) {
	charge, err := m.Footprint(mapping)
	if err != nil {
		return nil, err
	}
	m.mu.Lock()
	if m.usage.Allocations >= m.limit.Allocations || charge > m.limit.Bytes-m.usage.Bytes {
		m.mu.Unlock()
		return nil, surfaceport.ErrQuota
	}
	m.usage.Allocations++
	m.usage.Bytes += charge
	m.mu.Unlock()
	var stage, cleanup C.int
	ptr := C.vbsc_owned_create((*C.uint8_t)(unsafe.Pointer(&mapping.Namespace[0])), C.uint64_t(mapping.Generation), C.uint64_t(mapping.Capacity), &stage, &cleanup)
	if ptr == nil {
		m.release(charge)
		return nil, fmt.Errorf("%w: provider=linux-atomic stage=%s", surfaceport.ErrUnavailable, createStage(int(stage)))
	}
	a := &allocation{manager: m, ptr: ptr, charge: charge, capacity: mapping.Capacity}
	if cleanup != 0 {
		a.closed, a.finalized = true, true
		a.closeErr = fmt.Errorf("%w: provider=linux-atomic create_stage=%s release_stage=%s", surfaceport.ErrRelease, createStage(int(stage)), releaseStage(int(cleanup)))
		return a, fmt.Errorf("%w: %w", surfaceport.ErrUnavailable, a.closeErr)
	}
	return a, nil
}

func releaseStage(code int) string {
	if code == 1 {
		return "unmap"
	}
	if code == 2 {
		return "descriptor-close"
	}
	return "unknown"
}

func createStage(stage int) string {
	switch stage {
	case C.VBSC_CREATE_VALIDATION:
		return "validation"
	case C.VBSC_CREATE_HEAP:
		return "allocation"
	case C.VBSC_CREATE_DESCRIPTOR:
		return "descriptor"
	case C.VBSC_CREATE_SIZE:
		return "size"
	case C.VBSC_CREATE_MAP:
		return "mapping"
	case C.VBSC_CREATE_ATOMIC:
		return "atomic-admission"
	case C.VBSC_CREATE_SEALS:
		return "seals"
	default:
		return "unknown"
	}
}

// pin protects C mapping lifetime without holding a Go lock across the native
// copy/hash. It never exposes a pointer outside this provider.
func (a *allocation) pin() (*C.vbsc_owned, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return nil, surfaceport.ErrClosed
	}
	if a.active >= a.manager.limit.Operations {
		return nil, surfaceport.ErrQuota
	}
	a.active++
	return a.ptr, nil
}
func (a *allocation) unpin() {
	a.mu.Lock()
	a.active--
	var dead *C.vbsc_owned
	if a.closed && a.active == 0 {
		dead = a.ptr
		a.ptr = nil
		if dead != nil {
			a.closing = true
		}
	}
	a.mu.Unlock()
	if dead != nil {
		_ = a.finishClose(dead)
	}
}
func (a *allocation) finishClose(dead *C.vbsc_owned) error {
	code := int(C.vbsc_owned_destroy(dead))
	var err error
	if code != 0 {
		err = fmt.Errorf("%w: provider=linux-atomic stage=%s", surfaceport.ErrRelease, releaseStage(code))
	}
	a.mu.Lock()
	a.finalized = true
	a.closing = false
	a.closeErr = err
	if err != nil {
		a.ptr = dead
	} // quarantined owner, never retried or published
	a.mu.Unlock()
	if err == nil {
		a.manager.release(a.charge)
	}
	return err
}
func (a *allocation) Close() error {
	a.mu.Lock()
	a.closed = true
	if a.finalized {
		err := a.closeErr
		a.mu.Unlock()
		return err
	}
	if a.active > 0 || a.closing {
		a.mu.Unlock()
		return frame.ErrBusy
	}
	dead := a.ptr
	a.ptr = nil
	a.closing = true
	a.mu.Unlock()
	return a.finishClose(dead)
}
func result(code C.int) error {
	switch code {
	case C.VBSC_OK:
		return nil
	case C.VBSC_BUSY:
		return frame.ErrBusy
	case C.VBSC_MAPPING:
		return frame.ErrMapping
	default:
		return frame.ErrInvalid
	}
}
func (a *allocation) Publish(ctx context.Context, p []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(p) < frame.HeaderSize || uint64(len(p)) > a.capacity {
		return frame.ErrInvalid
	}
	if !a.writer.TryLock() {
		return frame.ErrBusy
	}
	defer a.writer.Unlock()
	ptr, err := a.pin()
	if err != nil {
		return err
	}
	defer a.unpin()
	err = result(C.vbsc_owned_publish(ptr, (*C.uint8_t)(unsafe.Pointer(&p[0])), C.size_t(len(p))))
	return err
}
func (a *allocation) SnapshotInto(ctx context.Context, p []byte) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if len(p) < frame.HeaderSize {
		return 0, frame.ErrInvalid
	}
	ptr, err := a.pin()
	if err != nil {
		return 0, err
	}
	defer a.unpin()
	var used C.size_t
	err = result(C.vbsc_owned_snapshot(ptr, (*C.uint8_t)(unsafe.Pointer(&p[0])), C.size_t(len(p)), &used))
	if cancel := ctx.Err(); cancel != nil {
		return 0, cancel
	}
	if err != nil {
		return 0, err
	}
	return int(used), nil
}
