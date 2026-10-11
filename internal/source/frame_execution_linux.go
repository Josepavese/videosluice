//go:build linux

package source

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"

	peer "github.com/Josepavese/videosluice/framepeer"
	pl "github.com/Josepavese/videosluice/framepeer/provider/linux"
)

// Mirrored by native/worker_release.h; contract tests compare both values.
const nativeWorkerFailedReleased = 64

type frameWorkerGroup interface {
	Attach(*exec.Cmd) error
	Started(*os.Process) error
	Close() error
}

// NativeFrameExecution verifies the signed worker's checked exit receipt. It
// never owns, starts, waits for or kills the capture process. A launcher may
// additionally own a delegated cgroup; that bound never certifies SDK/DMA.
type NativeFrameExecution struct {
	mu                sync.Mutex
	group             frameWorkerGroup
	admittedCapture   bool
	executable        string
	command           *exec.Cmd
	started, released bool
	releaseErr        error
}

// TrackAdmittedNativeCapture adds a checked release receipt to the EXISTING
// source capture owned/admitted by the broker. It creates no decoder, process,
// cgroup or DMA allocation. Existing hardware-context admission, finite codec
// buffers and thermal controls remain the source owner's responsibility. The
// Frame Access ledger separately covers its detached copies and transport.
// This is an explicit composition choice, never a failed-cgroup fallback.
func TrackAdmittedNativeCapture(executable string) (*NativeFrameExecution, error) {
	if !filepath.IsAbs(executable) || filepath.Clean(executable) != executable || executable == "/" {
		return nil, peer.ErrInvalid
	}
	return &NativeFrameExecution{executable: executable, admittedCapture: true}, nil
}

func PrepareNativeFrameExecution(parent *os.File, name, executable string, limits pl.WorkerLimits) (*NativeFrameExecution, error) {
	if !filepath.IsAbs(executable) || filepath.Clean(executable) != executable || executable == "/" {
		return nil, peer.ErrInvalid
	}
	group, err := pl.PrepareWorkerGroup(parent, name, limits)
	if group == nil {
		return nil, err
	}
	// A construction error may retain group ownership. Preserve that object so
	// the enclosing admitted resource can close/quarantine it, never lose it.
	return &NativeFrameExecution{group: group, executable: executable}, err
}

func (e *NativeFrameExecution) Attach(command *exec.Cmd) error {
	if e == nil {
		return peer.ErrInvalid
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.released || (e.group == nil && !e.admittedCapture) || e.command != nil || command == nil || command.Process != nil || command.Path != e.executable {
		return peer.ErrInvalid
	}
	if e.group != nil {
		if err := e.group.Attach(command); err != nil {
			return err
		}
	}
	e.command = command
	return nil
}

func (e *NativeFrameExecution) Started(process *os.Process) error {
	if e == nil {
		return peer.ErrInvalid
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.released || e.started || e.command == nil || process == nil || e.command.Process != process {
		return peer.ErrInvalid
	}
	e.started = true
	if e.group != nil {
		return e.group.Started(process)
	}
	return nil
}

func (e *NativeFrameExecution) Release() error {
	if e == nil {
		return nil
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.released {
		return e.releaseErr
	}
	if e.command != nil && e.command.Process != nil && e.command.ProcessState == nil {
		return fmt.Errorf("native execution stage=wait_missing: %w", peer.ErrBusy)
	}
	// Never cache a premature call as final: the sole process owner can finish
	// Wait and call again. Once a receipt exists, physical cleanup is attempted
	// once and its result is retained, including a failed native release.
	e.released = true
	if e.command != nil && e.command.Process != nil {
		state := e.command.ProcessState
		if state == nil || !state.Exited() || (state.ExitCode() != 0 && state.ExitCode() != nativeWorkerFailedReleased) {
			e.releaseErr = fmt.Errorf("native execution stage=sdk_release_unconfirmed process_state=%v: %w", state, peer.ErrUnavailable)
		}
	}
	if e.group != nil {
		e.releaseErr = errors.Join(e.releaseErr, e.group.Close())
	}
	return e.releaseErr
}
