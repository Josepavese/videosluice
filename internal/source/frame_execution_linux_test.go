//go:build linux

package source

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"

	peer "github.com/Josepavese/videosluice/framepeer"
	pl "github.com/Josepavese/videosluice/framepeer/provider/linux"
)

// Kernel policy/membership is covered by WorkerGroup's existing host/device
// tests. These tests exercise only composition with the actual child receipt.
type executionTestGroup struct {
	attaches, starts, closes      int
	attachErr, startErr, closeErr error
}

func (g *executionTestGroup) Attach(*exec.Cmd) error    { g.attaches++; return g.attachErr }
func (g *executionTestGroup) Started(*os.Process) error { g.starts++; return g.startErr }
func (g *executionTestGroup) Close() error              { g.closes++; return g.closeErr }

func executionChild(t *testing.T, script string) (*NativeFrameExecution, *executionTestGroup, *exec.Cmd) {
	t.Helper()
	shell, err := exec.LookPath("sh")
	if err != nil {
		t.Fatal(err)
	}
	shell, err = filepath.Abs(shell)
	if err != nil {
		t.Fatal(err)
	}
	g := &executionTestGroup{}
	e := &NativeFrameExecution{group: g, executable: shell}
	return e, g, exec.Command(shell, "-c", script)
}

func TestNativeFrameExecutionReceipts(t *testing.T) {
	for _, status := range []int{0, nativeWorkerFailedReleased, 65, 1, 127} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			e, g, cmd := executionChild(t, "exit "+strconv.Itoa(status))
			if err := e.Attach(cmd); err != nil {
				t.Fatal(err)
			}
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			if err := e.Started(cmd.Process); err != nil {
				t.Fatal(err)
			}
			waitErr := cmd.Wait()
			if (waitErr == nil) != (status == 0) {
				t.Fatalf("operation failure lost: %v", waitErr)
			}
			err := e.Release()
			wantReleased := status == 0 || status == nativeWorkerFailedReleased
			if (err == nil) != wantReleased {
				t.Fatalf("status %d receipt: %v", status, err)
			}
			if !wantReleased && !errors.Is(err, peer.ErrUnavailable) {
				t.Fatalf("wrong failure: %v", err)
			}
			if again := e.Release(); again != err || g.closes != 1 {
				t.Fatalf("release replay: %v/%v calls=%d", err, again, g.closes)
			}
		})
	}
}

func TestAdmittedNativeCaptureRequiresExactReapedWorker(t *testing.T) {
	for _, path := range []string{"", "/", "relative", "/bin/../bin/sh"} {
		if _, err := TrackAdmittedNativeCapture(path); !errors.Is(err, peer.ErrInvalid) {
			t.Fatal(path, err)
		}
	}
	for _, status := range []int{0, 64, 65} {
		_, _, cmd := executionChild(t, "exit "+strconv.Itoa(status))
		e, err := TrackAdmittedNativeCapture(cmd.Path)
		if err != nil {
			t.Fatal(err)
		}
		if err := e.Attach(exec.Command("/wrong-worker")); !errors.Is(err, peer.ErrInvalid) {
			t.Fatal(err)
		}
		if err := e.Attach(cmd); err != nil {
			t.Fatal(err)
		}
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		if err := e.Started(cmd.Process); err != nil {
			t.Fatal(err)
		}
		if err := e.Release(); !errors.Is(err, peer.ErrBusy) {
			t.Fatal("unreaped worker released", err)
		}
		_ = cmd.Wait()
		if err := e.Release(); (err == nil) != (status != 65) {
			t.Fatal(status, err)
		}
	}
}

func TestNativeFrameExecutionWaitAndSignal(t *testing.T) {
	e, g, cmd := executionChild(t, "read ignored; exit 0")
	input, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	defer input.Close()
	if err := e.Attach(cmd); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if cmd.ProcessState == nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	})
	if err := e.Started(cmd.Process); err != nil {
		t.Fatal(err)
	}
	if err := e.Release(); !errors.Is(err, peer.ErrBusy) || g.closes != 0 {
		t.Fatalf("premature release: %v closes=%d", err, g.closes)
	}
	// A killed/reaped child and an empty group do not certify SDK teardown.
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err == nil {
		t.Fatal("signal lost")
	}
	if err := e.Release(); !errors.Is(err, peer.ErrUnavailable) || g.closes != 1 {
		t.Fatalf("signal receipt: %v", err)
	}
}

// Early SIGTERM is distinct from the worker's cooperative 0/64 release receipt.
// This models cancellation before stop handlers/readiness, without an SDK or
// changing the runtime deadline. Empty process groups must not bypass the guard.
func TestNativeFrameExecutionRejectsEarlyTermination(t *testing.T) {
	e, g, cmd := executionChild(t, "trap - TERM; kill -TERM $$; exit 0")
	if err := e.Attach(cmd); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	if err := e.Started(cmd.Process); err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Fatal(err)
	}
	err := cmd.Wait()
	status, ok := cmd.ProcessState.Sys().(syscall.WaitStatus)
	if err == nil || !ok || !status.Signaled() || status.Signal() != syscall.SIGTERM {
		t.Fatalf("fixture did not terminate before a receipt: %v state=%v", err, cmd.ProcessState)
	}
	err = e.Release()
	if !errors.Is(err, peer.ErrUnavailable) || g.closes != 1 {
		t.Fatalf("early termination certified a hardware release: %v closes=%d", err, g.closes)
	}
	if again := e.Release(); again != err || g.closes != 1 {
		t.Fatalf("early termination was reset by release replay: %v/%v closes=%d", err, again, g.closes)
	}
}

func TestNativeFrameExecutionBindingAndFailures(t *testing.T) {
	e, g, cmd := executionChild(t, "exit 0")
	wrong := exec.Command("/not-the-admitted-worker")
	if err := e.Attach(wrong); !errors.Is(err, peer.ErrInvalid) || g.attaches != 0 {
		t.Fatalf("wrong worker: %v", err)
	}
	if err := e.Attach(cmd); err != nil {
		t.Fatal(err)
	}
	if err := e.Attach(cmd); !errors.Is(err, peer.ErrInvalid) || g.attaches != 1 {
		t.Fatalf("duplicate attach: %v", err)
	}
	if err := e.Started(&os.Process{Pid: os.Getpid()}); !errors.Is(err, peer.ErrInvalid) || g.starts != 0 {
		t.Fatalf("wrong process: %v", err)
	}
	g.closeErr = errors.New("retained group")
	if err := e.Release(); !errors.Is(err, g.closeErr) {
		t.Fatalf("group failure lost: %v", err)
	}
	if err := e.Release(); !errors.Is(err, g.closeErr) || g.closes != 1 {
		t.Fatalf("cleanup replay: %v", err)
	}
	if err := e.Attach(cmd); !errors.Is(err, peer.ErrInvalid) {
		t.Fatalf("attach after close: %v", err)
	}

	e, g, cmd = executionChild(t, "exit 0")
	g.startErr = errors.New("membership changed")
	if err := e.Attach(cmd); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	if err := e.Started(cmd.Process); !errors.Is(err, g.startErr) {
		t.Fatalf("start verification lost: %v", err)
	}
	if err := e.Started(cmd.Process); !errors.Is(err, peer.ErrInvalid) || g.starts != 1 {
		t.Fatalf("start replay: %v", err)
	}
	if err := cmd.Wait(); err != nil {
		t.Fatal(err)
	}
	if err := e.Release(); err != nil {
		t.Fatal(err)
	}
}

func TestNativeFrameExecutionRejectsUnboundedParent(t *testing.T) {
	parent, err := os.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	for _, executable := range []string{"worker", "/", "/worker/../worker"} {
		if e, err := PrepareNativeFrameExecution(parent, "worker", executable, pl.WorkerLimits{MemoryBytes: 4096, Tasks: 4}); e != nil || !errors.Is(err, peer.ErrInvalid) {
			t.Fatalf("invalid executable %q: %v/%v", executable, e, err)
		}
	}
	if e, err := PrepareNativeFrameExecution(parent, "worker", "/worker", pl.WorkerLimits{MemoryBytes: 4096, Tasks: 4}); e != nil || !errors.Is(err, peer.ErrUnavailable) {
		t.Fatalf("ordinary directory admitted: %v/%v", e, err)
	}
}

func TestNativeFrameExecutionCReceiptContract(t *testing.T) {
	header, err := os.ReadFile("../../native/worker_release.h")
	if err != nil {
		t.Fatal(err)
	}
	for _, declaration := range []string{
		"VIDEO_BROKER_WORKER_RELEASED = 0",
		"VIDEO_BROKER_WORKER_FAILED_RELEASED = " + strconv.Itoa(nativeWorkerFailedReleased),
		"VIDEO_BROKER_WORKER_RELEASE_UNCONFIRMED = 65",
	} {
		if !strings.Contains(string(header), declaration) {
			t.Fatalf("C/Go exit contract differs: %s", declaration)
		}
	}
}
