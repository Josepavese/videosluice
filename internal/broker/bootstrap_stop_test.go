package broker

import (
	"bufio"
	"context"
	"errors"
	"io"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"

	peer "github.com/Josepavese/videosluice/framepeer"
	"github.com/Josepavese/videosluice/internal/source"
)

func TestFailedBrokerBootstrapPreservesReleaseAndDiagnostics(t *testing.T) {
	for _, cooperative := range []bool{true, false} {
		name := "uncooperative"
		trap := "trap '' TERM"
		if cooperative {
			name = "cooperative"
			// Exceed pipe capacity during cleanup: stderr must be drained even
			// though the normal runtime scanner has never been started.
			trap = "trap 'printf \"%*s\" 131072 x >&2; exit 64' TERM"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			// Bash defers traps while waiting for a foreground external command.
			// TERM can arrive between ready and the fork of sleep, leaving a new
			// child outside that already-delivered group signal for one second.
			// Its wait builtin is interruptible: exercise cooperative SDK cleanup,
			// not that shell race, without changing this test's 200ms grace.
			cmd := exec.CommandContext(ctx, "/bin/bash", "-c", trap+"\nprintf 'native_startup_failure\\n' >&2\nprintf 'ready\\n'\nwhile :; do sleep 1 & wait $!; done")
			configureBrokerChildLifecycle(cmd)
			execution, err := source.TrackAdmittedNativeCapture(cmd.Path)
			if err != nil {
				t.Fatal(err)
			}
			if err = execution.Attach(cmd); err != nil {
				t.Fatal(err)
			}
			stderr, stderrWriter, err := brokerChildStderrPipe(cmd)
			if err != nil {
				t.Fatal(err)
			}
			defer stderr.Close()
			defer stderrWriter.Close()
			stdout, err := cmd.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			if err = cmd.Start(); err != nil {
				t.Fatal(err)
			}
			_ = stderrWriter.Close()
			t.Cleanup(func() {
				if cmd.ProcessState == nil {
					abortBrokerChildProcessGroup(cmd)
				}
			})
			if err = execution.Started(cmd.Process); err != nil {
				t.Fatal(err)
			}
			if line, err := bufio.NewReader(stdout).ReadString('\n'); err != nil || line != "ready\n" {
				t.Fatal(line, err)
			}
			start := time.Now()
			waitErr, diagnostic := stopFailedBrokerBootstrap(ctx, cancel, cmd, stderr, 200*time.Millisecond)
			if waitErr == nil || cmd.ProcessState == nil {
				t.Fatal("failed worker receipt lost", waitErr)
			}
			if elapsed := time.Since(start); elapsed > 2*time.Second {
				t.Fatal("cleanup exceeded bounded grace", elapsed)
			}
			if len(diagnostic) > 16*1024 || !strings.Contains(string(diagnostic), "native_startup_failure") {
				t.Fatalf("diagnostic missing or unbounded: %d bytes", len(diagnostic))
			}
			err = execution.Release()
			if cooperative {
				if err != nil || cmd.ProcessState.ExitCode() != 64 {
					t.Fatal("cooperative release was destroyed", cmd.ProcessState, err)
				}
			} else if !errors.Is(err, peer.ErrUnavailable) || !strings.Contains(err.Error(), "signal: killed") {
				t.Fatal("killed worker was falsely released or cause lost", err)
			}
		})
	}
}

func TestBrokerOwnedStderrRetainsTerminalBytesAfterWait(t *testing.T) {
	cmd := exec.Command("/bin/sh", "-c", "printf 'terminal_reason\\n' >&2; exit 7")
	reader, writer, err := brokerChildStderrPipe(cmd)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	defer writer.Close()
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	_ = writer.Close()
	if err = cmd.Wait(); err == nil || cmd.ProcessState.ExitCode() != 7 {
		t.Fatal("child receipt lost", err)
	}
	// Deliberately read AFTER Wait. Cmd.StderrPipe closes this descriptor and
	// loses the buffered reason; broker ownership must survive that ordering.
	data, err := io.ReadAll(reader)
	if err != nil || string(data) != "terminal_reason\n" {
		t.Fatalf("terminal bytes lost after Wait: %q %v", data, err)
	}
}

func TestBootstrapRetirementWaitsForStopHandlerWithoutAdmittingLateWorker(t *testing.T) {
	cmd := exec.Command("/bin/bash", "-c", "sleep 0.3; trap 'exit 64' TERM; while :; do sleep 1 & wait $!; done")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if cmd.ProcessState == nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	})
	observation := "startup_observation=proc state=D threads=1 term_caught=false term_blocked=false wait_channel=folio_wait_bit_common"
	start := time.Now()
	result := awaitBrokerBootstrapStopReady(context.Background(), cmd, observation, 2*time.Second)
	if result != "stop_handler_ready" || time.Since(start) < 200*time.Millisecond {
		t.Fatalf("premature termination or lost handler: result=%s elapsed=%s", result, time.Since(start))
	}
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err == nil || cmd.ProcessState.ExitCode() != 64 {
		t.Fatalf("late worker did not exit cooperatively: %v %v", err, cmd.ProcessState)
	}
}

func TestBootstrapRetirementWindowDoesNotChangeStartupAdmission(t *testing.T) {
	cmd := exec.Command("/bin/bash", "-c", "while :; do sleep 1 & wait $!; done")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if cmd.ProcessState == nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	})
	blocked := "startup_observation=proc state=D threads=1 term_caught=false term_blocked=false wait_channel=folio_wait_bit_common"
	start := time.Now()
	if got := awaitBrokerBootstrapStopReady(context.Background(), cmd, blocked, 120*time.Millisecond); got != "window_elapsed" {
		t.Fatalf("unready worker incorrectly admitted: %s", got)
	}
	if elapsed := time.Since(start); elapsed < 100*time.Millisecond || elapsed > time.Second {
		t.Fatalf("retirement window not bounded: %s", elapsed)
	}
	if got := awaitBrokerBootstrapStopReady(context.Background(), cmd, strings.Replace(blocked, "state=D", "state=S", 1), time.Second); got != "not_applicable" {
		t.Fatalf("non-page-in worker delayed: %s", got)
	}
}
