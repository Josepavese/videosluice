package broker

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"strings"
	"time"
)

// A startup deadline remains a failure. If the owned worker is blocked in a
// filesystem page-in before it can install its cooperative stop handler, give
// that specific pre-handler state a separate, bounded retirement window.
// Sending TERM while it is still in the loader makes an otherwise releasable
// worker exit by signal and turns a transient I/O stall into a permanent
// hardware-release hold. This does not admit the late worker or extend the
// startup SLA.
const brokerPreHandlerRetirementWindow = 30 * time.Second

func awaitBrokerBootstrapStopReady(ctx context.Context, cmd *exec.Cmd, observation string, budget time.Duration) string {
	if cmd == nil || cmd.Process == nil || budget <= 0 ||
		!strings.Contains(observation, "state=D ") ||
		!strings.Contains(observation, "term_caught=false ") ||
		!strings.Contains(observation, "wait_channel=folio_wait_bit_common") {
		return "not_applicable"
	}
	timer := time.NewTimer(budget)
	defer timer.Stop()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return "parent_cancelled"
		case <-timer.C:
			return "window_elapsed"
		case <-ticker.C:
			current := brokerBootstrapObservation(cmd)
			if strings.Contains(current, "term_caught=true ") {
				return "stop_handler_ready"
			}
			if strings.Contains(current, "state=Z ") || current == "startup_observation=unavailable" {
				return "process_exited"
			}
		}
	}
}

// Unlike Cmd.StderrPipe, this pipe belongs to the broker. Cmd.Wait must not
// close the reader while scanStderr is still consuming the worker's terminal
// reason. The caller closes its writer immediately after Start (also on
// failure), and retains/closes the reader through the bounded drain.
func brokerChildStderrPipe(cmd *exec.Cmd) (*os.File, *os.File, error) {
	if cmd == nil || cmd.Stderr != nil {
		return nil, nil, errors.New("capture stderr already assigned or command absent")
	}
	reader, writer, err := os.Pipe()
	if err != nil {
		return nil, nil, err
	}
	cmd.Stderr = writer
	return reader, writer, nil
}

// The process group is already attested here. A failed frame handshake does
// not waive the worker's SDK release obligations: use the normal TERM/CONT
// grace and reserve KILL for an uncooperative worker. The caller alone Waits.
// Unattested process groups still use abortBrokerChildProcessGroup.
func stopFailedBrokerBootstrap(ctx context.Context, cancel context.CancelFunc, cmd *exec.Cmd, stderr io.Reader, grace time.Duration) (error, []byte) {
	// scanStderr is not running until bootstrap succeeds. Preserve the first
	// bounded diagnostic, and drain the rest so a verbose failing SDK cannot
	// block its own cleanup on a full stderr pipe. Never interpret these lines
	// as admission/control receipts from a successfully started capture.
	var diagnostic []byte
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		if stderr != nil {
			diagnostic, _ = io.ReadAll(io.LimitReader(stderr, 16*1024))
			_, _ = io.Copy(io.Discard, stderr)
		}
	}()
	exitDone, enforced := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(enforced)
		enforceBrokerChildTermination(ctx, cmd, exitDone, grace)
	}()
	cancel()
	err := cmd.Wait()
	close(exitDone)
	<-enforced
	<-readDone
	return err, diagnostic
}
