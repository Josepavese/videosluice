//go:build linux

package linux

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	peer "github.com/Josepavese/videosluice/framepeer"
	"golang.org/x/sys/unix"
)

// Functional progress must finish within its original deadline, independently
// of sanitizer teardown after main returns. A late buffered line is not PASS.
func nativeProbeLine(ctx context.Context, reader *bufio.Reader) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	type reply struct {
		line string
		err  error
	}
	done := make(chan reply, 1)
	go func() {
		line, err := reader.ReadString('\n')
		done <- reply{line, err}
	}()
	select {
	case result := <-done:
		if err := ctx.Err(); err != nil {
			return "", err
		}
		if deadline, ok := ctx.Deadline(); ok && !time.Now().Before(deadline) {
			return "", context.DeadlineExceeded
		}
		return strings.TrimSuffix(result.line, "\n"), result.err
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func expectNativeProbeLine(t *testing.T, ctx context.Context, reader *bufio.Reader, want string) {
	t.Helper()
	line, err := nativeProbeLine(ctx, reader)
	if err != nil || line != want {
		t.Fatalf("native functional acknowledgement=%q want=%q err=%v", line, want, err)
	}
}

// cmd has a separate bounded process context. Never accept a final marker as
// a substitute for reaping the process and requiring sanitizer exit status 0.
func waitNativeProbeExit(t *testing.T, cmd *exec.Cmd) error {
	t.Helper()
	started := time.Now()
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	var err error
	select {
	case err = <-done:
	case <-time.After(time.Second):
		proc := fmt.Sprintf("/proc/%d", cmd.Process.Pid)
		wchan, wchanErr := os.ReadFile(filepath.Join(proc, "wchan"))
		children, childrenErr := os.ReadFile(filepath.Join(proc, "task", fmt.Sprint(cmd.Process.Pid), "children"))
		status, statusErr := os.ReadFile(filepath.Join(proc, "status"))
		var state []string
		for _, line := range strings.Split(string(status), "\n") {
			if strings.HasPrefix(line, "State:") || strings.HasPrefix(line, "TracerPid:") {
				state = append(state, line)
			}
		}
		t.Logf("native lifecycle phase=post_main_exit_wait pid=%d wchan=%q wchan_error=%v children=%q children_error=%v state=%q state_error=%v",
			cmd.Process.Pid, strings.TrimSpace(string(wchan)), wchanErr, strings.TrimSpace(string(children)), childrenErr, state, statusErr)
		err = <-done
	}
	t.Logf("native lifecycle phase=post_main_exit_complete elapsed_ms=%d exit_error=%v", time.Since(started).Milliseconds(), err)
	return err
}

func TestNativeProbeLineRequiresFunctionalDeadline(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	if _, err := nativeProbeLine(ctx, bufio.NewReader(strings.NewReader("late\n"))); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("late buffered marker accepted: %v", err)
	}
	if line, err := nativeProbeLine(context.Background(), bufio.NewReader(strings.NewReader("ready\n"))); err != nil || line != "ready" {
		t.Fatalf("valid marker: %q %v", line, err)
	}
	if _, err := nativeProbeLine(context.Background(), bufio.NewReader(strings.NewReader("incomplete"))); err == nil {
		t.Fatal("unterminated marker accepted")
	}
}

func compileNativeLaunch(t *testing.T) string {
	t.Helper()
	nativeLaunchFixture.Lock()
	defer nativeLaunchFixture.Unlock()
	if nativeLaunchFixture.binary != "" {
		return nativeLaunchFixture.binary
	}
	root, err := filepath.Abs(".")
	if err != nil {
		t.Fatal(err)
	}
	if nativeLaunchFixture.directory == "" {
		directory, err := os.MkdirTemp("", "framepeer-native-launch-")
		if err != nil {
			t.Fatal(err)
		}
		nativeLaunchFixture.directory = directory
	}
	binary := filepath.Join(nativeLaunchFixture.directory, "native-launch")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "cc", "-std=c11", "-O1", "-g", "-Wall", "-Wextra", "-Wpedantic", "-Werror",
		"-fsanitize=address,undefined", "-fno-omit-frame-pointer", "-fno-pie", "-no-pie",
		"-I"+filepath.Join(root, "native"), "-I"+filepath.Join(root, "..", "..", "native"),
		filepath.Join(root, "native", "launch.c"), filepath.Join(root, "native", "tests", "launch_probe.c"), "-Wl,--wrap=close", "-o", binary)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("native compile: %v\n%s", err, output)
	}
	nativeLaunchFixture.binary = binary
	return binary
}

func TestNativeLaunchInterop(t *testing.T) {
	binary := compileNativeLaunch(t)
	for _, mode := range []string{"normal", "fork", "deadline", "shared_deadline", "rights", "truncated", "foreign_sender", "close_failure"} {
		t.Run(mode, func(t *testing.T) {
			launch, _ := launchFixture(t)
			files, metadata, err := launch.Inherited()
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			workerCtx, workerCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer workerCancel()
			cmd := exec.CommandContext(workerCtx, binary, mode, hex.EncodeToString(metadata))
			cmd.ExtraFiles = files[:]
			input, err := cmd.StdinPipe()
			if err != nil {
				t.Fatal(err)
			}
			defer input.Close()
			output, err := cmd.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			reader := bufio.NewReader(output)
			var stderr bytes.Buffer
			cmd.Stderr = &stderr
			if err = cmd.Start(); err != nil {
				t.Fatal(err)
			}
			waited := false
			defer func() {
				cancel()
				workerCancel()
				if !waited {
					cmd.Wait()
				}
				if t.Failed() {
					t.Logf("native stderr after joined exit: %s", stderr.String())
				}
			}()
			if err = launch.CloseInherited(); err != nil {
				t.Fatal(err)
			}
			endpoint, err := launch.Deliver(ctx, cmd.Process, "native.worker.1", uint32(os.Getuid()), uint32(os.Getgid()), time.Second)
			if err != nil {
				t.Fatal(err)
			}
			var receive [128]byte
			expect := func(want string) {
				t.Helper()
				n, principal, err := endpoint.Receive(ctx, receive[:])
				if err != nil || string(receive[:n]) != want || principal.Generation != "native.worker.1" {
					t.Fatalf("receive=%q principal=%+v err=%v", receive[:n], principal, err)
				}
			}
			expect("READY")
			if mode == "deadline" {
				expect("TIMEOUT")
			}
			if mode == "shared_deadline" {
				expect("BUDGET")
				expect("TIMEOUT")
			}
			if mode == "rights" {
				if err = endpoint.WriteCommand(ctx, []byte("unexpected"), launch.child); err != nil {
					t.Fatal(err)
				}
				expect("REJECTED_RECORD")
			}
			if mode == "truncated" {
				if _, err = launch.connection.Write(bytes.Repeat([]byte{1}, 4097)); err != nil {
					t.Fatal(err)
				}
				expect("REJECTED_RECORD")
			}
			if mode == "foreign_sender" {
				file, err := launch.connection.File()
				if err != nil {
					t.Fatal(err)
				}
				// Sending/rejection uses ctx; reaping may follow the native
				// sanitizer's post-main exit and needs the bounded worker context.
				attacker := exec.CommandContext(workerCtx, os.Args[0], "-test.run=^TestNativeLaunchForeignSenderChild$")
				attacker.Env = append(os.Environ(), "FRAMEPEER_NATIVE_FOREIGN_SENDER=1")
				attacker.ExtraFiles = []*os.File{file}
				output, err := attacker.StdoutPipe()
				if err != nil {
					file.Close()
					t.Fatal(err)
				}
				var attackerErrors bytes.Buffer
				attacker.Stderr = &attackerErrors
				if err = attacker.Start(); err != nil {
					file.Close()
					t.Fatal(err)
				}
				file.Close()
				defer func() {
					if err := attacker.Wait(); err != nil {
						t.Errorf("foreign sender exit: %v %s", err, attackerErrors.String())
					}
				}()
				// Synchronize on completed send, not Go race-runtime exit cleanup.
				// The latter delays process exit independently of channel progress.
				expectNativeProbeLine(t, ctx, bufio.NewReader(output), "SENT")
				expect("REJECTED_RECORD")
			}
			if err = endpoint.WriteCommand(ctx, []byte("ping"), nil); err != nil {
				t.Fatal(err)
			}
			expect("ping")
			if err = endpoint.WriteCommand(ctx, []byte("done"), nil); err != nil {
				t.Fatal(err)
			}
			expectNativeProbeLine(t, ctx, reader, "NATIVE_LAUNCH_PASS handles_released=1")
			if _, err = input.Write([]byte{1}); err != nil {
				t.Fatal(err)
			}
			expectNativeProbeLine(t, ctx, reader, "NATIVE_MAIN_RETURN handles_released=1")
			waited = true
			if err = waitNativeProbeExit(t, cmd); err != nil {
				t.Fatalf("native child: %v\n%s", err, stderr.String())
			}
		})
	}
}

func TestNativeLaunchForeignSenderChild(t *testing.T) {
	if os.Getenv("FRAMEPEER_NATIVE_FOREIGN_SENDER") != "1" {
		return
	}
	file := os.NewFile(3, "foreign-writer")
	connection, err := net.FileConn(file)
	file.Close()
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	if _, err = connection.Write([]byte("spoof")); err != nil {
		t.Fatal(err)
	}
	fmt.Println("SENT")
}

func TestNativeLaunchRejectsInvalidBootstrap(t *testing.T) {
	binary := compileNativeLaunch(t)
	for _, mode := range []string{"foreign_self", "foreign_self_handle", "missing_handle", "multiple_handles", "excess_handles", "oversize", "foreign_controller", "malformed_metadata", "non_pidfd", "wrong_uid"} {
		t.Run(mode, func(t *testing.T) {
			launch, parent := launchFixture(t)
			files, metadata, err := launch.Inherited()
			if err != nil {
				t.Fatal(err)
			}
			if mode == "foreign_controller" {
				id, err := DecodeProcessIdentity(metadata)
				if err != nil {
					t.Fatal(err)
				}
				id.PID++
				metadata, err = EncodeProcessIdentity(id)
				if err != nil {
					t.Fatal(err)
				}
			}
			if mode == "malformed_metadata" {
				metadata[4] = 2
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			workerCtx, workerCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer workerCancel()
			cmd := exec.CommandContext(workerCtx, binary, "reject", hex.EncodeToString(metadata))
			cmd.ExtraFiles = files[:]
			if mode == "non_pidfd" {
				fake, err := os.Open(os.DevNull)
				if err != nil {
					t.Fatal(err)
				}
				defer fake.Close()
				cmd.ExtraFiles = []*os.File{files[0], fake}
			}
			input, err := cmd.StdinPipe()
			if err != nil {
				t.Fatal(err)
			}
			defer input.Close()
			output, err := cmd.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			var stderr bytes.Buffer
			cmd.Stderr = &stderr
			if err = cmd.Start(); err != nil {
				t.Fatal(err)
			}
			waited := false
			defer func() {
				cancel()
				workerCancel()
				if !waited {
					cmd.Wait()
				}
			}()
			if err = launch.CloseInherited(); err != nil {
				t.Fatal(err)
			}
			child, err := BindStartedProcess(cmd.Process, "native.start", uint32(os.Getuid()), uint32(os.Getgid()))
			if err != nil {
				t.Fatal(err)
			}
			defer child.Close()
			if mode != "foreign_controller" && mode != "malformed_metadata" && mode != "non_pidfd" {
				endpoint, err := NewControlConnection(launch.connection, child, time.Second)
				if err != nil {
					t.Fatal(err)
				}
				id := ProcessIdentity{PID: cmd.Process.Pid, UID: uint32(os.Getuid()), GID: uint32(os.Getgid()), Generation: "native.start"}
				var capability peer.ProcessCapability = child
				if mode == "foreign_self" {
					id.PID = os.Getpid()
					id.Generation = "launcher.self"
					capability = parent
				}
				if mode == "foreign_self_handle" {
					capability = parent
				}
				if mode == "wrong_uid" {
					id.UID++
				}
				packet, err := EncodeProcessIdentity(id)
				if err != nil {
					t.Fatal(err)
				}
				if mode == "missing_handle" {
					capability = nil
				}
				if mode == "oversize" {
					packet = make([]byte, 83)
				}
				if mode == "multiple_handles" || mode == "excess_handles" {
					file, _, err := child.Export()
					if err != nil {
						t.Fatal(err)
					}
					rights := []int{int(file.Fd()), int(file.Fd())}
					if mode == "excess_handles" {
						for len(rights) < 32 {
							rights = append(rights, int(file.Fd()))
						}
					}
					_, _, err = launch.connection.WriteMsgUnix(packet, unix.UnixRights(rights...), nil)
					file.Close()
					if err != nil {
						t.Fatal(err)
					}
				} else if err = endpoint.WriteCommand(ctx, packet, capability); err != nil {
					t.Fatal(err)
				}
			}
			reader := bufio.NewReader(output)
			expected := map[string]string{
				"foreign_self":        "REJECTED code=-3 stage=launch_self_identity",
				"foreign_self_handle": "REJECTED code=-3 stage=capability_pid",
				"missing_handle":      "REJECTED code=-1 stage=launch_self_missing",
				"multiple_handles":    "REJECTED code=-6 stage=receive_record",
				"excess_handles":      "REJECTED code=-6 stage=receive_truncated",
				"oversize":            "REJECTED code=-6 stage=receive_truncated",
				"foreign_controller":  "REJECTED code=-3 stage=capability_pid",
				"malformed_metadata":  "REJECTED code=-1 stage=launch_controller_metadata",
				"non_pidfd":           "REJECTED code=-1 stage=capability_type",
				"wrong_uid":           "REJECTED code=-3 stage=launch_self_identity",
			}
			expectNativeProbeLine(t, ctx, reader, expected[mode])
			if _, err = input.Write([]byte{1}); err != nil {
				t.Fatal(err)
			}
			expectNativeProbeLine(t, ctx, reader, "NATIVE_MAIN_RETURN handles_released=1")
			waited = true
			if err = waitNativeProbeExit(t, cmd); err != nil {
				t.Fatalf("native rejection: %v\n%s", err, stderr.String())
			}
			if _, err = parent.CurrentGeneration(); err != nil {
				t.Fatal("native reject consumed parent", err)
			}
		})
	}
}
