//go:build linux

package linux

import (
	"bufio"
	"context"
	"encoding/hex"
	"fmt"
	peer "github.com/Josepavese/videosluice/framepeer"
	"golang.org/x/sys/unix"
	"os"
	"os/exec"
	"testing"
	"time"
)

func TestLaunchReceiverChild(t *testing.T) {
	mode := os.Getenv("FAP_LAUNCH_TEST_MODE")
	if mode == "" {
		return
	}
	metadata, err := hex.DecodeString(os.Getenv("FAP_LAUNCH_TEST_METADATA"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	accepted, err := AcceptLaunch(ctx, os.NewFile(3, "child-channel"), os.NewFile(4, "parent-capability"), metadata, time.Second)
	if accepted != nil {
		defer accepted.Close()
	}
	if err == nil {
		t.Fatal("malformed bootstrap accepted", mode)
	}
	if accepted != nil {
		t.Fatal("clean rejection retained partial resources", err)
	}
	fmt.Println("rejected")
	bufio.NewReader(os.Stdin).ReadByte()
}

func launchFixture(t *testing.T) (*Launch, *Lease) {
	t.Helper()
	fd, err := unix.PidfdOpen(os.Getpid(), 0)
	if err != nil {
		t.Fatal(err)
	}
	file := os.NewFile(uintptr(fd), "fixture-launcher-self")
	parent, err := ImportProcessCapability(file, ProcessIdentity{PID: os.Getpid(), UID: uint32(os.Getuid()), GID: uint32(os.Getgid()), Generation: "launcher.self"})
	if err != nil {
		file.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { parent.Close() })
	l, err := PrepareLaunch(parent)
	if l != nil {
		t.Cleanup(func() { l.Close() })
	}
	if err != nil {
		t.Fatal(err)
	}
	return l, parent
}

func TestLaunchRejectsMalformedBootstrapAcrossProcesses(t *testing.T) {
	for _, mode := range []string{"foreign_self", "missing_handle", "oversize", "foreign_controller"} {
		t.Run(mode, func(t *testing.T) {
			launch, parent := launchFixture(t)
			files, metadata, err := launch.Inherited()
			if err != nil {
				t.Fatal(err)
			}
			if mode == "foreign_controller" {
				identity, err := DecodeProcessIdentity(metadata)
				if err != nil {
					t.Fatal(err)
				}
				identity.PID++
				metadata, err = EncodeProcessIdentity(identity)
				if err != nil {
					t.Fatal(err)
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestLaunchReceiverChild$")
			cmd.Env = append(os.Environ(), "FAP_LAUNCH_TEST_MODE="+mode, "FAP_LAUNCH_TEST_METADATA="+hex.EncodeToString(metadata))
			cmd.ExtraFiles = files[:]
			cmd.Stderr = os.Stderr
			input, err := cmd.StdinPipe()
			if err != nil {
				t.Fatal(err)
			}
			defer input.Close()
			output, err := cmd.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			if err = cmd.Start(); err != nil {
				t.Fatal(err)
			}
			defer func() { cancel(); cmd.Wait() }()
			if err = launch.CloseInherited(); err != nil {
				t.Fatal(err)
			}
			child, err := BindStartedProcess(cmd.Process, "child.start", uint32(os.Getuid()), uint32(os.Getgid()))
			if err != nil {
				t.Fatal(err)
			}
			defer child.Close()
			if mode != "foreign_controller" {
				endpoint, err := NewControlConnection(launch.connection, child, time.Second)
				if err != nil {
					t.Fatal(err)
				}
				identity := ProcessIdentity{PID: cmd.Process.Pid, UID: uint32(os.Getuid()), GID: uint32(os.Getgid()), Generation: "child.start"}
				var capability peer.ProcessCapability = child
				if mode == "foreign_self" {
					identity.PID = os.Getpid()
					identity.Generation = "launcher.self"
					capability = parent
				}
				packet, err := EncodeProcessIdentity(identity)
				if err != nil {
					t.Fatal(err)
				}
				if mode == "missing_handle" {
					capability = nil
				}
				if mode == "oversize" {
					packet = make([]byte, 257)
				}
				if err = endpoint.WriteCommand(ctx, packet, capability); err != nil {
					t.Fatal(err)
				}
			}
			scanner := bufio.NewScanner(output)
			if !scanner.Scan() || scanner.Text() != "rejected" {
				t.Fatal("child rejection not observed", scanner.Text())
			}
			if _, err = parent.CurrentGeneration(); err != nil {
				t.Fatal("rejection consumed launcher authority", err)
			}
		})
	}
}

func TestLaunchCancelledAcceptClosesInheritedHandles(t *testing.T) {
	l, _ := launchFixture(t)
	files, metadata, err := l.Inherited()
	if err != nil {
		t.Fatal(err)
	}
	// Duplicate as exec would: Accept owns these copies, not Launch's files.
	var copies [2]*os.File
	for i, file := range files {
		fd, e := unix.FcntlInt(file.Fd(), unix.F_DUPFD_CLOEXEC, 0)
		if e != nil {
			t.Fatal(e)
		}
		copies[i] = os.NewFile(uintptr(fd), "fixture-inherited")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	accepted, err := AcceptLaunch(ctx, copies[0], copies[1], metadata, time.Second)
	if accepted != nil || err == nil {
		t.Fatal("canceled bootstrap admitted", err)
	}
	for _, file := range copies {
		if _, err = file.Stat(); err == nil {
			t.Fatal("canceled bootstrap leaked descriptor")
		}
	}
	if err = l.CloseInherited(); err != nil {
		t.Fatal(err)
	}
	if _, _, err = l.Inherited(); err == nil {
		t.Fatal("closed inheritance reused")
	}
	if err = l.Close(); err != nil {
		t.Fatal(err)
	}
	if err = l.Close(); err != nil {
		t.Fatal("close is not idempotent", err)
	}
}

// Count actual descriptors over repeated construction/teardown, not GC-based
// recovery. This is an FD lifecycle check, NOT a whole-kernel memory quote.
func TestLaunchDescriptorLifecycleReturnsToBaseline(t *testing.T) {
	warm, parent := launchFixture(t)
	if err := warm.Close(); err != nil {
		t.Fatal(err)
	}
	count := func() int {
		t.Helper()
		entries, err := os.ReadDir("/proc/self/fd")
		if err != nil {
			t.Fatal(err)
		}
		return len(entries)
	}
	baseline := count()
	for cycle := 0; cycle < 20; cycle++ {
		l, err := PrepareLaunch(parent)
		if l != nil {
			t.Cleanup(func() { _ = l.Close() })
		}
		if err != nil {
			t.Fatal(err)
		}
		if got := count() - baseline; got != 3 {
			t.Fatalf("cycle %d: prepared FD delta=%d want 3", cycle, got)
		}
		if err = l.CloseInherited(); err != nil {
			t.Fatal(err)
		}
		if got := count() - baseline; got != 1 {
			t.Fatalf("cycle %d: after inheritance close FD delta=%d want 1", cycle, got)
		}
		if err = l.Close(); err != nil {
			t.Fatal(err)
		}
		if got := count() - baseline; got != 0 {
			t.Fatalf("cycle %d: after teardown FD delta=%d want 0", cycle, got)
		}
	}
	t.Log("20 cycles: prepared FD delta=3, inheritance closed=1, teardown=0; kernel memory footprint not inferred")
}
