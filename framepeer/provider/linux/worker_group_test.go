//go:build linux

package linux

import (
	"bufio"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	peer "github.com/Josepavese/videosluice/framepeer"
)

func TestWorkerGroupRejectsOrdinaryDirectoryAndUnboundedPolicies(t *testing.T) {
	directory, err := os.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer directory.Close()
	for _, limits := range []WorkerLimits{{}, {MemoryBytes: 4096}, {MemoryBytes: 4096, Tasks: 4097}, {MemoryBytes: 4097, Tasks: 1}} {
		if g, err := PrepareWorkerGroup(directory, "worker", limits); g != nil || !errors.Is(err, peer.ErrInvalid) {
			t.Fatal(g, err)
		}
	}
	for _, name := range []string{"", "..", "../outside", "a/b"} {
		if g, err := PrepareWorkerGroup(directory, name, WorkerLimits{16 << 20, 4}); g != nil || !errors.Is(err, peer.ErrInvalid) {
			t.Fatal(g, err)
		}
	}
	if g, err := PrepareWorkerGroup(directory, "worker", WorkerLimits{16 << 20, 4}); g != nil || !errors.Is(err, peer.ErrUnavailable) {
		t.Fatal(g, err)
	}
}

// Explicit opt-in inside a dedicated transient user service ONLY. No production
// hierarchy is modified and lack of delegation is not a passing kernel test.
func workerGroupKernelParent(t *testing.T) *os.File {
	t.Helper()
	if os.Getenv("FRAMEPEER_CGROUP_TEST") != "1" {
		t.Skip("dedicated delegated cgroup fixture not requested")
	}
	data, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		t.Fatal(err)
	}
	var group string
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "0::/") {
			group = strings.TrimPrefix(line, "0::")
		}
	}
	if !strings.HasPrefix(filepath.Base(group), "framepeer-worker-test-") || !strings.HasSuffix(group, ".service") {
		t.Fatalf("not the isolated fixture: %q", group)
	}
	path := filepath.Join("/sys/fs/cgroup", group)
	// Fixture controller occupies its own leaf before enabling child policy.
	if err := os.Mkdir(filepath.Join(path, "controller"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(path, "controller/cgroup.procs"), []byte(strconv.Itoa(os.Getpid())), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(path, "cgroup.subtree_control"), []byte("+memory +pids"), 0600); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	return f
}

func TestWorkerGroupKernel(t *testing.T) {
	parent := workerGroupKernelParent(t)
	binary := filepath.Join(t.TempDir(), "worker-probe")
	build := exec.Command("cc", "-std=c11", "-O1", "-Wall", "-Wextra", "-Werror", "native/tests/worker_group_probe.c", "-o", binary)
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, output)
	}
	limits := WorkerLimits{MemoryBytes: 16 << 20, Tasks: 4}
	for _, mode := range []string{"wait", "fork", "memory"} {
		t.Run(mode, func(t *testing.T) {
			g, err := PrepareWorkerGroup(parent, mode, limits)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, binary, mode)
			cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
			stdout, err := cmd.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			stdin, err := cmd.StdinPipe()
			if err != nil {
				t.Fatal(err)
			}
			if err = g.Attach(cmd); err != nil {
				t.Fatal(err)
			}
			if !cmd.SysProcAttr.Setpgid {
				t.Fatal("preexisting process options lost")
			}
			if err = cmd.Start(); err != nil {
				_ = g.Close()
				t.Fatal(err)
			}
			// memory may already be OOM-killed before Started; use output from
			// its first instructions for birth placement, not a later migration.
			if mode != "memory" {
				if err = g.Started(cmd.Process); err != nil {
					t.Fatal(err)
				}
				if err = g.Started(cmd.Process); !errors.Is(err, peer.ErrInvalid) {
					t.Fatal("Started replay admitted", err)
				}
			}
			if err = g.Close(); !errors.Is(err, peer.ErrBusy) {
				t.Fatal("unreaped process released", err)
			}
			scanner := bufio.NewScanner(stdout)
			seen := false
			for scanner.Scan() {
				line := scanner.Text()
				if strings.HasPrefix(line, "0::") && strings.HasSuffix(line, "/"+mode) {
					seen = true
				}
				if line == "READY" {
					break
				}
			}
			if !seen {
				t.Fatal("child was not born inside admitted group")
			}
			if mode == "fork" {
				if !scanner.Scan() || scanner.Text() != "FORKED 3 ERRNO 11" {
					t.Fatal("pids limit not enforced", scanner.Text())
				}
			}
			_ = stdin.Close()
			waitErr := cmd.Wait()
			if mode == "memory" {
				status, ok := cmd.ProcessState.Sys().(syscall.WaitStatus)
				if waitErr == nil || !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
					t.Fatal("memory cap did not stop allocation", waitErr)
				}
				events, err := g.read("memory.events")
				var kills uint64
				for _, line := range strings.Split(events, "\n") {
					fields := strings.Fields(line)
					if len(fields) == 2 && fields[0] == "oom_kill" {
						kills, err = strconv.ParseUint(fields[1], 10, 64)
						break
					}
				}
				if err != nil || kills == 0 {
					t.Fatal("no kernel OOM evidence", events, err)
				}
				t.Logf("kernel OOM kill evidence=%d; child terminated by SIGKILL", kills)
			} else if waitErr != nil {
				t.Fatal(waitErr)
			}
			if err = g.Close(); err != nil {
				t.Fatal(err)
			}
			if err = g.Close(); err != nil {
				t.Fatal("non-idempotent close", err)
			}
			t.Logf("atomic birth placement; mode=%s memory.max=%d swap.max=0 pids.max=4 empty group removed", mode, limits.MemoryBytes)
		})
	}

	t.Run("failed_start_and_existing_group", func(t *testing.T) {
		g, err := PrepareWorkerGroup(parent, "failed-start", limits)
		if err != nil {
			t.Fatal(err)
		}
		if duplicate, err := PrepareWorkerGroup(parent, "failed-start", limits); duplicate != nil || err == nil {
			t.Fatal("existing group adopted")
		}
		if err := g.verify(); err != nil {
			t.Fatal("duplicate attempt damaged owner", err)
		}
		cmd := exec.Command("/no/such/worker-binary")
		if err := g.Attach(cmd); err != nil {
			t.Fatal(err)
		}
		if cmd.Start() == nil {
			t.Fatal("missing executable started")
		}
		if err := g.Close(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("policy_mutation_quarantined", func(t *testing.T) {
		g, err := PrepareWorkerGroup(parent, "mutation", limits)
		if err != nil {
			t.Fatal(err)
		}
		if err := g.write("memory.max", "max"); err != nil {
			t.Fatal(err)
		}
		if err := g.Attach(exec.Command(binary, "wait")); err == nil {
			t.Fatal("mutated policy admitted")
		}
		if err := g.Close(); err == nil {
			t.Fatal("mutated policy reported released")
		}
		if g.directory == nil || !g.created {
			t.Fatal("quarantined ownership erased")
		}
		// The enclosing disposable unit owns final fixture cleanup. Never reset
		// the provider's error just to claim a successful release.
	})
}
