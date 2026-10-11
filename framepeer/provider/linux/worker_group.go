//go:build linux

package linux

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"syscall"

	peer "github.com/Josepavese/videosluice/framepeer"
	"golang.org/x/sys/unix"
)

// WorkerLimits is a finite process-memory/task boundary, NOT a DMA/SDK quote.
// Kernel memory accounting does not cover every accelerator allocation.
type WorkerLimits struct {
	MemoryBytes uint64
	Tasks       uint64
}

// WorkerGroup creates one leaf below an already delegated, trusted directory.
// It never enables controllers, moves its own process, starts/kills/reaps a
// child, or falls back to post-start migration. Deployment owns delegation.
// The trusted launcher must admit its footprint before construction and retain
// it through Close. Workers/administrators able to modify cgroup policy are
// outside this limit authority boundary; limits are checked before/after Start.
// Close only proves release of THIS limit group, never native DMA reclamation.
type WorkerGroup struct {
	mu         sync.Mutex
	parent     *os.File
	directory  *os.File
	name       string
	limits     WorkerLimits
	command    *exec.Cmd
	created    bool
	configured bool
	started    bool
	closed     bool
	closeErr   error
	// Fixed workspace: even an unexpectedly large process list is rejected.
	workspace [65536]byte
}

func PrepareWorkerGroup(parent *os.File, name string, limits WorkerLimits) (*WorkerGroup, error) {
	if parent == nil || !validGeneration(name) || name == "." || name == ".." ||
		limits.MemoryBytes < 4096 || limits.MemoryBytes > 1<<60 || limits.MemoryBytes%4096 != 0 ||
		limits.Tasks == 0 || limits.Tasks > 4096 {
		return nil, peer.ErrInvalid
	}
	var fs unix.Statfs_t
	if err := unix.Fstatfs(int(parent.Fd()), &fs); err != nil || fs.Type != unix.CGROUP2_SUPER_MAGIC {
		return nil, failure("worker_group_filesystem", peer.ErrUnavailable)
	}
	fd, err := unix.FcntlInt(parent.Fd(), unix.F_DUPFD_CLOEXEC, 0)
	if err != nil {
		return nil, failure("worker_group_parent", err)
	}
	g := &WorkerGroup{parent: os.NewFile(uintptr(fd), "worker-group-parent"), name: name, limits: limits}
	fail := func(cause error) (*WorkerGroup, error) {
		if closeErr := g.Close(); closeErr != nil {
			return g, errors.Join(cause, closeErr) // ownership not lost on rollback failure
		}
		return nil, cause
	}
	if err = unix.Mkdirat(fd, name, 0700); err != nil {
		return fail(failure("worker_group_create", err)) // EEXIST must never adopt/delete another group
	}
	g.created = true
	child, err := unix.Openat(fd, name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return g, failure("worker_group_open", err) // retain exact target; no blind unlink
	}
	g.directory = os.NewFile(uintptr(child), "worker-group")
	for _, item := range g.policy() {
		if err = g.write(item[0], item[1]); err != nil {
			return fail(failure("worker_group_configure_"+item[0], err))
		}
	}
	if err = g.verify(); err != nil {
		return fail(err)
	}
	g.configured = true
	return g, nil
}

func (g *WorkerGroup) policy() [5][2]string {
	return [5][2]string{
		{"memory.max", strconv.FormatUint(g.limits.MemoryBytes, 10)},
		{"memory.swap.max", "0"},
		{"memory.oom.group", "1"},
		{"pids.max", strconv.FormatUint(g.limits.Tasks, 10)},
		{"cgroup.max.descendants", "0"},
	}
}

func (g *WorkerGroup) write(name, value string) error {
	fd, err := unix.Openat(int(g.directory.Fd()), name, unix.O_WRONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return err
	}
	f := os.NewFile(uintptr(fd), "worker-policy")
	n, err := f.WriteString(value)
	if err == nil && n != len(value) {
		err = io.ErrShortWrite
	}
	return errors.Join(err, f.Close())
}

func (g *WorkerGroup) read(name string) (string, error) {
	fd, err := unix.Openat(int(g.directory.Fd()), name, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return "", err
	}
	f := os.NewFile(uintptr(fd), "worker-policy")
	n, readErr := io.ReadFull(f, g.workspace[:])
	closeErr := f.Close()
	if readErr != io.EOF && readErr != io.ErrUnexpectedEOF {
		return "", errors.Join(peer.ErrInvalid, readErr, closeErr)
	}
	return strings.TrimSpace(string(g.workspace[:n])), closeErr
}

// Bind the directory descriptor to its exact owned name, not merely a pathname.
func (g *WorkerGroup) identity() error {
	if g.directory == nil || g.parent == nil {
		return peer.ErrUnavailable
	}
	var owned, named unix.Stat_t
	if err := unix.Fstat(int(g.directory.Fd()), &owned); err != nil {
		return err
	}
	if err := unix.Fstatat(int(g.parent.Fd()), g.name, &named, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return err
	}
	if owned.Dev != named.Dev || owned.Ino != named.Ino || named.Mode&unix.S_IFMT != unix.S_IFDIR {
		return peer.ErrInvalid
	}
	return nil
}

func (g *WorkerGroup) verify() error {
	if err := g.identity(); err != nil {
		return failure("worker_group_identity", err)
	}
	for _, item := range g.policy() {
		actual, err := g.read(item[0])
		if err != nil || actual != item[1] {
			return failure("worker_group_policy_"+item[0], errors.Join(peer.ErrUnavailable, err))
		}
	}
	return nil
}

func (g *WorkerGroup) empty() error {
	events, err := g.read("cgroup.events")
	if err != nil {
		return failure("worker_group_population", err)
	}
	for _, line := range strings.Split(events, "\n") {
		if line == "populated 0" {
			return nil
		}
	}
	return failure("worker_group_populated", peer.ErrBusy)
}

// Attach requests atomic placement via clone3/CLONE_INTO_CGROUP before child
// code executes. Unsupported kernels/permissions fail Start, never run unbound.
func (g *WorkerGroup) Attach(cmd *exec.Cmd) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed || cmd == nil || cmd.Process != nil || g.command != nil || !g.configured {
		return peer.ErrInvalid
	}
	if err := g.verify(); err != nil {
		return err
	}
	if err := g.empty(); err != nil {
		return err
	}
	attributes := syscall.SysProcAttr{}
	if cmd.SysProcAttr != nil {
		attributes = *cmd.SysProcAttr
	}
	if attributes.UseCgroupFD || attributes.CgroupFD != 0 {
		return failure("worker_group_existing_attachment", peer.ErrInvalid)
	}
	attributes.UseCgroupFD = true
	attributes.CgroupFD = int(g.directory.Fd())
	cmd.SysProcAttr = &attributes
	g.command = cmd
	return nil
}

func (g *WorkerGroup) Started(process *os.Process) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed || g.started || process == nil || g.command == nil || g.command.Process != process {
		return peer.ErrInvalid
	}
	g.started = true // no replay even if confirmation fails
	if err := g.verify(); err != nil {
		return err
	}
	procs, err := g.read("cgroup.procs")
	if err != nil {
		return failure("worker_group_membership", err)
	}
	pid := strconv.Itoa(process.Pid)
	for _, member := range strings.Fields(procs) {
		if member == pid {
			return nil
		}
	}
	return failure("worker_group_membership", peer.ErrUnavailable)
}

// Close has no timeout/retry or kill side effect. Live descendants remain an
// explicit error, even after the main process was reaped. Cached errors retain
// the group/handles for the outer resource ledger's quarantine. Empty/rmdir is
// NOT evidence that DMA or residual page-cache charges have been reclaimed.
func (g *WorkerGroup) Close() error {
	if g == nil {
		return nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return g.closeErr
	}
	if g.command != nil && g.command.Process != nil && g.command.ProcessState == nil {
		return failure("worker_group_not_reaped", peer.ErrBusy)
	}
	g.closed = true
	fail := func(stage string, err error) error {
		g.closeErr = failure(stage, err)
		return g.closeErr
	}
	if g.created {
		if err := g.identity(); err != nil {
			return fail("worker_group_close_identity", err)
		}
		if g.configured {
			if err := g.verify(); err != nil {
				return fail("worker_group_close_policy", err)
			}
		}
		if err := g.empty(); err != nil {
			return fail("worker_group_close_population", err)
		}
		if err := unix.Unlinkat(int(g.parent.Fd()), g.name, unix.AT_REMOVEDIR); err != nil {
			return fail("worker_group_remove", err)
		}
		g.created = false
	}
	if g.directory != nil {
		g.closeErr = errors.Join(g.closeErr, g.directory.Close())
		g.directory = nil
	}
	if g.parent != nil {
		g.closeErr = errors.Join(g.closeErr, g.parent.Close())
		g.parent = nil
	}
	if g.closeErr != nil {
		g.closeErr = fmt.Errorf("worker group handle release: %w", g.closeErr)
	}
	return g.closeErr
}
