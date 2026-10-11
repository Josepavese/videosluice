//go:build linux

package linux

import (
	peer "github.com/Josepavese/videosluice/framepeer"
	"golang.org/x/sys/unix"
	"io"
	"os"
	"strconv"
	"strings"
)

// ProcessIdentity is Linux provider metadata, never a domain-facing authority.
// It MUST arrive together with the capability on a trusted launcher bootstrap
// or authenticated control channel. Import does not authenticate the sender.
// PID namespaces must agree; mismatches fail closed rather than being guessed.
type ProcessIdentity struct {
	PID        int
	UID, GID   uint32
	Generation string
}

// Export duplicates the exact pinned process handle with CLOEXEC. The caller
// owns the returned file and must charge/close it; no FD is cached or queued.
func (l *Lease) Export() (*os.File, ProcessIdentity, error) {
	if _, err := l.CurrentGeneration(); err != nil {
		return nil, ProcessIdentity{}, err
	}
	fd := -1
	var duplicateErr error
	err := l.process.WithHandle(func(handle uintptr) { fd, duplicateErr = unix.FcntlInt(handle, unix.F_DUPFD_CLOEXEC, 0) })
	if err != nil || duplicateErr != nil {
		return nil, ProcessIdentity{}, failure("capability_duplicate", peer.ErrUnavailable)
	}
	file := os.NewFile(uintptr(fd), "framepeer-process-capability")
	if _, err = l.CurrentGeneration(); err != nil {
		_ = file.Close()
		return nil, ProcessIdentity{}, err
	}
	return file, ProcessIdentity{PID: l.pid, UID: l.uid, GID: l.gid, Generation: l.generation}, nil
}

type importedHandle struct{ file *os.File }

func (h importedHandle) WithHandle(visit func(uintptr)) error {
	raw, err := h.file.SyscallConn()
	if err != nil {
		return err
	}
	return raw.Control(visit)
}

func inspectCapability(handle uintptr, expectedPID int) error {
	// Both reads refer to this already pinned descriptor, never /proc/<peerPID>.
	// A regular file, socket, or memfd cannot supply kernel pidfd identity.
	number := strconv.FormatUint(uint64(handle), 10)
	link, err := os.Readlink("/proc/self/fd/" + number)
	if err != nil {
		return failure("capability_descriptor", peer.ErrUnavailable)
	}
	if link != "anon_inode:[pidfd]" {
		return failure("capability_type", peer.ErrInvalid)
	}
	file, err := os.Open("/proc/self/fdinfo/" + number)
	if err != nil {
		return failure("capability_fdinfo", peer.ErrUnavailable)
	}
	data, readErr := io.ReadAll(io.LimitReader(file, 4097))
	closeErr := file.Close()
	if readErr != nil || closeErr != nil || len(data) > 4096 {
		return failure("capability_fdinfo", peer.ErrUnavailable)
	}
	found := false
	for _, line := range strings.Split(string(data), "\n") {
		if !strings.HasPrefix(line, "Pid:") {
			continue
		}
		if found {
			return failure("capability_pid", peer.ErrInvalid)
		}
		found = true
		pid, err := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(line, "Pid:")))
		if err == nil && pid < 0 {
			return failure("capability_retired", peer.ErrRetired)
		}
		if err != nil || pid != expectedPID {
			return failure("capability_pid", peer.ErrUnauthorized)
		}
	}
	if !found {
		return failure("capability_fdinfo", peer.ErrUnavailable)
	}
	return nil
}

// ImportProcessCapability takes exclusive ownership of file on SUCCESS only.
// The caller closes it on error. It must be received from a trusted launch
// boundary, not the normal frame request channel (which rejects every FD).
// Validation binds metadata to the actual pidfd, then checks execution liveness.
func ImportProcessCapability(file *os.File, identity ProcessIdentity) (*Lease, error) {
	if file == nil || identity.PID <= 0 || !validGeneration(identity.Generation) {
		return nil, peer.ErrInvalid
	}
	imported := importedHandle{file: file}
	var validation error
	err := imported.WithHandle(func(handle uintptr) {
		// Inherited bootstrap FDs intentionally survive the initial exec. They
		// must not leak into later subprocesses after admission. Dynamic control
		// receive must additionally use atomic MSG_CMSG_CLOEXEC at receipt.
		flags, e := unix.FcntlInt(handle, unix.F_GETFD, 0)
		if e == nil {
			_, e = unix.FcntlInt(handle, unix.F_SETFD, flags|unix.FD_CLOEXEC)
		}
		if e != nil {
			validation = failure("capability_cloexec", peer.ErrUnavailable)
			return
		}
		validation = inspectCapability(handle, identity.PID)
	})
	if err != nil {
		return nil, failure("capability_handle", peer.ErrUnavailable)
	}
	if validation != nil {
		return nil, validation
	}
	l := &Lease{process: imported, pid: identity.PID, generation: identity.Generation, uid: identity.UID, gid: identity.GID, closeOwned: file.Close}
	if _, err = l.CurrentGeneration(); err != nil {
		return nil, err
	}
	return l, nil
}
