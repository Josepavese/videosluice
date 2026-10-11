package broker

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	brokerSubShmMagic      = "NMSH"
	brokerSubShmVersion    = 2
	brokerSubShmHeaderSize = 64
	brokerSubShmStreamNV12 = brokerStreamNV12
)

type brokerSubSharedFrame struct {
	mu          sync.Mutex
	path        string
	privatePath string
	identity    os.FileInfo
	file        *os.File
	payloadCap  int
	seq         uint64
	epoch       uint64
	header      []byte
}

var brokerSubSharedPathMu sync.Mutex

func newBrokerSubSharedFrameEpoch() uint64 {
	const maxExactJSONInteger = uint64(1<<53) - 1
	var encoded [8]byte
	if _, err := rand.Read(encoded[:]); err == nil {
		if epoch := binary.BigEndian.Uint64(encoded[:]) & maxExactJSONInteger; epoch != 0 {
			return epoch
		}
	}
	epoch := uint64(time.Now().UnixNano()) & maxExactJSONInteger
	if epoch == 0 {
		return 1
	}
	return epoch
}

func newBrokerSubSharedFrame(path string, payloadCap int) (*brokerSubSharedFrame, error) {
	path = filepath.Clean(path)
	if !filepath.IsAbs(path) || path == "/" || payloadCap <= 0 {
		return nil, errors.New("shared-frame path or capacity is invalid")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return nil, err
	}
	privatePath, err := newBrokerSubSharedPrivatePath(filepath.Dir(path))
	if err != nil {
		return nil, err
	}
	f, err := os.OpenFile(privatePath, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o660)
	if err != nil {
		return nil, err
	}
	cleanup := true
	defer func() {
		if cleanup {
			_ = f.Close()
			_ = os.Remove(privatePath)
		}
	}()
	// The service runs with UMask=0027, so the create mode alone would leave
	// this cross-process frame transport at 0640 and make it unreadable by the
	// tracker service group.  Apply the protocol mode to the opened inode so
	// the runtime contract does not depend on the launcher or platform umask.
	if err := f.Chmod(0o660); err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return nil, fmt.Errorf("acquire shared-frame producer lease: %w", err)
	}
	if err := f.Truncate(int64(brokerSubShmHeaderSize + payloadCap)); err != nil {
		return nil, err
	}
	identity, err := f.Stat()
	if err != nil || !identity.Mode().IsRegular() {
		return nil, errors.New("shared-frame private identity is invalid")
	}
	if err := publishBrokerSubSharedPath(privatePath, path, identity); err != nil {
		return nil, err
	}
	cleanup = false
	return &brokerSubSharedFrame{
		path:        path,
		privatePath: privatePath,
		identity:    identity,
		file:        f,
		payloadCap:  payloadCap,
		epoch:       newBrokerSubSharedFrameEpoch(),
		header:      make([]byte, brokerSubShmHeaderSize),
	}, nil
}

func newBrokerSubSharedPrivatePath(directory string) (string, error) {
	for attempt := 0; attempt < 8; attempt++ {
		nonce := make([]byte, 6)
		if _, err := rand.Read(nonce); err != nil {
			return "", fmt.Errorf("create shared-frame identity: %w", err)
		}
		path := filepath.Join(directory, ".vb-shm-"+hex.EncodeToString(nonce))
		if _, err := os.Lstat(path); errors.Is(err, os.ErrNotExist) {
			return path, nil
		} else if err != nil {
			return "", err
		}
	}
	return "", errors.New("could not allocate shared-frame private identity")
}

func publishBrokerSubSharedPath(privatePath, target string, identity os.FileInfo) error {
	brokerSubSharedPathMu.Lock()
	defer brokerSubSharedPathMu.Unlock()
	var previous os.FileInfo
	var previousLease *os.File
	defer func() {
		if previousLease != nil {
			_ = previousLease.Close()
		}
	}()
	if info, err := os.Lstat(target); err == nil {
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() ||
			info.Mode().Perm()&0o002 != 0 || !brokerSubSharedOwned(info) {
			return errors.New("shared-frame target metadata is unsafe")
		}
		lease, live, err := brokerSubSharedStaleLease(target, info)
		if err != nil {
			return err
		}
		if live {
			return errors.New("shared-frame target already has a live producer")
		}
		previousLease = lease
		previous = info
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	temporary, err := newBrokerSubSharedPrivatePath(filepath.Dir(target))
	if err != nil {
		return err
	}
	if err := os.Link(privatePath, temporary); err != nil {
		return err
	}
	cleanupTemporary := true
	defer func() {
		if cleanupTemporary {
			_ = removeBrokerSubSharedIfSame(temporary, identity)
		}
	}()
	if err := os.Rename(temporary, target); err != nil {
		return err
	}
	cleanupTemporary = false
	if previous != nil {
		_ = removeBrokerSubSharedSiblingLinks(filepath.Dir(target), previous)
	}
	published, err := os.Lstat(target)
	if err != nil || !os.SameFile(identity, published) {
		return errors.New("shared-frame publication identity mismatch")
	}
	return nil
}

func brokerSubSharedStaleLease(path string, identity os.FileInfo) (*os.File, bool, error) {
	file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_CLOEXEC|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, false, fmt.Errorf("open shared-frame target: %w", err)
	}
	opened, err := file.Stat()
	if err != nil || !os.SameFile(identity, opened) {
		_ = file.Close()
		return nil, false, errors.New("shared-frame target changed while checking producer lease")
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = file.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return nil, true, nil
		}
		return nil, false, fmt.Errorf("inspect shared-frame producer lease: %w", err)
	}
	return file, false, nil
}

func brokerSubSharedOwned(info os.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && stat.Uid == uint32(os.Geteuid())
}

func removeBrokerSubSharedSiblingLinks(directory string, identity os.FileInfo) error {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		path := filepath.Join(directory, entry.Name())
		info, err := os.Lstat(path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		if info.Mode().IsRegular() && os.SameFile(identity, info) {
			if err := removeBrokerSubSharedIfSame(path, identity); err != nil {
				return err
			}
		}
	}
	return nil
}

func removeBrokerSubSharedIfSame(path string, identity os.FileInfo) error {
	if identity == nil || strings.TrimSpace(path) == "" {
		return nil
	}
	current, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if !os.SameFile(identity, current) {
		return nil
	}
	return os.Remove(path)
}

func (s *brokerSubSharedFrame) Epoch() uint64 {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.epoch
}

func (s *brokerSubSharedFrame) RotateEpoch() uint64 {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.epoch = newBrokerSubSharedFrameEpoch()
	s.seq = 0
	return s.epoch
}

func (s *brokerSubSharedFrame) Close() error {
	if s == nil {
		return nil
	}
	brokerSubSharedPathMu.Lock()
	defer brokerSubSharedPathMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	var closeErr error
	if s.file != nil {
		closeErr = s.file.Close()
		s.file = nil
	}
	removeErr := errors.Join(
		removeBrokerSubSharedIfSame(s.path, s.identity),
		removeBrokerSubSharedIfSame(s.privatePath, s.identity),
	)
	return errors.Join(closeErr, removeErr)
}

// Replace atomically publishes this prepared frame over the incumbent's
// logical path. Both files retain a private hard link so rollback can perform
// the same identity-bound operation in the opposite direction.
func (s *brokerSubSharedFrame) Replace(incumbent *brokerSubSharedFrame) error {
	if s == nil || incumbent == nil || s == incumbent || s.identity == nil || incumbent.identity == nil {
		return errors.New("shared-frame replacement owners are invalid")
	}
	brokerSubSharedPathMu.Lock()
	defer brokerSubSharedPathMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	incumbent.mu.Lock()
	defer incumbent.mu.Unlock()
	target := filepath.Clean(incumbent.path)
	if !filepath.IsAbs(target) || target == "/" ||
		filepath.Dir(s.privatePath) != filepath.Dir(target) {
		return errors.New("shared-frame replacement path is invalid")
	}
	current, err := os.Lstat(target)
	if err != nil {
		return fmt.Errorf("inspect shared-frame replacement target: %w", err)
	}
	if !current.Mode().IsRegular() || current.Mode()&os.ModeSymlink != 0 {
		return errors.New("shared-frame replacement target is not a regular file")
	}
	if os.SameFile(current, s.identity) {
		s.path = target
		return nil
	}
	if !os.SameFile(current, incumbent.identity) {
		return errors.New("shared-frame replacement target changed owner")
	}
	private, err := os.Lstat(s.privatePath)
	if err != nil || !private.Mode().IsRegular() || !os.SameFile(private, s.identity) || s.file == nil {
		return errors.New("shared-frame successor private identity is unavailable")
	}
	temporary, err := newBrokerSubSharedPrivatePath(filepath.Dir(target))
	if err != nil {
		return err
	}
	if err := os.Link(s.privatePath, temporary); err != nil {
		return fmt.Errorf("stage shared-frame replacement: %w", err)
	}
	cleanupTemporary := true
	defer func() {
		if cleanupTemporary {
			_ = removeBrokerSubSharedIfSame(temporary, s.identity)
		}
	}()
	if err := os.Rename(temporary, target); err != nil {
		return fmt.Errorf("activate shared-frame replacement: %w", err)
	}
	cleanupTemporary = false
	previousPath := s.path
	s.path = target
	published, err := os.Lstat(target)
	if err != nil || !os.SameFile(s.identity, published) {
		return errors.New("shared-frame replacement identity mismatch")
	}
	if previousPath != target {
		if err := removeBrokerSubSharedIfSame(previousPath, s.identity); err != nil {
			return fmt.Errorf("remove previous shared-frame alias: %w", err)
		}
	}
	return nil
}

func (s *brokerSubSharedFrame) Owns(path string) bool {
	if s == nil || s.identity == nil {
		return false
	}
	brokerSubSharedPathMu.Lock()
	defer brokerSubSharedPathMu.Unlock()
	info, err := os.Lstat(filepath.Clean(path))
	return err == nil && info.Mode().IsRegular() && os.SameFile(s.identity, info)
}

func (s *brokerSubSharedFrame) Write(frameID uint32, timestampNS uint64, width uint32, height uint32, flags uint16, payload []byte) error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	// Reporting success without writing would make the caller notify a frame it
	// never published: the consumer would read the previous frame's pixels under
	// a new frame ID and timestamp, with the write metric counting a write that
	// did not happen. Returning the failure lets dispatchFrame log it and count
	// a write error instead of a write.
	if len(payload) == 0 {
		return errors.New("analysis shared frame payload is empty")
	}
	if s.file == nil {
		return errors.New("analysis shared frame is closed")
	}
	if len(payload) > s.payloadCap {
		return fmt.Errorf("analysis shared frame payload %d exceeds capacity %d", len(payload), s.payloadCap)
	}

	odd := s.seq + 1
	even := s.seq + 2

	header := s.header
	for i := range header {
		header[i] = 0
	}
	copy(header[:4], []byte(brokerSubShmMagic))
	header[4] = brokerSubShmVersion
	header[5] = brokerSubShmStreamNV12
	binary.BigEndian.PutUint16(header[6:8], flags)
	binary.BigEndian.PutUint64(header[8:16], odd)
	binary.BigEndian.PutUint64(header[16:24], odd)
	binary.BigEndian.PutUint64(header[24:32], uint64(frameID))
	binary.BigEndian.PutUint64(header[32:40], timestampNS)
	binary.BigEndian.PutUint32(header[40:44], width)
	binary.BigEndian.PutUint32(header[44:48], height)
	binary.BigEndian.PutUint32(header[48:52], uint32(len(payload)))
	binary.BigEndian.PutUint32(header[52:56], uint32(s.payloadCap))
	binary.BigEndian.PutUint64(header[56:64], s.epoch)

	if _, err := s.file.WriteAt(header, 0); err != nil {
		return err
	}
	if _, err := s.file.WriteAt(payload, brokerSubShmHeaderSize); err != nil {
		return err
	}
	binary.BigEndian.PutUint64(header[16:24], even)
	if _, err := s.file.WriteAt(header[16:24], 16); err != nil {
		return err
	}
	binary.BigEndian.PutUint64(header[8:16], even)
	if _, err := s.file.WriteAt(header[8:16], 8); err != nil {
		return err
	}
	s.seq = even
	return nil
}
