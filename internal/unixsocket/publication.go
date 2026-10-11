// Package unixsocket owns safe publication and teardown of Unix listeners.
package unixsocket

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

// Publication binds a private socket inode and exposes it through one stable
// logical hard link. Delayed teardown cannot unlink a successor because every
// removal is checked against the recorded inode identity.
type Publication struct {
	listener    net.Listener
	logicalPath string
	privatePath string
	identity    os.FileInfo
	cleanupOnce sync.Once
	cleanupErr  error
}

var publicationPathMu sync.Mutex

// Listen publishes a new Unix stream listener. A live owner, symlink or
// non-socket path is rejected; an unreachable stale socket and all of its
// same-inode aliases in the directory are reclaimed.
func Listen(path string) (*Publication, error) {
	path = filepath.Clean(path)
	if !filepath.IsAbs(path) || path == "/" {
		return nil, errors.New("unix socket path must be absolute and non-root")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return nil, err
	}
	publicationPathMu.Lock()
	defer publicationPathMu.Unlock()
	if err := removeStale(path); err != nil {
		return nil, err
	}
	privatePath, err := newPrivatePath(filepath.Dir(path))
	if err != nil {
		return nil, err
	}
	listener, err := net.Listen("unix", privatePath)
	if err != nil {
		return nil, err
	}
	cleanupPrivate := true
	defer func() {
		if cleanupPrivate {
			_ = listener.Close()
			_ = os.Remove(privatePath)
		}
	}()
	if err := os.Chmod(privatePath, 0o660); err != nil {
		return nil, err
	}
	identity, err := os.Lstat(privatePath)
	if err != nil || identity.Mode()&os.ModeSocket == 0 {
		return nil, errors.New("private endpoint is not a unix socket")
	}
	if err := os.Link(privatePath, path); err != nil {
		return nil, fmt.Errorf("publish unix socket: %w", err)
	}
	published, err := os.Lstat(path)
	if err != nil || !os.SameFile(identity, published) {
		_ = removeIfSame(path, identity)
		return nil, errors.New("unix socket publication identity mismatch")
	}
	cleanupPrivate = false
	return &Publication{
		listener: listener, logicalPath: path, privatePath: privatePath, identity: identity,
	}, nil
}

// ListenSingleOwner publishes a non-replaceable control listener with exactly
// one pathname and owner-only permissions. Unlike Listen, it intentionally
// retains no private hard link: control endpoints stay stable for the lifetime
// of their server and are never part of a media cutover transaction.
func ListenSingleOwner(path string) (*Publication, error) {
	return listenSingleOwner(path, "unix", nil, false)
}

func listenSingleOwner(path, network string, prepare func(*net.UnixListener) error, retainPartial bool) (publication *Publication, result error) {
	path = filepath.Clean(path)
	if !filepath.IsAbs(path) || path == "/" {
		return nil, errors.New("unix socket path must be absolute and non-root")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return nil, err
	}
	publicationPathMu.Lock()
	defer publicationPathMu.Unlock()
	if err := removeStaleNetwork(path, network); err != nil {
		return nil, err
	}
	listener, err := net.Listen(network, path)
	if err != nil {
		return nil, err
	}
	unixListener, ok := listener.(*net.UnixListener)
	if !ok {
		_ = listener.Close()
		return nil, errors.New("single-owner endpoint is not a unix listener")
	}
	unixListener.SetUnlinkOnClose(false)
	owner := &Publication{listener: listener, logicalPath: path}
	defer func() {
		if result != nil && retainPartial {
			publication = owner
		}
	}()
	identity, err := os.Lstat(path)
	owner.identity = identity
	if err != nil || identity.Mode()&os.ModeSocket == 0 {
		if !retainPartial {
			_ = listener.Close()
		}
		return nil, errors.New("single-owner endpoint is not a unix socket")
	}
	cleanup := true
	defer func() {
		if cleanup && !retainPartial {
			_ = listener.Close()
			_ = removeIfSame(path, identity)
		}
	}()
	if prepare != nil {
		if err := prepare(unixListener); err != nil {
			return nil, err
		}
	}
	if err := os.Chmod(path, 0o600); err != nil {
		return nil, err
	}
	published, err := os.Lstat(path)
	if err != nil {
		return nil, errors.New("single-owner unix socket publication disappeared")
	}
	metadata, metadataOK := published.Sys().(*syscall.Stat_t)
	if published.Mode()&os.ModeSocket == 0 ||
		!os.SameFile(identity, published) || published.Mode().Perm() != 0o600 ||
		!metadataOK || metadata.Nlink != 1 {
		return nil, errors.New("single-owner unix socket publication metadata mismatch")
	}
	cleanup = false
	return owner, nil
}

func newPrivatePath(directory string) (string, error) {
	for attempt := 0; attempt < 8; attempt++ {
		nonce := make([]byte, 6)
		if _, err := rand.Read(nonce); err != nil {
			return "", fmt.Errorf("create unix socket identity: %w", err)
		}
		path := filepath.Join(directory, ".vb-"+hex.EncodeToString(nonce))
		if _, err := os.Lstat(path); errors.Is(err, os.ErrNotExist) {
			return path, nil
		} else if err != nil {
			return "", fmt.Errorf("inspect private unix socket: %w", err)
		}
	}
	return "", errors.New("could not allocate a unique unix socket identity")
}

func removeStale(path string) error {
	return removeStaleNetwork(path, "unix")
}

func removeStaleNetwork(path, network string) error {
	identity, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect unix socket: %w", err)
	}
	if identity.Mode()&os.ModeSymlink != 0 || identity.Mode()&os.ModeSocket == 0 {
		return errors.New("unix socket path is occupied by a non-socket")
	}
	connection, dialErr := net.DialTimeout(network, path, 100*time.Millisecond)
	if dialErr == nil {
		_ = connection.Close()
		return errors.New("unix socket path already has a live listener")
	}
	if !errors.Is(dialErr, syscall.ECONNREFUSED) && !errors.Is(dialErr, os.ErrNotExist) {
		return fmt.Errorf("probe existing unix socket: %w", dialErr)
	}
	if err := removeIfSame(path, identity); err != nil {
		return err
	}
	return removeSiblingLinks(filepath.Dir(path), identity)
}

func removeSiblingLinks(directory string, identity os.FileInfo) error {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		candidate := filepath.Join(directory, entry.Name())
		info, err := os.Lstat(candidate)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSocket == 0 || !os.SameFile(identity, info) {
			continue
		}
		if err := removeIfSame(candidate, identity); err != nil {
			return err
		}
	}
	return nil
}

func removeIfSame(path string, identity os.FileInfo) error {
	if identity == nil {
		return errors.New("unix socket identity is required")
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

// Replace atomically moves the successor onto the incumbent's logical path.
// Both listeners keep a private hard link, so the same operation can restore
// the incumbent during rollback without reopening either listener. The target
// must still name the exact incumbent inode; arbitrary files and a newer owner
// are never replaced.
func (publication *Publication) Replace(incumbent *Publication) error {
	if publication == nil || incumbent == nil || publication == incumbent ||
		publication.identity == nil || incumbent.identity == nil ||
		publication.privatePath == "" || incumbent.privatePath == "" {
		return errors.New("unix socket replacement publications are invalid")
	}
	publicationPathMu.Lock()
	defer publicationPathMu.Unlock()
	target := filepath.Clean(incumbent.logicalPath)
	if !filepath.IsAbs(target) || target == "/" {
		return errors.New("unix socket incumbent has no valid logical path")
	}
	if filepath.Dir(publication.privatePath) != filepath.Dir(target) {
		return errors.New("unix socket replacement must stay in one directory")
	}
	current, err := os.Lstat(target)
	if err != nil {
		return fmt.Errorf("inspect unix socket replacement target: %w", err)
	}
	if current.Mode()&os.ModeSocket == 0 || current.Mode()&os.ModeSymlink != 0 {
		return errors.New("unix socket replacement target is not a socket")
	}
	if os.SameFile(current, publication.identity) {
		publication.logicalPath = target
		return nil
	}
	if !os.SameFile(current, incumbent.identity) {
		return errors.New("unix socket replacement target changed owner")
	}
	private, err := os.Lstat(publication.privatePath)
	if err != nil || private.Mode()&os.ModeSocket == 0 ||
		!os.SameFile(private, publication.identity) {
		return errors.New("unix socket successor private identity is unavailable")
	}
	temporary, err := newPrivatePath(filepath.Dir(target))
	if err != nil {
		return err
	}
	if err := os.Link(publication.privatePath, temporary); err != nil {
		return fmt.Errorf("stage unix socket replacement: %w", err)
	}
	cleanupTemporary := true
	defer func() {
		if cleanupTemporary {
			_ = removeIfSame(temporary, publication.identity)
		}
	}()
	if err := os.Rename(temporary, target); err != nil {
		return fmt.Errorf("activate unix socket replacement: %w", err)
	}
	cleanupTemporary = false
	previousLogical := publication.logicalPath
	publication.logicalPath = target
	replaced, err := os.Lstat(target)
	if err != nil || !os.SameFile(publication.identity, replaced) {
		return errors.New("unix socket replacement identity mismatch")
	}
	if previousLogical != target {
		if err := removeIfSame(previousLogical, publication.identity); err != nil {
			return fmt.Errorf("remove previous unix socket alias: %w", err)
		}
	}
	return nil
}

// Owns reports whether path currently resolves to this publication's inode.
func (publication *Publication) Owns(path string) bool {
	if publication == nil || publication.identity == nil {
		return false
	}
	publicationPathMu.Lock()
	defer publicationPathMu.Unlock()
	info, err := os.Lstat(filepath.Clean(path))
	return err == nil && info.Mode()&os.ModeSocket != 0 && os.SameFile(publication.identity, info)
}

func (publication *Publication) Accept() (net.Conn, error) {
	return publication.listener.Accept()
}

func (publication *Publication) Close() error {
	if publication == nil || publication.listener == nil {
		return nil
	}
	return publication.listener.Close()
}

func (publication *Publication) Addr() net.Addr {
	if publication == nil || publication.listener == nil {
		return nil
	}
	return publication.listener.Addr()
}

// Cleanup closes the listener and removes only pathnames that still name this
// publication's inode. It is idempotent.
func (publication *Publication) Cleanup() {
	_ = publication.CleanupChecked()
}

// CleanupChecked retains the release receipt for callers whose resource
// ledger must distinguish confirmed cleanup from a failed physical release.
func (publication *Publication) CleanupChecked() error {
	if publication == nil {
		return nil
	}
	publication.cleanupOnce.Do(func() {
		publicationPathMu.Lock()
		defer publicationPathMu.Unlock()
		closeErr := publication.Close()
		if errors.Is(closeErr, net.ErrClosed) {
			closeErr = nil
		}
		publication.cleanupErr = errors.Join(closeErr, removeIfSame(publication.logicalPath, publication.identity))
		if publication.privatePath != "" {
			publication.cleanupErr = errors.Join(publication.cleanupErr, removeIfSame(publication.privatePath, publication.identity))
		}
	})
	return publication.cleanupErr
}
