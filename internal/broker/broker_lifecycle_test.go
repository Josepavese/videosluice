package broker

import (
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

func TestBrokerSharedFramePublishesExactlyOneCanonicalRollbackLink(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "frame.nv12")
	shared, err := newBrokerSubSharedFrame(path, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer shared.Close()

	logical, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	private, err := os.Lstat(shared.privatePath)
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(logical, private) || filepath.Dir(shared.privatePath) != directory ||
		!regexp.MustCompile(`^\.vb-shm-[0-9a-f]{12}$`).MatchString(filepath.Base(shared.privatePath)) {
		t.Fatalf("shared-memory rollback alias is not canonical: %q", shared.privatePath)
	}
	metadata, ok := logical.Sys().(*syscall.Stat_t)
	if !ok || metadata.Nlink != 2 {
		t.Fatalf("shared-memory link count=%v want=2", metadata)
	}
	links := 0
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		info, err := os.Lstat(filepath.Join(directory, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if os.SameFile(logical, info) {
			links++
		}
	}
	if links != 2 {
		t.Fatalf("accounted shared-memory links=%d want=2", links)
	}
}

func TestBrokerSharedFrameEnforcesProtocolModeUnderServiceUmask(t *testing.T) {
	path := filepath.Join(t.TempDir(), "frame.nv12")
	originalUmask := syscall.Umask(0o027)
	defer syscall.Umask(originalUmask)

	shared, err := newBrokerSubSharedFrame(path, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := shared.Close(); err != nil {
			t.Errorf("close shared frame: %v", err)
		}
	}()

	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0o660 {
		t.Fatalf("shared-memory protocol mode=%#o want=%#o", got, os.FileMode(0o660))
	}
}

func TestBrokerSharedFrameCloseIsConcurrentSafe(t *testing.T) {
	path := filepath.Join(t.TempDir(), "frame.nv12")
	shared, err := newBrokerSubSharedFrame(path, 16)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for frame := 0; frame < 100; frame++ {
				_ = shared.Write(uint32(frame), uint64(frame), 4, 2, 0, make([]byte, 12))
			}
		}(i)
	}
	if err := shared.Close(); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	if err := shared.Close(); err != nil {
		t.Fatalf("second close must be idempotent: %v", err)
	}
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("closed shared-memory publication survived: %v", err)
	}
}

func TestBrokerSharedFrameCloseDoesNotRemoveReplacementEpoch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "frame.nv12")
	shared, err := newBrokerSubSharedFrame(path, 16)
	if err != nil {
		t.Fatal(err)
	}
	replacement := path + ".replacement"
	if err := os.WriteFile(replacement, []byte("new-epoch"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(replacement, path); err != nil {
		t.Fatal(err)
	}
	if err := shared.Close(); err != nil {
		t.Fatal(err)
	}
	if payload, err := os.ReadFile(path); err != nil || string(payload) != "new-epoch" {
		t.Fatalf("replacement epoch was removed or changed payload=%q err=%v", payload, err)
	}
}

func TestBrokerSharedFrameRejectsLiveProducerAndReclaimsStaleEpoch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "frame.nv12")
	owner, err := newBrokerSubSharedFrame(path, 16)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := newBrokerSubSharedFrame(path, 16); err == nil ||
		!strings.Contains(err.Error(), "live producer") {
		_ = owner.Close()
		t.Fatalf("second live shared-frame producer was accepted: %v", err)
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, make([]byte, brokerSubShmHeaderSize+16), 0o660); err != nil {
		t.Fatal(err)
	}
	replacement, err := newBrokerSubSharedFrame(path, 16)
	if err != nil {
		t.Fatalf("stale shared-frame epoch was not reclaimed: %v", err)
	}
	defer replacement.Close()
	if !replacement.Owns(path) {
		t.Fatal("replacement did not own reclaimed shared-frame path")
	}
}

func TestBrokerSharedFrameReplaceAndRollbackAreIdentityBound(t *testing.T) {
	directory := t.TempDir()
	logicalPath := filepath.Join(directory, "frame.nv12")
	stagingPath := filepath.Join(directory, "candidate.nv12")
	incumbent, err := newBrokerSubSharedFrame(logicalPath, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer incumbent.Close()
	successor, err := newBrokerSubSharedFrame(stagingPath, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer successor.Close()
	if err := successor.Replace(incumbent); err != nil {
		t.Fatal(err)
	}
	if !successor.Owns(logicalPath) || incumbent.Owns(logicalPath) {
		t.Fatal("candidate shared frame did not acquire the logical path")
	}
	if _, err := os.Lstat(stagingPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("candidate shared-frame alias survived cutover: %v", err)
	}
	if err := incumbent.Replace(successor); err != nil {
		t.Fatal(err)
	}
	if !incumbent.Owns(logicalPath) || successor.Owns(logicalPath) {
		t.Fatal("incumbent shared frame did not reacquire the logical path")
	}
}

func TestBrokerNotifierCloseIsConcurrentSafe(t *testing.T) {
	notifier := newBrokerSubFrameNotifier(filepath.Join(t.TempDir(), "missing.sock"))
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for frame := 0; frame < 100; frame++ {
				notifier.Notify(uint32(frame), uint64(frame), 0, time.Now())
			}
		}()
	}
	if err := notifier.Close(); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
}
