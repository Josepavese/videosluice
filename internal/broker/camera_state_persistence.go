package broker

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	cameraAdmissionDocument = iota
	cameraModeHealthDocument
	cameraDocumentKinds
	maxCameraDocumentBytes = 1 << 20
)

type cameraStateDocument struct {
	kind     int
	sequence uint64
	path     string
	body     []byte
}

// One source-local writer, at most one in-flight and two latest snapshots.
// No pixels or runtime lock cross into filesystem I/O. The runtime wait group
// joins the writer before source teardown completes, preventing an old graph
// from publishing after its replacement starts. Kernel I/O is not cancellable:
// ownership is retained until it really returns, never hidden by a timeout.
type cameraStatePersistence struct {
	mu        sync.Mutex
	pending   [cameraDocumentKinds]*cameraStateDocument
	running   bool
	next      uint64
	committed uint64
	failed    uint64
	coalesced uint64
	write     func(string, []byte) error // trusted per-instance filesystem adapter
}

// body transfers ownership of an encoded immutable snapshot, never a runtime
// map or borrowed frame. A queued snapshot is NOT a durability acknowledgement.
func (rt *videoBrokerRuntime) queueCameraStateLocked(kind int, path string, body []byte) bool {
	if kind < 0 || kind >= cameraDocumentKinds || !filepath.IsAbs(path) || len(body) == 0 || len(body) > maxCameraDocumentBytes {
		log.Printf("[video-broker] event=camera_state_persistence status=rejected reason=invalid_snapshot kind=%d bytes=%d", kind, len(body))
		return false
	}
	p := &rt.cameraPersistence
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.next == ^uint64(0) {
		log.Printf("[video-broker] event=camera_state_persistence status=rejected reason=sequence_exhausted kind=%d", kind)
		return false
	}
	p.next++
	if p.pending[kind] != nil {
		p.coalesced++
	}
	p.pending[kind] = &cameraStateDocument{kind: kind, sequence: p.next, path: path, body: body}
	if !p.running {
		p.running = true
		if !rt.startGoroutine(p.drain) {
			p.running = false
			p.pending[kind] = nil
			log.Printf("[video-broker] event=camera_state_persistence status=skipped reason=runtime_stopping kind=%d", kind)
			return false
		}
	}
	return true
}

func (p *cameraStatePersistence) drain() {
	for {
		p.mu.Lock()
		var next *cameraStateDocument
		for _, document := range p.pending {
			if document != nil && (next == nil || document.sequence < next.sequence) {
				next = document
			}
		}
		if next == nil {
			p.running = false
			p.mu.Unlock()
			return
		}
		p.pending[next.kind] = nil
		write := p.write
		if write == nil {
			write = writeCameraStateDocument
		}
		p.mu.Unlock()
		started := time.Now()
		err := write(next.path, next.body)
		p.mu.Lock()
		if err == nil {
			p.committed = next.sequence
		} else {
			p.failed++
		}
		p.mu.Unlock()
		if err != nil {
			log.Printf("[video-broker] event=camera_state_persistence status=failed kind=%d sequence=%d elapsed_ms=%d bytes=%d err=%v", next.kind, next.sequence, time.Since(started).Milliseconds(), len(next.body), err)
		} else {
			log.Printf("[video-broker] event=camera_state_persistence status=committed kind=%d sequence=%d elapsed_ms=%d bytes=%d", next.kind, next.sequence, time.Since(started).Milliseconds(), len(next.body))
		}
	}
}

// Preserve the existing atomic replacement and file + directory fsync. Only
// the execution plane changes: storage latency never owns the media mutex.
func writeCameraStateDocument(path string, body []byte) error {
	if !filepath.IsAbs(path) || len(body) == 0 || len(body) > maxCameraDocumentBytes {
		return fmt.Errorf("invalid camera state document")
	}
	dir := filepath.Dir(path)
	if err := ensureCameraStateDirectory(dir); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".camera-state-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	defer tmp.Close()
	if err := tmp.Chmod(0o640); err != nil {
		return err
	}
	if _, err := tmp.Write(append(body, '\n')); err != nil {
		return err
	}
	if err := tmp.Sync(); err != nil {
		return fmt.Errorf("sync camera state: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return fmt.Errorf("publish camera state: %w", err)
	}
	directory, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return fmt.Errorf("sync camera state directory: %w", err)
	}
	return nil
}
