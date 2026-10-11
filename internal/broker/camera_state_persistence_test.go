package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	wire "github.com/Josepavese/videosluice/framewire"
	videoproviders "github.com/Josepavese/videosluice/internal/source"
)

func awaitCameraWrite(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("camera state writer did not complete its bounded test operation")
	}
}

func joinCameraWrites(t *testing.T, rt *videoBrokerRuntime) {
	t.Helper()
	done := make(chan struct{})
	go func() { rt.wg.Wait(); close(done) }()
	awaitCameraWrite(t, done)
}

// Synchronous fixture writers deliberately live only in tests. Production
// camera transitions must enqueue snapshots, never call filesystem I/O inline.
func persistCameraModeHealthEntries(stateRoot, fingerprint string, entries map[string]cameraModeHealthEntry, now time.Time) error {
	body, err := encodeCameraModeHealthEntries(stateRoot, fingerprint, entries, now)
	if err != nil {
		return err
	}
	return writeCameraStateDocument(cameraModeHealthStatePath(stateRoot), body)
}

func writeCameraAdmissionState(path string, state cameraAdmissionState) error {
	body, err := json.Marshal(state)
	if err != nil {
		return err
	}
	return writeCameraStateDocument(path, body)
}

func TestCameraAdmissionStorageCannotBlockCausalFrames(t *testing.T) {
	now := time.Now()
	rt := &videoBrokerRuntime{
		plan: videoBrokerPlan{SourceID: "front", AnalysisStreamID: "analysis", AnalysisEnabled: true,
			StateDirectory: t.TempDir(), CameraAdmissionStatePath: filepath.Join(t.TempDir(), "admission.json")},
		cameraAdmission: videoproviders.CameraAdmissionDecision{DevicePath: "/dev/video0", Format: "h265",
			CaptureWidth: 1280, CaptureHeight: 720, CaptureFPS: 20},
		observedCaptureCodec: "h265", observedCaptureWidth: 1280, observedCaptureHeight: 720, observedCaptureFPS: 20,
		cameraAdmissionTracker: cameraAdmissionRuntimeTracker{SubFrames: 2, FirstSubFrameAt: now.Add(-100 * time.Millisecond), LastSubFrameAt: now.Add(-50 * time.Millisecond)},
		mediaEpoch:             7,
	}
	rt.modeHealthFingerprint = cameraModeRuntimeFingerprint(rt.plan)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer func() { close(release); joinCameraWrites(t, rt) }()
	rt.cameraPersistence.write = func(string, []byte) error {
		once.Do(func() { close(entered) })
		<-release // blocked filesystem, not a slow frame consumer
		return nil
	}
	header := wire.Header{Kind: wire.Delivery, Codec: wire.NV12, Source: "front", Stream: "analysis", SourceEpoch: 7, PayloadBytes: 24}
	observer := rt.newCausalDeliveryObserverLocked(context.Background())
	first := make(chan error, 1)
	go func() {
		err := observer.observe(context.Background(), header, make([]byte, 24))
		rt.mu.Lock()
		rt.applyCausalDeliveryLocked(observer)
		rt.mu.Unlock()
		first <- err
	}()
	awaitCameraWrite(t, entered)
	select {
	case err := <-first:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("causal admission waited for filesystem completion")
	}
	if !rt.mu.TryLock() {
		t.Fatal("persistence retained the media mutex")
	}
	admitted := rt.cameraAdmitted
	rt.mu.Unlock()
	if !admitted {
		t.Fatal("camera contract was not admitted in memory")
	}
	second := make(chan error, 1)
	go func() { second <- observer.observe(context.Background(), header, make([]byte, 24)) }()
	select {
	case err := <-second:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("next causal frame waited for blocked state I/O")
	}
	rt.applyCausalDeliveryLocked(observer)
	if rt.totalSubFrames != 2 {
		t.Fatalf("causal frames=%d", rt.totalSubFrames)
	}
}

func TestCameraStateWriterCoalescesBoundedLatestSnapshotsAndJoins(t *testing.T) {
	rt := &videoBrokerRuntime{}
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer func() { unblock(); joinCameraWrites(t, rt) }()
	var writes []string
	rt.cameraPersistence.write = func(_ string, body []byte) error {
		once.Do(func() { close(entered); <-release })
		writes = append(writes, string(body))
		return nil
	}
	if !rt.queueCameraStateLocked(cameraAdmissionDocument, "/state/admission.json", []byte(`{"revision":0}`)) {
		t.Fatal("initial snapshot rejected")
	}
	awaitCameraWrite(t, entered)
	for revision := 1; revision <= 100; revision++ {
		for kind := 0; kind < cameraDocumentKinds; kind++ {
			body := []byte(fmt.Sprintf(`{"revision":%d,"kind":%d}`, revision, kind))
			if !rt.queueCameraStateLocked(kind, "/state/camera.json", body) {
				t.Fatal("latest snapshot rejected")
			}
		}
	}
	p := &rt.cameraPersistence
	p.mu.Lock()
	bounded := p.running && p.pending[0] != nil && p.pending[1] != nil && p.coalesced == 198 && p.committed == 0
	p.mu.Unlock()
	if !bounded {
		unblock()
		t.Fatal("pending storage is not bounded to one snapshot per kind")
	}
	joined := make(chan struct{})
	go func() { rt.wg.Wait(); close(joined) }()
	select {
	case <-joined:
		unblock()
		t.Fatal("runtime lost ownership of an in-flight state write")
	default:
	}
	unblock()
	awaitCameraWrite(t, joined)
	if len(writes) != 3 || writes[1] != `{"revision":100,"kind":0}` || writes[2] != `{"revision":100,"kind":1}` {
		t.Fatalf("stale, duplicate or reordered writes: %v", writes)
	}
	if p.running || p.committed != 201 || p.failed != 0 {
		t.Fatalf("writer did not drain: %+v", p)
	}
}

func TestCameraStateWriterReportsFailureWithoutFalseCommitOrRetryLoop(t *testing.T) {
	rt := &videoBrokerRuntime{}
	calls := 0
	rt.cameraPersistence.write = func(string, []byte) error { calls++; return os.ErrPermission }
	if !rt.queueCameraStateLocked(cameraModeHealthDocument, "/state/health.json", []byte(`{}`)) {
		t.Fatal("snapshot rejected")
	}
	joinCameraWrites(t, rt)
	p := &rt.cameraPersistence
	if calls != 1 || p.failed != 1 || p.committed != 0 || p.running {
		t.Fatal("failed write was acknowledged or retried")
	}
	rt.cameraPersistence.write = func(string, []byte) error { calls++; return nil }
	if !rt.queueCameraStateLocked(cameraModeHealthDocument, "/state/health.json", []byte(`{}`)) {
		t.Fatal("next state rejected")
	}
	joinCameraWrites(t, rt)
	if calls != 2 || p.failed != 1 || p.committed != 2 {
		t.Fatal("next snapshot did not recover persistence")
	}
	rt.stopping = true
	if rt.queueCameraStateLocked(cameraModeHealthDocument, "/state/health.json", []byte(`{}`)) {
		t.Fatal("stopped runtime started a writer")
	}
	if p.running || p.pending[1] != nil {
		t.Fatal("rejected snapshot retained ownership")
	}
}

func TestCameraStateSnapshotOwnsModeHealthAndKeepsDurableFormat(t *testing.T) {
	root := t.TempDir()
	fingerprint := cameraModeRuntimeFingerprint(videoBrokerPlan{})
	entries := map[string]cameraModeHealthEntry{"mode": {Failures: 2}}
	body, err := encodeCameraModeHealthEntries(root, fingerprint, entries, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	entries["mode"] = cameraModeHealthEntry{Failures: 50}
	path := cameraModeHealthStatePath(root)
	if err := writeCameraStateDocument(path, body); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var state cameraModeHealthState
	if err := json.Unmarshal(raw, &state); err != nil || len(state.Entries) != 1 || state.Entries[0].Failures != 2 {
		t.Fatal("snapshot borrowed mutable runtime data", err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0o640 {
		t.Fatal("state permissions changed", err)
	}
	for _, test := range []struct {
		path string
		body []byte
	}{{"relative", []byte(`{}`)}, {path, nil}, {path, make([]byte, maxCameraDocumentBytes+1)}} {
		if err := writeCameraStateDocument(test.path, test.body); err == nil {
			t.Fatal("invalid or unbounded state accepted")
		}
	}
	if err := writeCameraStateDocument(filepath.Join(path, "child"), []byte(`{}`)); err == nil {
		t.Fatal("invalid directory accepted")
	}
}
