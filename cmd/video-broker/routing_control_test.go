package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	videobroker "github.com/Josepavese/videosluice"
	"github.com/Josepavese/videosluice/config"
	"github.com/Josepavese/videosluice/routing"
)

func TestRoutingFailureLogsBoundedQuotedCauseAndKeepsPublicReplySafe(t *testing.T) {
	var output bytes.Buffer
	previous := log.Writer()
	log.SetOutput(&output)
	t.Cleanup(func() { log.SetOutput(previous) })
	request := routing.ControlRequest{RequestID: "diagnostic\nrequest", Action: routing.ControlActionPreview, ExpectedRevision: 93}
	cause := errors.New("candidate routing graph: analysis unbound\nforged record")
	logRoutingControlFailure(request, cause)
	line := output.String()
	if strings.Count(line, "\n") != 1 || !strings.Contains(line, `analysis unbound\nforged record`) || !strings.Contains(line, `expected_revision=93`) || !strings.Contains(line, `truncated=false`) {
		t.Fatalf("cause not retained as one quoted log record: %q", line)
	}
	public := safeRoutingControlError(cause)
	if public.Code != "rejected" || public.Message != "routing operation was rejected" || public.Phase != "" {
		t.Fatalf("public reply leaked the internal validation cause: %+v", public)
	}
	output.Reset()
	logRoutingControlFailure(request, errors.New(strings.Repeat("x", 2048)+"beyond-limit"))
	if line = output.String(); !strings.Contains(line, `truncated=true`) || strings.Contains(line, "beyond-limit") || len(line) > 1400 {
		t.Fatalf("diagnostic cause is not bounded: length=%d", len(line))
	}
}

type routingControlStub struct {
	mu           sync.Mutex
	plan         routing.Plan
	applyErr     error
	applyEntered chan<- struct{}
	applyRelease <-chan struct{}
}

func (controller *routingControlStub) Snapshot(time.Time) videobroker.Status {
	return videobroker.Status{}
}

func TestRoutingRuntimeProjectionBindsPerformanceToConfirmedSource(t *testing.T) {
	for _, count := range []int{1, 2, 3, 5} {
		catalog := routing.ControlCatalog{Streams: make(map[string]routing.ControlStreamCatalog)}
		plan := routing.Plan{Selections: make(map[string]string), StreamGenerations: make(map[string]uint64)}
		status := videobroker.Status{Sources: make(map[string]videobroker.SourceStatus), Streams: make(map[string]videobroker.StreamStatus)}
		for index := 0; index < count; index++ {
			id := string(rune('a' + index))
			catalog.Streams[id] = routing.ControlStreamCatalog{Options: map[string]routing.ControlStreamOption{"selected": {SourceRef: id, FPS: 25}}}
			plan.Selections[id], plan.StreamGenerations[id] = "selected", uint64(index+1)
			status.Sources[id] = videobroker.SourceStatus{State: "running"}
			status.Streams[id] = videobroker.StreamStatus{SourceRef: id, Performance: videobroker.StreamPerformance{
				State: "reduced", RequestedFPS: 25, TargetFPS: 25, ObservedFPS: 10.8, WindowMS: 2000,
			}}
		}
		projected := projectRoutingRuntime(catalog, plan, status)
		if len(projected) != count {
			t.Fatalf("source count=%d projected=%d", count, len(projected))
		}
		for id, value := range projected {
			if !value.Available || value.State != "reduced" || value.SourceRef != id || value.Generation != plan.StreamGenerations[id] || value.ObservedFPS != 10.8 || value.RequestedFPS != 25 {
				t.Fatalf("wrong runtime projection: %+v", value)
			}
		}
		wrongSource := status.Streams["a"]
		wrongSource.SourceRef = "unconfirmed-source"
		status.Streams["a"] = wrongSource
		if got := projectRoutingRuntime(catalog, plan, status)["a"]; got.Available || got.State != "unavailable" {
			t.Fatalf("runtime from another source was accepted: %+v", got)
		}
	}
}

func (controller *routingControlStub) RoutingPlan() (routing.Plan, error) {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	return cloneControlPlan(controller.plan), nil
}

func (controller *routingControlStub) PreviewRouting(
	_ context.Context,
	catalog routing.Catalog,
	store *routing.Store,
	expectedRevision uint64,
	selections map[string]string,
	sourceSelections ...map[string]string,
) (routing.Preview, error) {
	return routing.PreviewGraph(catalog, store, expectedRevision, selections, sourceSelections...)
}

func (controller *routingControlStub) ApplyRoutingObserved(
	_ context.Context,
	catalog routing.Catalog,
	store *routing.Store,
	expectedRevision uint64,
	selections map[string]string,
	observer routing.PhaseObserver,
	sourceSelections ...map[string]string,
) (routing.Plan, error) {
	observer(routing.ApplyPhaseValidate)
	if controller.applyEntered != nil {
		select {
		case controller.applyEntered <- struct{}{}:
		default:
		}
	}
	if controller.applyRelease != nil {
		select {
		case <-controller.applyRelease:
		case <-time.After(2 * time.Second):
			return routing.Plan{}, errors.New("test apply release timed out")
		}
	}
	controller.mu.Lock()
	defer controller.mu.Unlock()
	if controller.applyErr != nil {
		return routing.Plan{}, controller.applyErr
	}
	observer(routing.ApplyPhasePrepare)
	transaction, err := store.Begin(catalog, expectedRevision, selections, sourceSelections...)
	if err != nil {
		return routing.Plan{}, err
	}
	plan := transaction.Plan()
	observer(routing.ApplyPhasePersist)
	if err := transaction.Commit(); err != nil {
		return routing.Plan{}, err
	}
	controller.plan = cloneControlPlan(plan)
	observer(routing.ApplyPhaseRetire)
	return plan, nil
}

func TestRoutingControlSerializesMutationsAndRetainsTerminalProgress(t *testing.T) {
	catalog := routingControlCatalog(t)
	plan, err := routing.DefaultPlan(catalog)
	if err != nil {
		t.Fatal(err)
	}
	store, socketPath := routingControlStore(t)
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	controller := &routingControlStub{
		plan: plan, applyEntered: entered, applyRelease: release,
	}
	cancel, done := startRoutingControlServer(t, socketPath, catalog, store, controller)
	defer stopRoutingControlServer(t, cancel, done, socketPath)

	accepted := exchangeRoutingControl(t, socketPath, routing.ControlRequest{
		Schema: routing.ControlSchema, RequestID: "apply-slow-1",
		Action: routing.ControlActionApply, ExpectedRevision: 1,
		Selections: map[string]string{"live": "economy"},
	})
	if !accepted.OK || accepted.Operation == nil {
		t.Fatalf("slow operation not accepted: %+v", accepted)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("slow operation did not start")
	}
	readStarted := time.Now()
	readWhileBusy := exchangeRoutingControl(t, socketPath, routing.ControlRequest{
		Schema: routing.ControlSchema, RequestID: "read-busy-1",
		Action: routing.ControlActionRead,
	})
	if readWhileBusy.OK || readWhileBusy.Error == nil || readWhileBusy.Error.Code != "busy" {
		t.Fatalf("read during mutation did not fail fast as busy: %+v", readWhileBusy)
	}
	if elapsed := time.Since(readStarted); elapsed > 500*time.Millisecond {
		t.Fatalf("read during mutation blocked instead of failing fast: %s", elapsed)
	}
	previewWhileBusy := exchangeRoutingControl(t, socketPath, routing.ControlRequest{
		Schema: routing.ControlSchema, RequestID: "preview-busy-1", Action: routing.ControlActionPreview,
		ExpectedRevision: 1, Selections: map[string]string{"live": "economy"},
	})
	if previewWhileBusy.OK || previewWhileBusy.Error == nil || previewWhileBusy.Error.Code != "busy" {
		t.Fatalf("preview during mutation did not fail fast: %+v", previewWhileBusy)
	}
	busy := exchangeRoutingControl(t, socketPath, routing.ControlRequest{
		Schema: routing.ControlSchema, RequestID: "rollback-busy-1",
		Action: routing.ControlActionRollback, ExpectedRevision: 1,
	})
	if busy.OK || busy.Error == nil || busy.Error.Code != "busy" {
		t.Fatalf("concurrent mutation was not rejected: %+v", busy)
	}
	progress := exchangeRoutingControl(t, socketPath, routing.ControlRequest{
		Schema: routing.ControlSchema, RequestID: "progress-slow-1",
		Action: routing.ControlActionProgress, OperationID: "apply-slow-1",
	})
	if !progress.OK || progress.Operation == nil ||
		progress.Operation.State != routing.ControlOperationRunning ||
		progress.Operation.Phase != routing.ApplyPhaseValidate {
		t.Fatalf("running progress is wrong: %+v", progress)
	}
	close(release)
	completed := awaitRoutingOperation(t, socketPath, "apply-slow-1")
	if completed.State != routing.ControlOperationSucceeded || completed.Phase != "complete" {
		t.Fatalf("terminal progress is wrong: %+v", completed)
	}
	duplicate := exchangeRoutingControl(t, socketPath, routing.ControlRequest{
		Schema: routing.ControlSchema, RequestID: "apply-slow-1",
		Action: routing.ControlActionApply, ExpectedRevision: 2,
		Selections: map[string]string{"live": "balanced"},
	})
	if duplicate.OK || duplicate.Error == nil || duplicate.Error.Code != "duplicate_request" {
		t.Fatalf("duplicate operation id was not rejected: %+v", duplicate)
	}
	missing := exchangeRoutingControl(t, socketPath, routing.ControlRequest{
		Schema: routing.ControlSchema, RequestID: "progress-missing-1",
		Action: routing.ControlActionProgress, OperationID: "missing-1",
	})
	if missing.OK || missing.Error == nil || missing.Error.Code != "operation_not_found" {
		t.Fatalf("missing operation was not rejected: %+v", missing)
	}
}

func (controller *routingControlStub) RollbackRoutingObserved(
	_ context.Context,
	catalog routing.Catalog,
	store *routing.Store,
	expectedRevision uint64,
	observer routing.PhaseObserver,
) (routing.Plan, error) {
	observer(routing.ApplyPhaseValidate)
	controller.mu.Lock()
	defer controller.mu.Unlock()
	observer(routing.ApplyPhasePrepare)
	transaction, err := store.BeginRollback(catalog, expectedRevision)
	if err != nil {
		return routing.Plan{}, err
	}
	plan := transaction.Plan()
	observer(routing.ApplyPhasePersist)
	if err := transaction.Commit(); err != nil {
		return routing.Plan{}, err
	}
	controller.plan = cloneControlPlan(plan)
	observer(routing.ApplyPhaseRetire)
	return plan, nil
}

func TestRoutingControlServerReadPreviewApplyAndRollback(t *testing.T) {
	catalog := routingControlCatalog(t)
	plan, err := routing.DefaultPlan(catalog)
	if err != nil {
		t.Fatal(err)
	}
	store, socketPath := routingControlStore(t)
	controller := &routingControlStub{plan: plan}
	cancel, done := startRoutingControlServer(t, socketPath, catalog, store, controller)
	defer stopRoutingControlServer(t, cancel, done, socketPath)

	read := exchangeRoutingControl(t, socketPath, routing.ControlRequest{
		Schema: routing.ControlSchema, RequestID: "read-1", Action: routing.ControlActionRead,
	})
	if !read.OK || read.Plan == nil || read.Plan.Revision != 1 ||
		read.Catalog == nil || read.Catalog.Revision != catalog.Revision ||
		len(read.Catalog.Sources) != 1 || read.Catalog.Sources[0] != "front" ||
		read.Catalog.Streams["live"].Options["economy"].FPS != 15 {
		t.Fatalf("unexpected read response: %+v", read)
	}
	readBody, err := json.Marshal(read.Catalog)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(readBody), "/dev/") || strings.Contains(string(readBody), "/run/") {
		t.Fatalf("control catalog exposed physical/runtime paths: %s", readBody)
	}
	preview := exchangeRoutingControl(t, socketPath, routing.ControlRequest{
		Schema: routing.ControlSchema, RequestID: "preview-1", Action: routing.ControlActionPreview,
		ExpectedRevision: 1, Selections: map[string]string{"live": "economy"},
	})
	if !preview.OK || preview.Preview == nil || preview.Preview.Candidate.Revision != 2 {
		t.Fatalf("unexpected preview response: %+v", preview)
	}
	unchanged, err := store.Read(catalog)
	if err != nil || unchanged.Revision != 1 {
		t.Fatalf("preview mutated store: plan=%+v err=%v", unchanged, err)
	}
	accepted := exchangeRoutingControl(t, socketPath, routing.ControlRequest{
		Schema: routing.ControlSchema, RequestID: "apply-1", Action: routing.ControlActionApply,
		ExpectedRevision: 1, Selections: map[string]string{"live": "economy"},
	})
	if !accepted.OK || accepted.Operation == nil || accepted.Operation.OperationID != "apply-1" {
		t.Fatalf("unexpected apply acceptance: %+v", accepted)
	}
	applied := awaitRoutingOperation(t, socketPath, "apply-1")
	if applied.State != routing.ControlOperationSucceeded || applied.Plan == nil ||
		applied.Plan.Revision != 2 || applied.Plan.Selections["live"] != "economy" {
		t.Fatalf("unexpected apply operation: %+v", applied)
	}
	accepted = exchangeRoutingControl(t, socketPath, routing.ControlRequest{
		Schema: routing.ControlSchema, RequestID: "rollback-1", Action: routing.ControlActionRollback,
		ExpectedRevision: 2,
	})
	if !accepted.OK || accepted.Operation == nil || accepted.Operation.OperationID != "rollback-1" {
		t.Fatalf("unexpected rollback acceptance: %+v", accepted)
	}
	rolledBack := awaitRoutingOperation(t, socketPath, "rollback-1")
	if rolledBack.State != routing.ControlOperationSucceeded || rolledBack.Plan == nil ||
		rolledBack.Plan.Revision != 3 || rolledBack.Plan.Selections["live"] != "balanced" {
		t.Fatalf("unexpected rollback response: %+v", rolledBack)
	}
}

func TestRoutingControlServerRejectsMalformedAndSanitizesRuntimeErrors(t *testing.T) {
	catalog := routingControlCatalog(t)
	plan, err := routing.DefaultPlan(catalog)
	if err != nil {
		t.Fatal(err)
	}
	store, socketPath := routingControlStore(t)
	controller := &routingControlStub{plan: plan}
	cancel, done := startRoutingControlServer(t, socketPath, catalog, store, controller)
	defer stopRoutingControlServer(t, cancel, done, socketPath)

	connection, err := net.DialTimeout("unix", socketPath, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := connection.Write([]byte(`{"schema":"video-broker.routing-control.v2","request_id":"bad-1","action":"read","unknown":true}` + "\n")); err != nil {
		t.Fatal(err)
	}
	var malformed routing.ControlResponse
	if err := json.NewDecoder(connection).Decode(&malformed); err != nil {
		t.Fatal(err)
	}
	_ = connection.Close()
	if malformed.OK || malformed.Error == nil || malformed.Error.Code != "invalid_request" {
		t.Fatalf("malformed request response: %+v", malformed)
	}

	controller.mu.Lock()
	controller.applyErr = errors.New("device /dev/secret-camera failed with vendor details")
	controller.mu.Unlock()
	accepted := exchangeRoutingControl(t, socketPath, routing.ControlRequest{
		Schema: routing.ControlSchema, RequestID: "apply-safe-1", Action: routing.ControlActionApply,
		ExpectedRevision: 1, Selections: map[string]string{"live": "economy"},
	})
	if !accepted.OK || accepted.Operation == nil {
		t.Fatalf("failed operation was not accepted for asynchronous execution: %+v", accepted)
	}
	rejected := awaitRoutingOperation(t, socketPath, "apply-safe-1")
	body, err := json.Marshal(rejected)
	if err != nil {
		t.Fatal(err)
	}
	if rejected.State != routing.ControlOperationFailed || rejected.Error == nil || rejected.Error.Code != "rejected" ||
		strings.Contains(string(body), "/dev/") || strings.Contains(string(body), "vendor") {
		t.Fatalf("runtime error was not sanitized: %s", body)
	}
}

func TestDecodeRoutingControlRequestIsBoundedAndLineFramed(t *testing.T) {
	valid := `{"schema":"video-broker.routing-control.v2","request_id":"read-1","action":"read"}`
	if _, err := decodeRoutingControlRequest(strings.NewReader(valid)); err == nil {
		t.Fatal("request without line terminator was accepted")
	}
	oversized := strings.Repeat("x", routingControlMaxRequestBytes) + "\n"
	if _, err := decodeRoutingControlRequest(strings.NewReader(oversized)); err == nil {
		t.Fatal("oversized request was accepted")
	}
	request, err := decodeRoutingControlRequest(strings.NewReader(valid + "\n"))
	if err != nil || request.RequestID != "read-1" {
		t.Fatalf("valid line request=%+v err=%v", request, err)
	}
}

func TestValidateRoutingControlPathRejectsGraphAndAuthorityCollisions(t *testing.T) {
	catalog := routingControlCatalog(t)
	plan, err := routing.DefaultPlan(catalog)
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := routing.Materialize(catalog, plan)
	if err != nil {
		t.Fatal(err)
	}
	catalogPath := "/etc/video-broker/routing-catalog.json"
	planPath := "/var/lib/video/routing-plan.json"
	cfg.Runtime.FrameAccess = &config.FrameAccessConfig{CopyProvider: "cpu_nv12_copy", Readers: []config.FrameReaderConfig{{ID: "reader", Stream: "analysis", DeliverySocket: "/run/video/frames.sock", ROISocket: "/run/video/roi.sock"}}}
	for _, path := range []string{
		"relative.sock",
		cfg.Runtime.Status.Socket,
		cfg.Streams["live"].Socket,
		cfg.Sources["front"].Device,
		catalogPath,
		planPath,
		"/run/video/frames.sock",
		"/run/video/roi.sock",
	} {
		if err := validateRoutingControlPath(path, cfg, catalogPath, planPath); err == nil {
			t.Fatalf("unsafe routing control path accepted: %q", path)
		}
	}
	if err := validateRoutingControlPath(
		"/run/video/routing-control.sock", cfg, catalogPath, planPath,
	); err != nil {
		t.Fatalf("independent routing control path rejected: %v", err)
	}
}

func routingControlCatalog(t *testing.T) routing.Catalog {
	t.Helper()
	catalog, err := routing.NormalizeCatalog(routing.Catalog{
		Schema: routing.CatalogSchema, Revision: "sha256:" + strings.Repeat("a", 64),
		Sources: map[string]config.SourceConfig{
			"front": {
				Identity: "front", Transport: "v4l2_uvc", Device: "/dev/camera-front",
				Format: "h265", Width: 1280, Height: 720, FPS: 20,
			},
		},
		Streams: map[string]routing.StreamCatalog{
			"live": {
				Type: "live", Socket: "/run/video/live.sock",
				Options: map[string]routing.StreamOption{
					"balanced": {SourceRef: "front", Codec: "h265", Width: 1280, Height: 720, FPS: 20, BitrateKbps: 4096},
					"economy":  {SourceRef: "front", Codec: "h265", Width: 1280, Height: 720, FPS: 15, BitrateKbps: 3072},
				},
			},
		},
		DefaultSelections: map[string]string{"live": "balanced"},
		Runtime: config.RuntimeConfig{
			RuntimeDirectory: "/run/video", StateDirectory: "/var/lib/video",
			Status: config.StatusConfig{Socket: "/run/video/status.sock"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return catalog
}

func TestRoutingControlSourceModeRoundTripAndExplicitReset(t *testing.T) {
	catalog := routingControlCatalog(t)
	catalog.SourceModes = map[string]map[string]routing.SourceMode{"front": {"qualified": {Format: "h265", Width: 1920, Height: 1080, FPS: 20}}}
	plan, err := routing.DefaultPlan(catalog)
	if err != nil {
		t.Fatal(err)
	}
	store, socket := routingControlStore(t)
	controller := &routingControlStub{plan: plan}
	cancel, done := startRoutingControlServer(t, socket, catalog, store, controller)
	defer stopRoutingControlServer(t, cancel, done, socket)
	for i, modes := range []map[string]string{{"front": "qualified"}, {}} {
		id := fmt.Sprintf("mode-%d", i)
		request := routing.ControlRequest{Schema: routing.ControlSchema, RequestID: id + "-preview", Action: routing.ControlActionPreview, ExpectedRevision: uint64(i + 1), Selections: plan.Selections, SourceSelections: modes}
		preview := exchangeRoutingControl(t, socket, request)
		if !preview.OK || preview.Preview == nil || preview.Preview.Candidate.SourceSelections["front"] != modes["front"] {
			t.Fatalf("mode preview: %+v", preview)
		}
		request.Action, request.RequestID = routing.ControlActionApply, id
		accepted := exchangeRoutingControl(t, socket, request)
		if !accepted.OK {
			t.Fatalf("mode apply: %+v", accepted)
		}
		terminal := awaitRoutingOperation(t, socket, id)
		if terminal.State != "succeeded" {
			t.Fatalf("mode progress: %+v", terminal)
		}
		persisted, err := store.Read(catalog)
		if err != nil || persisted.Revision != uint64(i+2) || persisted.SourceSelections["front"] != modes["front"] {
			t.Fatalf("mode persistence: %+v %v", persisted, err)
		}
	}
}

func routingControlStore(t *testing.T) (*routing.Store, string) {
	t.Helper()
	directory := filepath.Join(t.TempDir(), "routing")
	store, err := routing.NewStoreForPlan(filepath.Join(directory, "routing-plan.json"))
	if err != nil {
		t.Fatal(err)
	}
	return store, filepath.Join(t.TempDir(), "routing-control.sock")
}

func startRoutingControlServer(
	t *testing.T,
	path string,
	catalog routing.Catalog,
	store *routing.Store,
	controller routingController,
) (context.CancelFunc, <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- serveRoutingControl(ctx, path, catalog, store, controller) }()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if info, err := os.Lstat(path); err == nil && info.Mode()&os.ModeSocket != 0 {
			metadata, ok := info.Sys().(*syscall.Stat_t)
			if info.Mode().Perm() != 0o600 || !ok || metadata.Nlink != 1 {
				cancel()
				t.Fatalf("routing control socket metadata mode=%v metadata=%+v", info.Mode(), metadata)
			}
			return cancel, done
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	t.Fatal("routing control socket did not become available")
	return nil, nil
}

func stopRoutingControlServer(
	t *testing.T,
	cancel context.CancelFunc,
	done <-chan error,
	path string,
) {
	t.Helper()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("routing control server did not stop")
	}
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("routing control endpoint survived shutdown: %v", err)
	}
}

func exchangeRoutingControl(
	t *testing.T,
	path string,
	request routing.ControlRequest,
) routing.ControlResponse {
	t.Helper()
	connection, err := net.DialTimeout("unix", path, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	_ = connection.SetDeadline(time.Now().Add(2 * time.Second))
	if err := json.NewEncoder(connection).Encode(request); err != nil {
		t.Fatal(err)
	}
	var response routing.ControlResponse
	if err := json.NewDecoder(connection).Decode(&response); err != nil {
		t.Fatal(err)
	}
	if response.Schema != routing.ControlSchema || response.RequestID != request.RequestID {
		t.Fatalf("routing control response identity mismatch: %+v", response)
	}
	return response
}

func awaitRoutingOperation(
	t *testing.T,
	path string,
	operationID string,
) routing.ControlOperation {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		response := exchangeRoutingControl(t, path, routing.ControlRequest{
			Schema: routing.ControlSchema, RequestID: "progress-1",
			Action: routing.ControlActionProgress, OperationID: operationID,
		})
		if !response.OK || response.Operation == nil {
			t.Fatalf("progress failed: %+v", response)
		}
		if response.Operation.State != routing.ControlOperationRunning {
			return *response.Operation
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("operation %s did not finish", operationID)
	return routing.ControlOperation{}
}

func cloneControlPlan(plan routing.Plan) routing.Plan {
	body, _ := json.Marshal(plan)
	var cloned routing.Plan
	_ = json.Unmarshal(body, &cloned)
	return cloned
}
