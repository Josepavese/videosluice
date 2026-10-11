package broker

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Josepavese/videosluice/capability"
	productconfig "github.com/Josepavese/videosluice/config"
)

func serviceWithGraphs(graphs map[string]*graphService) *Service {
	sources := make(map[string]*sourceSupervisor, len(graphs))
	for sourceID, graph := range graphs {
		sources[sourceID] = &sourceSupervisor{
			config: productconfig.SourceGraph{SourceID: sourceID},
			graph:  graph, state: "running", lastChangeAt: time.Now().UTC(),
		}
	}
	return &Service{sources: sources}
}

func TestServiceSnapshotSeparatesSourceAndStreamIdentity(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	runtime := &videoBrokerRuntime{
		plan: videoBrokerPlan{
			SourceIdentity: "front-camera",
			LiveEnabled:    true, AnalysisEnabled: true, ArchiveEnabled: true,
			Transport: "v4l2_uvc", CaptureDevice: "/dev/video0", CaptureFormat: "mjpeg",
			MainFPS: 20, SubFPS: 15, FrameWatchdogSec: 12,
		},
		createdAt: now.Add(-time.Minute), childRunning: true, childStartedAt: now.Add(-time.Minute),
		lastFrameAt: now.Add(-time.Second), lastSubFrameAt: now.Add(-time.Second), lastArchiveFrameAt: now.Add(-time.Second),
		mainClients: map[int]*brokerClient{}, archiveClients: map[int]*brokerClient{}, subClients: map[int]*brokerClient{},
		componentRecovery: map[string]hardwareComponentRecovery{},
		mediaEpoch:        7,
		subShared:         &brokerSubSharedFrame{epoch: 7007},
	}
	service := serviceWithGraphs(map[string]*graphService{
		"front": {
			sourceID: "front", liveStreamID: "preview", analysisStreamID: "vision", archiveStreamID: "recording",
			runtime: runtime,
		},
	})
	status := service.Snapshot(now)
	if status.Schema != "video-broker.status.v5" || status.State != "running" || len(status.Sources) != 1 || len(status.Streams) != 3 {
		t.Fatalf("status projection=%+v", status)
	}
	for streamID, stream := range status.Streams {
		if stream.SourceRef != "front" {
			t.Fatalf("stream %q lost source binding: %+v", streamID, stream)
		}
	}
	if status.Sources["front"].Identity != "front-camera" {
		t.Fatalf("source identity was not published: %+v", status.Sources["front"])
	}
	if status.Streams["preview"].Type != "live" || status.Streams["vision"].Type != "analysis" || status.Streams["recording"].Type != "archive" {
		t.Fatalf("stream roles=%+v", status.Streams)
	}
	if status.Streams["preview"].Epoch != 7 || status.Streams["recording"].Epoch != 7 ||
		status.Streams["vision"].Epoch != 7007 || status.Sources["front"].MediaEpoch != 7 {
		t.Fatalf("capture and NMSH epoch domains were conflated: source=%+v streams=%+v", status.Sources["front"], status.Streams)
	}
}

func TestServiceSnapshotSelectsAggregateFailureDeterministically(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	processStopped := &videoBrokerRuntime{
		plan: videoBrokerPlan{FrameWatchdogSec: 1}, createdAt: now.Add(-time.Minute),
		mainClients: map[int]*brokerClient{}, archiveClients: map[int]*brokerClient{}, subClients: map[int]*brokerClient{},
	}
	staleAnalysis := &videoBrokerRuntime{
		plan:      videoBrokerPlan{AnalysisEnabled: true, FrameWatchdogSec: 1},
		createdAt: now.Add(-time.Minute), childRunning: true, childStartedAt: now.Add(-time.Minute),
		lastSubFrameAt: now.Add(-time.Minute), mainClients: map[int]*brokerClient{}, archiveClients: map[int]*brokerClient{}, subClients: map[int]*brokerClient{},
	}
	service := serviceWithGraphs(map[string]*graphService{
		"z-source": {sourceID: "z-source", runtime: staleAnalysis},
		"a-source": {sourceID: "a-source", runtime: processStopped},
	})
	for iteration := 0; iteration < 100; iteration++ {
		status := service.Snapshot(now)
		if status.State != "degraded" || !strings.Contains(status.LastError, "process is not running") {
			t.Fatalf("iteration=%d nondeterministic aggregate status=%+v", iteration, status)
		}
	}
}

func TestSourceSupervisorRuntimeFailureDoesNotCancelPeerAndRestartsLocally(t *testing.T) {
	cfg := twoSourceManagerConfig(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	type attemptRecord struct {
		ctx  context.Context
		done chan error
	}
	var mu sync.Mutex
	attempts := map[string][]attemptRecord{}
	var allocatorSeen *capabilityAllocator
	starter := func(
		graphCtx context.Context,
		graphConfig productconfig.SourceGraph,
		_ []productconfig.SourceRoleProfile,
		_ capability.Inventory,
		allocator *capabilityAllocator,
	) (*graphService, error) {
		allocatorSeen = allocator
		done := make(chan error, 1)
		mu.Lock()
		attempt := len(attempts[graphConfig.SourceID]) + 1
		attempts[graphConfig.SourceID] = append(
			attempts[graphConfig.SourceID], attemptRecord{ctx: graphCtx, done: done},
		)
		mu.Unlock()
		allocator.mu.Lock()
		allocator.claims["test."+graphConfig.SourceID] = map[string]int{graphConfig.SourceID: 1}
		allocator.mu.Unlock()
		go func() {
			<-graphCtx.Done()
			select {
			case done <- graphCtx.Err():
			default:
			}
		}()
		return managerTestGraph(graphConfig, allocator, done, uint64(attempt)), nil
	}
	service, err := startServiceWithRestartPolicy(
		ctx, cfg, capability.Inventory{Schema: capability.Schema}, starter,
		sourceRestartPolicy{InitialDelay: 100 * time.Millisecond, MaxDelay: 100 * time.Millisecond, StableWindow: time.Second},
	)
	if err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	frontFirst := attempts["front"][0]
	sideFirst := attempts["side"][0]
	mu.Unlock()
	sideEpoch := service.Snapshot(time.Now()).Sources["side"].MediaEpoch
	frontFirst.done <- errors.New("synthetic front capture failure")

	deadline := time.Now().Add(time.Second)
	for {
		status := service.Snapshot(time.Now())
		if status.Sources["front"].State == "degraded" {
			if status.Sources["side"].State != "running" || status.Sources["side"].MediaEpoch != sideEpoch {
				t.Fatalf("peer source changed during front failure: %+v", status)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("front source never exposed its local failure: %+v", status)
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case <-sideFirst.ctx.Done():
		t.Fatal("front failure canceled the independent side source")
	default:
	}
	for {
		mu.Lock()
		frontAttempts := len(attempts["front"])
		sideAttempts := len(attempts["side"])
		mu.Unlock()
		if frontAttempts >= 2 {
			if sideAttempts != 1 {
				t.Fatalf("peer source restarted unexpectedly: attempts=%d", sideAttempts)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("front source did not perform a bounded local restart")
		}
		time.Sleep(time.Millisecond)
	}
	if got := service.Snapshot(time.Now()).Sources["side"].MediaEpoch; got != sideEpoch {
		t.Fatalf("peer epoch changed across front restart: before=%d after=%d", sideEpoch, got)
	}
	cancel()
	if err := service.Wait(context.Background()); err != nil {
		t.Fatalf("parent cancellation was not a clean service shutdown: %v", err)
	}
	allocatorSeen.mu.Lock()
	defer allocatorSeen.mu.Unlock()
	if len(allocatorSeen.claims) != 0 {
		t.Fatalf("hardware leases survived parent shutdown: %+v", allocatorSeen.claims)
	}
}

func TestStartServiceInstallsProductionFanoutBackend(t *testing.T) {
	cfg := twoSourceManagerConfig(t)
	ctx, cancel := context.WithCancel(context.Background())
	starter := func(
		graphCtx context.Context,
		graphConfig productconfig.SourceGraph,
		_ []productconfig.SourceRoleProfile,
		_ capability.Inventory,
		allocator *capabilityAllocator,
	) (*graphService, error) {
		done := make(chan error, 1)
		go func() {
			<-graphCtx.Done()
			done <- graphCtx.Err()
		}()
		return managerTestGraph(graphConfig, allocator, done, 1), nil
	}
	service, err := startServiceWithRestartPolicy(
		ctx, cfg, capability.Inventory{Schema: capability.Schema}, starter,
		defaultSourceRestartPolicy(),
	)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	backend, ok := serviceFanoutBackend(service).(*workerFanoutBackend)
	if !ok || backend == nil {
		cancel()
		t.Fatal("service did not install the production worker fan-out backend")
	}
	if backend.allocator != service.allocator || backend.publications == nil {
		cancel()
		t.Fatal("production fan-out backend does not share service hardware/publication authorities")
	}
	cancel()
	if err := service.Wait(context.Background()); err != nil {
		t.Fatalf("service shutdown after backend wiring test: %v", err)
	}
}

func TestStartServiceWithAdmissionPropagatesDetachedCatalogProfiles(t *testing.T) {
	cfg := twoSourceManagerConfig(t)
	profiles := map[string][]productconfig.SourceRoleProfile{
		"front": {
			{Role: "analysis", PixelFormat: "nv12", Width: 640, Height: 360, FPS: 20},
			{Role: "live", Codec: "h265", Width: 1280, Height: 720, FPS: 20, BitrateKbps: 4096},
		},
		"side": {
			{Role: "archive", Codec: "h265", Width: 640, Height: 360, FPS: 15, BitrateKbps: 750},
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	seen := make(chan struct{}, len(cfg.Sources))
	starter := func(
		graphCtx context.Context,
		graphConfig productconfig.SourceGraph,
		admission []productconfig.SourceRoleProfile,
		_ capability.Inventory,
		allocator *capabilityAllocator,
	) (*graphService, error) {
		if !reflect.DeepEqual(admission, profiles[graphConfig.SourceID]) {
			return nil, fmt.Errorf("source %s admission=%+v", graphConfig.SourceID, admission)
		}
		done := make(chan error, 1)
		go func() {
			<-graphCtx.Done()
			done <- graphCtx.Err()
		}()
		seen <- struct{}{}
		return managerTestGraph(graphConfig, allocator, done, 1), nil
	}
	service, err := startServiceWithAdmission(
		ctx, cfg, capability.Inventory{Schema: capability.Schema}, starter, profiles,
	)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	for range cfg.Sources {
		select {
		case <-seen:
		case <-time.After(time.Second):
			t.Fatal("catalog admission did not reach source starter")
		}
	}
	profiles["front"][0].Width = 2
	if service.admission["front"][0].Width != 640 {
		t.Fatal("running service aliases caller-owned admission profiles")
	}
	cancel()
	if err := service.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestServiceMutableLifecycleStartsAndStopsOnlyRequestedSource(t *testing.T) {
	cfg := twoSourceManagerConfig(t)
	managerCtx, cancelManager := context.WithCancel(context.Background())
	defer cancelManager()
	var mu sync.Mutex
	attempts := map[string]int{}
	contexts := map[string]context.Context{}
	starter := func(
		graphCtx context.Context,
		graphConfig productconfig.SourceGraph,
		_ []productconfig.SourceRoleProfile,
		_ capability.Inventory,
		allocator *capabilityAllocator,
	) (*graphService, error) {
		done := make(chan error, 1)
		mu.Lock()
		attempts[graphConfig.SourceID]++
		attempt := attempts[graphConfig.SourceID]
		contexts[graphConfig.SourceID] = graphCtx
		mu.Unlock()
		go func() {
			<-graphCtx.Done()
			done <- graphCtx.Err()
		}()
		return managerTestGraph(graphConfig, allocator, done, uint64(attempt)), nil
	}
	service := &Service{
		sources: make(map[string]*sourceSupervisor), config: cfg,
		inventory: capability.Inventory{Schema: capability.Schema}, allocator: newCapabilityAllocator(),
		starter: starter, retryPolicy: sourceRestartPolicy{
			InitialDelay: time.Millisecond, MaxDelay: time.Millisecond, StableWindow: time.Second,
		},
		managerCtx: managerCtx, cancel: cancelManager,
	}
	graphs := cfg.SourceGraphs()
	for _, graph := range graphs {
		if _, err := service.startSource(graph, true); err != nil {
			t.Fatal(err)
		}
	}
	if status := service.Snapshot(time.Now()); len(status.Sources) != 2 {
		t.Fatalf("dynamic sources did not start: %+v", status)
	}
	mu.Lock()
	frontCtx := contexts["front"]
	mu.Unlock()
	stopCtx, cancelStop := context.WithTimeout(context.Background(), time.Second)
	if err := service.stopSource(stopCtx, "side"); err != nil {
		cancelStop()
		t.Fatal(err)
	}
	cancelStop()
	if status := service.Snapshot(time.Now()); len(status.Sources) != 1 || status.Sources["front"].State != "running" {
		t.Fatalf("source-local stop changed its peer: %+v", status)
	}
	select {
	case <-frontCtx.Done():
		t.Fatal("stopping side canceled front capture owner")
	default:
	}
	if _, err := service.startSource(graphs[1], true); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	sideAttempts := attempts["side"]
	frontAttempts := attempts["front"]
	mu.Unlock()
	if sideAttempts != 2 || frontAttempts != 1 {
		t.Fatalf("source-local reactivation restarted peer: front=%d side=%d", frontAttempts, sideAttempts)
	}
	cancelManager()
	waitDone := make(chan struct{})
	go func() {
		service.workers.Wait()
		close(waitDone)
	}()
	select {
	case <-waitDone:
	case <-time.After(2 * time.Second):
		t.Fatal("mutable source workers did not stop with manager")
	}
}

func TestStartServiceMaterializesTwoIsolatedSourceGraphs(t *testing.T) {
	cfg := twoSourceManagerConfig(t)
	ctx, cancel := context.WithCancel(context.Background())
	starter := func(
		graphCtx context.Context,
		graphConfig productconfig.SourceGraph,
		_ []productconfig.SourceRoleProfile,
		_ capability.Inventory,
		allocator *capabilityAllocator,
	) (*graphService, error) {
		done := make(chan error, 1)
		go func() {
			<-graphCtx.Done()
			done <- graphCtx.Err()
		}()
		analysisID := ""
		if graphConfig.Analysis != nil {
			analysisID = graphConfig.Analysis.ID
		}
		now := time.Now()
		return &graphService{
			sourceID: graphConfig.SourceID, analysisStreamID: analysisID, done: done,
			plan: videoBrokerPlan{CapabilityAllocator: allocator},
			runtime: &videoBrokerRuntime{
				plan: videoBrokerPlan{
					AnalysisEnabled: true, Transport: graphConfig.Source.Transport,
					CaptureDevice: graphConfig.Source.Device, CaptureFormat: graphConfig.Source.Format,
					SubFPS: graphConfig.Analysis.Config.FPS, FrameWatchdogSec: 12,
				},
				createdAt: now, childRunning: true, childStartedAt: now,
				lastFrameAt: now, lastSubFrameAt: now,
				mainClients: map[int]*brokerClient{}, archiveClients: map[int]*brokerClient{}, subClients: map[int]*brokerClient{},
				componentRecovery: map[string]hardwareComponentRecovery{},
			},
		}, nil
	}
	service, err := startService(ctx, cfg, capability.Inventory{Schema: capability.Schema}, starter)
	if err != nil {
		t.Fatal(err)
	}
	status := service.Snapshot(time.Now())
	if len(status.Sources) != 2 || len(status.Streams) != 2 ||
		status.Streams["front_analysis"].SourceRef != "front" ||
		status.Streams["side_analysis"].SourceRef != "side" {
		t.Fatalf("multi-source topology lost source bindings: %+v", status)
	}
	cancel()
	if err := service.Wait(context.Background()); err != nil {
		t.Fatalf("external manager shutdown was not clean: %v", err)
	}
}

func TestStartServiceKeepsHealthySourceRunningAcrossPeerStartupFailure(t *testing.T) {
	cfg := twoSourceManagerConfig(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	frontCanceled := make(chan struct{})
	sideRestarted := make(chan struct{})
	var allocatorSeen *capabilityAllocator
	var mu sync.Mutex
	sideAttempts := 0
	starter := func(
		graphCtx context.Context,
		graphConfig productconfig.SourceGraph,
		_ []productconfig.SourceRoleProfile,
		_ capability.Inventory,
		allocator *capabilityAllocator,
	) (*graphService, error) {
		allocatorSeen = allocator
		if graphConfig.SourceID == "side" {
			mu.Lock()
			sideAttempts++
			attempt := sideAttempts
			mu.Unlock()
			if attempt == 1 {
				allocator.mu.Lock()
				allocator.claims["test.side"] = map[string]int{"side": 1}
				allocator.mu.Unlock()
				return nil, errors.New("synthetic bind failure")
			}
			close(sideRestarted)
		}
		allocator.mu.Lock()
		allocator.claims["test."+graphConfig.SourceID] = map[string]int{graphConfig.SourceID: 1}
		allocator.mu.Unlock()
		done := make(chan error, 1)
		go func() {
			<-graphCtx.Done()
			if graphConfig.SourceID == "front" {
				close(frontCanceled)
			}
			select {
			case done <- graphCtx.Err():
			default:
			}
		}()
		return managerTestGraph(graphConfig, allocator, done, 1), nil
	}
	service, err := startServiceWithRestartPolicy(
		ctx, cfg, capability.Inventory{Schema: capability.Schema}, starter,
		sourceRestartPolicy{InitialDelay: 100 * time.Millisecond, MaxDelay: 100 * time.Millisecond, StableWindow: time.Second},
	)
	if err != nil || service == nil {
		t.Fatalf("isolated startup failure rejected the whole service: service=%v err=%v", service, err)
	}
	status := service.Snapshot(time.Now())
	if status.Sources["front"].State != "running" || status.Sources["side"].State != "degraded" ||
		!strings.Contains(status.Sources["side"].LastError, "synthetic bind failure") {
		t.Fatalf("startup failure was not source-local: %+v", status)
	}
	select {
	case <-frontCanceled:
		t.Fatal("peer startup failure canceled the healthy front source")
	default:
	}
	select {
	case <-sideRestarted:
	case <-time.After(time.Second):
		t.Fatal("failed side source did not retry locally")
	}
	select {
	case <-frontCanceled:
		t.Fatal("side retry canceled the healthy front source")
	default:
	}
	cancel()
	if err := service.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	allocatorSeen.mu.Lock()
	defer allocatorSeen.mu.Unlock()
	if len(allocatorSeen.claims) != 0 {
		t.Fatalf("shutdown retained source leases: %+v", allocatorSeen.claims)
	}
}

func TestDistinctRoleSourcesRemainIndependentAcrossLiveRestart(t *testing.T) {
	cfg := threeRoleSourceManagerConfig(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	type attemptRecord struct {
		ctx  context.Context
		done chan error
	}
	var mu sync.Mutex
	attempts := map[string][]attemptRecord{}
	starter := func(
		graphCtx context.Context,
		graphConfig productconfig.SourceGraph,
		_ []productconfig.SourceRoleProfile,
		_ capability.Inventory,
		allocator *capabilityAllocator,
	) (*graphService, error) {
		done := make(chan error, 1)
		mu.Lock()
		attempt := len(attempts[graphConfig.SourceID]) + 1
		attempts[graphConfig.SourceID] = append(
			attempts[graphConfig.SourceID], attemptRecord{ctx: graphCtx, done: done},
		)
		mu.Unlock()
		go func() {
			<-graphCtx.Done()
			select {
			case done <- graphCtx.Err():
			default:
			}
		}()
		return managerTestGraph(graphConfig, allocator, done, uint64(attempt)), nil
	}
	service, err := startServiceWithRestartPolicy(
		ctx, cfg, capability.Inventory{Schema: capability.Schema}, starter,
		sourceRestartPolicy{InitialDelay: 50 * time.Millisecond, MaxDelay: 50 * time.Millisecond, StableWindow: time.Second},
	)
	if err != nil {
		t.Fatal(err)
	}
	before := service.Snapshot(time.Now())
	for streamID, expectedSource := range map[string]string{
		"live": "live-camera", "analysis": "analysis-camera", "archive": "archive-camera",
	} {
		if before.Streams[streamID].SourceRef != expectedSource {
			t.Fatalf("stream %q binding=%+v", streamID, before.Streams[streamID])
		}
	}
	analysisEpoch := before.Sources["analysis-camera"].MediaEpoch
	archiveEpoch := before.Sources["archive-camera"].MediaEpoch
	mu.Lock()
	liveFirst := attempts["live-camera"][0]
	analysisFirst := attempts["analysis-camera"][0]
	archiveFirst := attempts["archive-camera"][0]
	mu.Unlock()
	liveFirst.done <- errors.New("synthetic live input failure")

	deadline := time.Now().Add(time.Second)
	for {
		mu.Lock()
		liveAttempts := len(attempts["live-camera"])
		analysisAttempts := len(attempts["analysis-camera"])
		archiveAttempts := len(attempts["archive-camera"])
		mu.Unlock()
		if liveAttempts >= 2 {
			if analysisAttempts != 1 || archiveAttempts != 1 {
				t.Fatalf("unrelated sources restarted: analysis=%d archive=%d", analysisAttempts, archiveAttempts)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("live source did not restart")
		}
		time.Sleep(time.Millisecond)
	}
	for sourceID, sourceCtx := range map[string]context.Context{
		"analysis-camera": analysisFirst.ctx,
		"archive-camera":  archiveFirst.ctx,
	} {
		select {
		case <-sourceCtx.Done():
			t.Fatalf("live restart canceled independent source %q", sourceID)
		default:
		}
	}
	after := service.Snapshot(time.Now())
	if after.Sources["analysis-camera"].MediaEpoch != analysisEpoch ||
		after.Sources["archive-camera"].MediaEpoch != archiveEpoch {
		t.Fatalf("unrelated source epochs changed: before=%+v after=%+v", before.Sources, after.Sources)
	}
	cancel()
	if err := service.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestSourceSupervisorRestartWaitsForRealEndpointTeardown(t *testing.T) {
	tmp := t.TempDir()
	mainSocket := filepath.Join(tmp, "live.sock")
	archiveSocket := filepath.Join(tmp, "archive.sock")
	analysisSocket := filepath.Join(tmp, "analysis.sock")
	analysisShared := filepath.Join(tmp, "analysis.nv12")
	cfg := productconfig.Config{
		SchemaVersion: productconfig.SchemaVersion,
		Sources: map[string]productconfig.SourceConfig{
			"camera": {
				Identity: "test-camera", Transport: "v4l2_uvc", Device: "/dev/video-test", Format: "nv12",
				Width: 1280, Height: 720, FPS: 30,
			},
		},
		Streams: map[string]productconfig.StreamConfig{
			"live": {
				Type: "live", SourceRef: "camera", Socket: mainSocket,
				Codec: "h264", Content: "processed", Width: 1280, Height: 720, FPS: 20,
			},
			"analysis": {
				Type: "analysis", SourceRef: "camera", Socket: analysisSocket,
				SharedMemory: analysisShared, PixelFormat: "nv12", Width: 640, Height: 360, FPS: 15,
			},
			"archive": {
				Type: "archive", SourceRef: "camera", Socket: archiveSocket,
				Codec: "h265", Content: "processed", Width: 1280, Height: 720, FPS: 30,
			},
		},
		Runtime: productconfig.RuntimeConfig{
			StateDirectory: filepath.Join(tmp, "state"), RuntimeDirectory: tmp,
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var mu sync.Mutex
	var runtimes []*videoBrokerRuntime
	secondStarted := make(chan struct{})
	starter := func(
		graphCtx context.Context,
		graphConfig productconfig.SourceGraph,
		_ []productconfig.SourceRoleProfile,
		_ capability.Inventory,
		allocator *capabilityAllocator,
	) (*graphService, error) {
		mu.Lock()
		attempt := len(runtimes) + 1
		mu.Unlock()
		runtimeCtx, cancelRuntime := context.WithCancel(graphCtx)
		t.Cleanup(cancelRuntime)
		plan := videoBrokerPlan{
			Enabled: true, SourceID: graphConfig.SourceID, SourceIdentity: graphConfig.Source.Identity,
			LiveEnabled: true, AnalysisEnabled: true, ArchiveEnabled: true,
			MainSocketPath: mainSocket, ArchiveSocketPath: archiveSocket,
			SubSocketPath: analysisSocket, SubShmPath: analysisShared,
			SubFormat: "nv12", SubWidth: 640, SubHeight: 360,
			CaptureFPS: 30, FrameWatchdogSec: 1, Transport: "unsupported-test",
			StateDirectory:      filepath.Join(tmp, "state", graphConfig.SourceID),
			CapabilityAllocator: allocator,
		}
		runtime, done := startVideoBroker(runtimeCtx, plan)
		if err := runtime.waitStartup(graphCtx); err != nil {
			cancelRuntime()
			<-done
			return nil, err
		}
		mu.Lock()
		runtimes = append(runtimes, runtime)
		mu.Unlock()
		if attempt == 1 {
			cancelRuntime()
		} else if attempt == 2 {
			close(secondStarted)
		}
		return &graphService{
			sourceID: graphConfig.SourceID, liveStreamID: "live",
			analysisStreamID: "analysis", archiveStreamID: "archive",
			runtime: runtime, done: done, plan: plan,
		}, nil
	}
	service, err := startServiceWithRestartPolicy(
		ctx, cfg, capability.Inventory{Schema: capability.Schema}, starter,
		sourceRestartPolicy{InitialDelay: 10 * time.Millisecond, MaxDelay: 10 * time.Millisecond, StableWindow: time.Second},
	)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-secondStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("source did not restart after the first real runtime stopped")
	}
	mu.Lock()
	if len(runtimes) != 2 {
		mu.Unlock()
		t.Fatalf("runtime attempts=%d want=2", len(runtimes))
	}
	firstRuntime, secondRuntime := runtimes[0], runtimes[1]
	mu.Unlock()
	firstRuntime.subShared.mu.Lock()
	firstSharedClosed := firstRuntime.subShared.file == nil
	firstRuntime.subShared.mu.Unlock()
	if !firstSharedClosed {
		t.Fatal("first runtime shared-memory file remained open across source restart")
	}
	if firstRuntime.subShared == secondRuntime.subShared || firstRuntime.subShared.Epoch() == secondRuntime.subShared.Epoch() {
		t.Fatal("source restart reused the previous shared-memory owner or epoch")
	}
	for _, socketPath := range []string{mainSocket, archiveSocket, analysisSocket} {
		conn, dialErr := net.DialTimeout("unix", socketPath, 250*time.Millisecond)
		if dialErr != nil {
			t.Fatalf("replacement runtime did not own endpoint %s: %v", socketPath, dialErr)
		}
		_ = conn.Close()
	}
	cancel()
	if err := service.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, socketPath := range []string{mainSocket, archiveSocket, analysisSocket} {
		if _, err := os.Lstat(socketPath); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("socket survived parent shutdown path=%s err=%v", socketPath, err)
		}
	}
	secondRuntime.subShared.mu.Lock()
	secondSharedClosed := secondRuntime.subShared.file == nil
	secondRuntime.subShared.mu.Unlock()
	if !secondSharedClosed {
		t.Fatal("replacement shared-memory file remained open after parent shutdown")
	}
}

func managerTestGraph(
	graphConfig productconfig.SourceGraph,
	allocator *capabilityAllocator,
	done <-chan error,
	attempt uint64,
) *graphService {
	now := time.Now()
	epoch := uint64(1000) + attempt
	if graphConfig.SourceID == "side" {
		epoch += 1000
	}
	runtime := &videoBrokerRuntime{
		plan: videoBrokerPlan{
			SourceIdentity: graphConfig.Source.Identity,
			LiveEnabled:    graphConfig.Live != nil, AnalysisEnabled: graphConfig.Analysis != nil,
			ArchiveEnabled: graphConfig.Archive != nil, Transport: graphConfig.Source.Transport,
			CaptureDevice: graphConfig.Source.Device, CaptureFormat: graphConfig.Source.Format,
			FrameWatchdogSec: 12, CapabilityAllocator: allocator,
		},
		createdAt: now, childRunning: true, childStartedAt: now,
		lastFrameAt: now, lastMainFrameAt: now, lastSubFrameAt: now, lastArchiveFrameAt: now,
		mediaEpoch:  epoch,
		mainClients: map[int]*brokerClient{}, archiveClients: map[int]*brokerClient{}, subClients: map[int]*brokerClient{},
		componentRecovery: map[string]hardwareComponentRecovery{},
	}
	service := &graphService{
		sourceID: graphConfig.SourceID, runtime: runtime, done: done,
		plan: videoBrokerPlan{CapabilityAllocator: allocator},
	}
	if graphConfig.Live != nil {
		service.liveStreamID = graphConfig.Live.ID
		runtime.plan.MainFPS = graphConfig.Live.Config.FPS
	}
	if graphConfig.Analysis != nil {
		service.analysisStreamID = graphConfig.Analysis.ID
		runtime.plan.SubFPS = graphConfig.Analysis.Config.FPS
	}
	if graphConfig.Archive != nil {
		service.archiveStreamID = graphConfig.Archive.ID
	}
	return service
}

func twoSourceManagerConfig(t *testing.T) productconfig.Config {
	t.Helper()
	cfg := productconfig.Config{
		SchemaVersion: productconfig.SchemaVersion,
		Sources: map[string]productconfig.SourceConfig{
			"front": {Identity: "front-camera", Transport: "v4l2_uvc", Device: "/dev/video0", Format: "nv12", Width: 1280, Height: 720, FPS: 30},
			"side":  {Identity: "side-camera", Transport: "v4l2_uvc", Device: "/dev/video2", Format: "nv12", Width: 1280, Height: 720, FPS: 30},
		},
		Streams: map[string]productconfig.StreamConfig{
			"front_analysis": {Type: "analysis", SourceRef: "front", Socket: "/run/video-broker/front.sock", PixelFormat: "nv12", Width: 640, Height: 360, FPS: 15},
			"side_analysis":  {Type: "analysis", SourceRef: "side", Socket: "/run/video-broker/side.sock", PixelFormat: "nv12", Width: 640, Height: 360, FPS: 15},
		},
		Runtime: productconfig.RuntimeConfig{StateDirectory: "/var/lib/video-broker", RuntimeDirectory: "/run/video-broker"},
	}
	normalized, err := productconfig.Normalize(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return normalized
}

func threeRoleSourceManagerConfig(t *testing.T) productconfig.Config {
	t.Helper()
	cfg := productconfig.Config{
		SchemaVersion: productconfig.SchemaVersion,
		Sources: map[string]productconfig.SourceConfig{
			"live-camera":     {Identity: "live-camera", Transport: "v4l2_uvc", Device: "/dev/video0", Format: "nv12", Width: 1280, Height: 720, FPS: 30},
			"analysis-camera": {Identity: "analysis-camera", Transport: "v4l2_uvc", Device: "/dev/video2", Format: "nv12", Width: 1280, Height: 720, FPS: 30},
			"archive-camera":  {Identity: "archive-camera", Transport: "v4l2_uvc", Device: "/dev/video4", Format: "nv12", Width: 1280, Height: 720, FPS: 30},
		},
		Streams: map[string]productconfig.StreamConfig{
			"live":     {Type: "live", SourceRef: "live-camera", Socket: "/run/video-broker/live.sock", Codec: "h264", Width: 1280, Height: 720, FPS: 20},
			"analysis": {Type: "analysis", SourceRef: "analysis-camera", SharedMemory: "/run/video-broker/analysis.nv12", NotifySocket: "/run/video-broker/analysis.notify", PixelFormat: "nv12", Width: 640, Height: 360, FPS: 15},
			"archive":  {Type: "archive", SourceRef: "archive-camera", Socket: "/run/video-broker/archive.sock", Codec: "h265", Width: 1280, Height: 720, FPS: 30},
		},
		Runtime: productconfig.RuntimeConfig{StateDirectory: "/var/lib/video-broker", RuntimeDirectory: "/run/video-broker"},
	}
	normalized, err := productconfig.Normalize(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return normalized
}
