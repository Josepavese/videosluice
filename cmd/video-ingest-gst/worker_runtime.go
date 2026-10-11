package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	videoproviders "github.com/Josepavese/videosluice/internal/source"
)

const gstreamerBusReadyTimeout = 8 * time.Second

type isolatedGStreamerChild struct {
	name string
	cmd  *exec.Cmd
	done chan struct{}

	mu      sync.Mutex
	waitErr error
}

type isolatedGStreamerRole struct {
	role        string
	codec       string
	runtimeMode string
	stream      byte
	branchEpoch uint64
	cfg         ingestConfig
	child       *isolatedGStreamerChild
	reader      *os.File

	mu           sync.Mutex
	frameID      uint64
	bootstrap    gstreamerCodecBootstrap
	frameChanged chan struct{}
	forwardDone  chan struct{}
	forwardOnce  sync.Once
	forwardErr   error
}

func startIsolatedGStreamerChild(
	ctx context.Context,
	name string,
	args []string,
	extraFiles []*os.File,
) (*isolatedGStreamerChild, error) {
	if ctx == nil || name == "" || len(args) == 0 {
		return nil, errors.New("GStreamer child definition is incomplete")
	}
	cmd := exec.CommandContext(ctx, "gst-launch-1.0", args...)
	configureGStreamerChildLifecycle(cmd)
	cmd.ExtraFiles = extraFiles
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start %s GStreamer graph: %w", name, err)
	}
	child := &isolatedGStreamerChild{name: name, cmd: cmd, done: make(chan struct{})}
	go func() {
		err := cmd.Wait()
		child.mu.Lock()
		child.waitErr = err
		child.mu.Unlock()
		close(child.done)
	}()
	return child, nil
}

func (child *isolatedGStreamerChild) result() error {
	if child == nil {
		return nil
	}
	child.mu.Lock()
	defer child.mu.Unlock()
	return child.waitErr
}

func (child *isolatedGStreamerChild) stop(grace time.Duration) error {
	if child == nil || child.cmd == nil || child.cmd.Process == nil {
		return nil
	}
	select {
	case <-child.done:
		if gstreamerProcessGroupExists(child.cmd.Process.Pid) {
			_ = syscall.Kill(-child.cmd.Process.Pid, syscall.SIGKILL)
		}
		return expectedGStreamerStopResult(child.result())
	default:
	}
	pid := child.cmd.Process.Pid
	var signalErr error
	if err := syscall.Kill(-pid, syscall.SIGTERM); err != nil && !errors.Is(err, syscall.ESRCH) {
		signalErr = fmt.Errorf("signal %s GStreamer group TERM: %w", child.name, err)
	}
	if err := syscall.Kill(-pid, syscall.SIGCONT); err != nil && !errors.Is(err, syscall.ESRCH) {
		signalErr = errors.Join(signalErr, fmt.Errorf("signal %s GStreamer group CONT: %w", child.name, err))
	}
	timer := time.NewTimer(grace)
	defer timer.Stop()
	select {
	case <-child.done:
		if gstreamerProcessGroupExists(pid) {
			_ = syscall.Kill(-pid, syscall.SIGKILL)
		}
		return errors.Join(signalErr, expectedGStreamerStopResult(child.result()))
	case <-timer.C:
		if err := syscall.Kill(-pid, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
			signalErr = errors.Join(signalErr, fmt.Errorf("kill %s GStreamer group: %w", child.name, err))
		}
	}
	killTimer := time.NewTimer(gstChildKillWait)
	defer killTimer.Stop()
	select {
	case <-child.done:
		return errors.Join(signalErr, expectedGStreamerStopResult(child.result()))
	case <-killTimer.C:
		return errors.Join(signalErr, fmt.Errorf("%s GStreamer graph did not exit after group kill", child.name))
	}
}

func expectedGStreamerStopResult(err error) error {
	if err == nil || errors.Is(err, context.Canceled) {
		return nil
	}
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		return err
	}
	status, ok := exitErr.Sys().(syscall.WaitStatus)
	if ok && status.Signaled() && (status.Signal() == syscall.SIGTERM || status.Signal() == syscall.SIGKILL) {
		return nil
	}
	return err
}

func waitForGStreamerBus(
	ctx context.Context,
	child *isolatedGStreamerChild,
	path string,
) error {
	if ctx == nil || child == nil || !canonicalPrivateBusPath(path) {
		return errors.New("GStreamer bus readiness request is invalid")
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	timer := time.NewTimer(gstreamerBusReadyTimeout)
	defer timer.Stop()
	for {
		info, err := os.Lstat(path)
		if err == nil {
			if info.Mode()&os.ModeSymlink != 0 || info.Mode()&os.ModeSocket == 0 {
				return fmt.Errorf("GStreamer bus %s is not an owned Unix socket", path)
			}
			return nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("inspect GStreamer bus %s: %w", path, err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-child.done:
			return fmt.Errorf("capture GStreamer graph exited before bus %s: %w", path, child.result())
		case <-timer.C:
			return fmt.Errorf("timed out waiting for GStreamer bus %s", path)
		case <-ticker.C:
		}
	}
}

func startIsolatedGStreamerRole(
	ctx context.Context,
	cfg ingestConfig,
	elements gstElements,
	buses gstreamerBusPaths,
	role string,
	branchEpoch uint64,
) (*isolatedGStreamerRole, error) {
	if branchEpoch == 0 {
		return nil, errors.New("GStreamer branch epoch is required")
	}
	args, err := buildGStreamerRoleArgs(cfg, elements, buses, role)
	if err != nil {
		return nil, err
	}
	reader, writer, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	child, err := startIsolatedGStreamerChild(ctx, role, args, []*os.File{writer})
	_ = writer.Close()
	if err != nil {
		_ = reader.Close()
		return nil, err
	}
	result := &isolatedGStreamerRole{
		role: role, branchEpoch: branchEpoch, cfg: cfg,
		child: child, reader: reader,
		frameChanged: make(chan struct{}, 1), forwardDone: make(chan struct{}),
	}
	switch role {
	case "live":
		result.codec, result.stream = cfg.liveCodec, 1
		result.runtimeMode = videoproviders.WorkerFanoutRuntimeBackendStatic
	case "analysis":
		result.stream = 2
	case "archive":
		result.runtimeMode = gstreamerArchiveRuntimeMode(cfg, elements)
		if archiveUsesNativeSource(cfg) {
			result.codec = cfg.inputFormat
			result.stream = 3
			if result.codec == "h264" {
				result.stream = 4
			}
		} else {
			result.codec = cfg.archiveCodec
			result.stream = 3
			if result.codec == "h264" {
				result.stream = 6
			}
		}
	}
	return result, nil
}

func runIsolatedGStreamer(ctx context.Context, cfg ingestConfig, elements gstElements) (runErr error) {
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	busDirectory, err := os.MkdirTemp("", "video-ingest-gst-")
	if err != nil {
		return err
	}
	defer func() { runErr = errors.Join(runErr, os.RemoveAll(busDirectory)) }()
	buses := gstreamerBusPaths{
		raw:       filepath.Join(busDirectory, "raw.sock"),
		sourceRaw: filepath.Join(busDirectory, "source-raw.sock"),
		native:    filepath.Join(busDirectory, "native.sock"),
	}
	captureArgs, err := buildGStreamerCaptureArgs(cfg, elements, buses)
	if err != nil {
		return err
	}
	var qualitySourceR, qualitySourceW, qualityProcessedR, qualityProcessedW *os.File
	var captureFiles []*os.File
	if cfg.qualityEnabled {
		qualitySourceR, qualitySourceW, err = os.Pipe()
		if err != nil {
			return err
		}
		defer qualitySourceR.Close()
		defer qualitySourceW.Close()
		qualityProcessedR, qualityProcessedW, err = os.Pipe()
		if err != nil {
			return err
		}
		defer qualityProcessedR.Close()
		defer qualityProcessedW.Close()
		captureFiles = []*os.File{qualitySourceW, qualityProcessedW}
	}
	capture, err := startIsolatedGStreamerChild(workerCtx, "capture", captureArgs, captureFiles)
	if cfg.qualityEnabled {
		_ = qualitySourceW.Close()
		_ = qualityProcessedW.Close()
	}
	if err != nil {
		return err
	}
	roles := make([]*isolatedGStreamerRole, 0, 3)
	var controller *gstreamerFanoutController
	defer func() {
		cancel()
		if controller != nil {
			if stopErr := controller.stopAll(); runErr == nil && ctx.Err() == nil && stopErr != nil {
				runErr = stopErr
			}
		} else {
			for _, role := range roles {
				if stopErr := stopGStreamerRole(role); runErr == nil && ctx.Err() == nil && stopErr != nil {
					runErr = stopErr
				}
			}
		}
		if stopErr := capture.stop(gstChildTerminationGrace); runErr == nil && ctx.Err() == nil && stopErr != nil {
			runErr = stopErr
		}
	}()
	if rawProcessingRequired(cfg) {
		if err := waitForGStreamerBus(workerCtx, capture, buses.raw); err != nil {
			return err
		}
	}
	if gstreamerSourceRawBusRequired(cfg) {
		if err := waitForGStreamerBus(workerCtx, capture, buses.sourceRaw); err != nil {
			return err
		}
	}
	if gstreamerNativeBusRequired(cfg) {
		if err := waitForGStreamerBus(workerCtx, capture, buses.native); err != nil {
			return err
		}
	}
	writer := &muxWriter{out: os.Stdout}
	qualityRequests := make(chan qualityRequest)
	router := newGStreamerFrameRouter(writer, qualityRequests)
	for _, role := range []struct {
		name    string
		enabled bool
	}{
		{name: "live", enabled: cfg.liveEnabled},
		{name: "analysis", enabled: cfg.analysisEnabled},
		{name: "archive", enabled: cfg.archiveEnabled},
	} {
		if !role.enabled {
			continue
		}
		started, err := startIsolatedGStreamerRole(workerCtx, cfg, elements, buses, role.name, 1)
		if err != nil {
			return fmt.Errorf("start GStreamer %s role: %w", role.name, err)
		}
		roles = append(roles, started)
	}
	if err := router.setInitial(roles); err != nil {
		return err
	}
	warmupUntil := time.Now().Add(time.Duration(cfg.warmupSec) * time.Second)
	events := make(chan error, len(roles)+3)
	go func() {
		<-capture.done
		if workerCtx.Err() == nil {
			err := capture.result()
			if err == nil {
				err = errors.New("unexpected clean exit")
			}
			events <- fmt.Errorf("capture GStreamer graph: %w", err)
		}
	}()
	for _, role := range roles {
		startGStreamerRoleForwarder(workerCtx, role, router, warmupUntil)
	}
	fanoutMode := "disabled"
	var fanoutFatal <-chan error
	if gstreamerFanoutIdentityValid(cfg) {
		controller, err = newGStreamerFanoutController(workerCtx, cfg, elements, buses, router, roles)
		if err != nil {
			return err
		}
		fanoutFatal = controller.fatal
		fanoutMode = "branch_transaction_v1"
		go func() {
			if err := runGStreamerFanoutControl(workerCtx, os.Stdin, os.Stderr, controller); workerCtx.Err() == nil {
				events <- err
			}
		}()
	} else {
		for _, role := range roles {
			role := role
			go func() {
				<-role.forwardDone
				if workerCtx.Err() == nil {
					err, _ := role.forwardResult()
					events <- fmt.Errorf("forward GStreamer %s role: %w", role.role, err)
				}
			}()
		}
	}
	emitGStreamerRuntimeCapability(cfg, elements, fanoutMode)
	if cfg.qualityEnabled {
		go func() {
			err := forwardQualityCapture(
				workerCtx, qualitySourceR, qualityProcessedR, cfg, warmupUntil, qualityRequests,
				func() *isolatedGStreamerRole { return router.activeRole("analysis") })
			if workerCtx.Err() == nil {
				events <- fmt.Errorf("forward GStreamer quality: %w", err)
			}
		}()
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-events:
		return err
	case err := <-fanoutFatal:
		return err
	}
}
