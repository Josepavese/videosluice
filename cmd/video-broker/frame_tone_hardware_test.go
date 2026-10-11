//go:build linux && cgo && hardware

package main

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	fa "github.com/Josepavese/videosluice/frameaccess"
	fd "github.com/Josepavese/videosluice/framedomain"
	"github.com/Josepavese/videosluice/framedomain/entropy"
	"github.com/Josepavese/videosluice/framedomain/monotime"
	linux "github.com/Josepavese/videosluice/framepeer/provider/linux"
	wire "github.com/Josepavese/videosluice/framewire"
	"github.com/Josepavese/videosluice/internal/source"
	"golang.org/x/sys/unix"
)

// Opt-in, destructive to neither configuration nor corpus. The operator must
// first quiesce the existing camera owner through the deployment procedure.
// This is a real camera/codec/OpenCL/FAP lifecycle probe, not a Tracker or soak
// certification. Device modes must have been enumerated before selecting them.
func TestHardwareToneCaptureROIAndRestart(t *testing.T) {
	if os.Getenv("VIDEO_BROKER_HW_TONE_PROBE") != "1" {
		t.Skip("requires exclusive camera and explicit hardware probe opt-in")
	}
	device, bridge, logs := os.Getenv("VIDEO_BROKER_HW_CAMERA"), os.Getenv("VIDEO_BROKER_HW_BRIDGE"), os.Getenv("VIDEO_BROKER_HW_LOGS")
	for _, path := range []string{device, bridge, logs} {
		if !filepath.IsAbs(path) || path == "/" {
			t.Fatal("explicit absolute device, bridge and private log paths required")
		}
	}
	info, err := os.Stat(device)
	captureMust(t, err)
	if info.Mode()&os.ModeCharDevice == 0 {
		t.Fatal("capture source is not a character device")
	}
	controllerFD, err := unix.PidfdOpen(os.Getpid(), 0)
	captureMust(t, err)
	controller, err := linux.ImportProcessCapability(os.NewFile(uintptr(controllerFD), "hardware-probe-controller"), linux.ProcessIdentity{PID: os.Getpid(), UID: uint32(os.Getuid()), GID: uint32(os.Getgid()), Generation: "hardware.tone.probe"})
	captureMust(t, err)
	defer controller.Close()
	clock := monotime.New()
	runtime, err := fd.New(controller, entropy.System{}, clock, fd.Limits{MaxSources: 16, MaxStreams: 64, MaxSourceGenerations: 64, MaxMemoryBytes: 256 << 20, MaxSourceBytes: 96 << 20, MaxBuffers: 128, MaxBufferBytes: 16 << 20, MaxResources: 512, MaxResourceOperations: 16})
	captureMust(t, err)
	defer runtime.Revoke()
	bootstrap, err := fd.OwnRuntimeResource(context.Background(), runtime, fd.ResourcePlan[*linux.AcceptedLaunch]{Bytes: 4096, Create: func(context.Context) (fd.Created[*linux.AcceptedLaunch], error) {
		return fd.Created[*linux.AcceptedLaunch]{Value: &linux.AcceptedLaunch{Self: controller}, Owned: true}, nil
	}, Release: func(*linux.AcceptedLaunch) error { return nil }})
	captureMust(t, err)
	b := &nativeFrameBootstrap{runtime: runtime, resource: bootstrap, clock: clock, admission: nativeFrameAdmission{source: func(_ context.Context, p source.BrokerMuxPlan) (nativeFrameSourceAdmission, error) {
		return composeRDKFrameSource(p)
	}}}
	var previousEpoch uint64
	for _, mode := range []struct {
		codec         string
		width, height int
	}{{"h265", 1920, 1080}, {"mjpeg", 1280, 720}} {
		if !t.Run(mode.codec, func(t *testing.T) {
			plan := productSourcePlan()
			plan.SourceID, plan.AnalysisStreamID = "probe-camera", "probe-analysis"
			plan.CaptureDevicePath, plan.V4L2BridgePath = device, bridge
			plan.CaptureFormat, plan.CaptureWidth, plan.CaptureHeight = mode.codec, mode.width, mode.height
			plan.CaptureAspectPolicy, plan.WarmupSec = "crop", 5
			plan.MainWidth, plan.MainHeight, plan.MainFPS = mode.width, mode.height, 30
			plan.LiveCodec, plan.LiveBitrateKbps = "h265", 4096
			plan.MJPEGDecodeHWAvailable, plan.H265EncodeHWAvailable = true, true
			// Match the integrated processed archive: this exercises shared
			// source tone processing and real VPU encoding alongside FAP, without
			// writing a recording or injecting any media into the Registry corpus.
			plan.ArchiveEnabled, plan.ArchiveCodec, plan.ArchiveContent = true, "h265", "adaptive"
			plan.ArchiveWidth, plan.ArchiveHeight, plan.ArchiveFPS = 640, 360, 15
			plan.ArchiveBitrateKbps, plan.ArchiveRateControl = 750, "avbr"
			plan.ArchiveEncodeProvider = "native_sp"
			plan.CaptureSelectTimeoutMs, plan.CaptureMaxTimeouts = 2000, 3
			plan.ToneMappingEnabled, plan.ToneMappingMode, plan.ToneMappingAccelerator = true, "local", "opencl"
			plan.ToneMappingTarget, plan.ToneMappingHysteresis = 128, 12
			plan.ToneMappingGridColumns, plan.ToneMappingGridRows, plan.ToneMappingStatisticsHz = 16, 9, 8
			plan.ToneMappingMaxShadowGain, plan.ToneMappingMaxHighlightReduce, plan.ToneMappingTemporalAlpha = 2.4, .2, .35
			plan.ToneMappingNoiseGuard = true
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			capture, err := b.FrameCaptures()(ctx, plan)
			captureMust(t, err)
			c := capture.(*nativeFrameCapture)
			if c.SourceEpoch() <= previousEpoch {
				t.Fatal("source epoch reused after restart")
			}
			previousEpoch, plan.MediaEpoch = c.SourceEpoch(), c.SourceEpoch()
			cmd, _, err := source.BuildBrokerMuxCommand(ctx, plan)
			captureMust(t, err)
			logfile, err := os.OpenFile(filepath.Join(logs, mode.codec+"-native.log"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
			captureMust(t, err)
			defer logfile.Close()
			cmd.Stderr = logfile
			captureMust(t, c.Attach(cmd))
			// The real worker owns a persistent fanout control channel. EOF is
			// an owner failure, not an idle command stream.
			input, err := cmd.StdinPipe()
			captureMust(t, err)
			defer input.Close()
			captureMust(t, cmd.Start())
			waitDone := make(chan error, 1)
			go func() { waitDone <- cmd.Wait() }()
			waited := false
			defer func() {
				if !waited {
					cmd.Process.Signal(syscall.SIGTERM)
					select {
					case exitErr := <-waitDone:
						t.Logf("probe cleanup cooperative worker result=%v", exitErr)
					case <-time.After(10 * time.Second):
						cmd.Process.Kill()
						<-waitDone
						t.Error("probe cleanup exceeded native stop deadline")
					}
				}
				retireCtx, retireCancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer retireCancel()
				if err := c.Retire(retireCtx); err != nil {
					t.Errorf("checked retirement: %v", err)
				}
			}()
			captureMust(t, c.Started(ctx, cmd.Process))
			runCtx, stop := context.WithCancel(ctx)
			done := make(chan error, 1)
			defer stop()
			var delivered atomic.Uint64
			headers := make(chan wire.Header, 1)
			go func() {
				done <- c.Run(runCtx, func(_ context.Context, h wire.Header, p []byte) error {
					if h.SourceEpoch != c.SourceEpoch() || len(p) != 640*360*3/2 {
						return errors.New("delivery identity/shape")
					}
					delivered.Add(1)
					select {
					case headers <- h:
					default:
					}
					return nil
				})
			}()
			// Cancel and join receive before retirement even when an assertion fails.
			joined := false
			defer func() {
				if !joined {
					stop()
					<-done
				}
			}()
			select {
			case <-headers:
			case err := <-done:
				joined = true
				t.Fatalf("capture startup: %v", err)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			started, initial := time.Now(), delivered.Load()
			roiCompleted := 0
			roiPending := 0
			var roiRequested uint64
			roiUnavailable := make(map[string]int)
			for time.Since(started) < 20*time.Second {
				select {
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				case err := <-done:
					joined = true
					t.Fatalf("capture receive: %v", err)
				case h := <-headers:
					if roiCompleted >= 10 {
						continue
					}
					ref := fd.DeliveryRef{Fence: c.binding.Fence(), Ref: fa.DeliveryRef{Stream: h.Stream, Epoch: h.PublicationEpoch, FrameID: h.DeliveryID, Graph: h.Graph}}
					scope := c.binding.Scope()
					roiRequested++
					var requestID [16]byte
					binary.LittleEndian.PutUint64(requestID[:8], c.SourceEpoch())
					binary.LittleEndian.PutUint64(requestID[8:], roiRequested)
					request := fd.Request{ID: requestID, Frame: ref, Source: scope.Source, Surface: scope.Surface, Regions: []fa.Region{{ID: "native-center", BBox: [4]float64{160, 90, 480, 270}}}, DeadlineNS: clock.NowNS() + 100_000_000}
					result, extractErr := runtime.Extract(ctx, request)
					// Delivery and immutable source publication are independent.
					// Retry absent pixels or bounded producer/copy contention, with
					// the SAME identity and fixed 100ms deadline. Never use the next
					// frame, extend the deadline or bypass the shared resource quota.
					for (errors.Is(extractErr, fa.Expired) || errors.Is(extractErr, fa.CapacityRejected)) && clock.NowNS()+5_000_000 < request.DeadlineNS {
						roiPending++
						select {
						case <-ctx.Done():
							t.Fatal(ctx.Err())
						case <-time.After(5 * time.Millisecond):
						}
						result, extractErr = runtime.Extract(ctx, request)
					}
					if extractErr != nil {
						// The bounded latest-source store does not promise retention
						// of every delivery. Preserve this request's negative outcome;
						// a later delivery is a NEW request, never its replacement.
						if errors.Is(extractErr, fa.Expired) || errors.Is(extractErr, fa.CapacityRejected) || errors.Is(extractErr, fa.DeadlineExceeded) {
							roiUnavailable[extractErr.Error()]++
							if roiRequested <= 10 {
								t.Logf("native ROI unavailable request=%d origin=%d delivery=%d complete=%d reason=%v", roiRequested, h.OriginSequence, h.DeliveryID, roiCompleted, extractErr)
							}
							continue
						}
						t.Fatalf("native ROI request rejected source=%s epoch=%d origin=%d delivery=%d correlation=%d staged=%d pending_attempts=%d reason=%v", h.Source, h.SourceEpoch, h.OriginSequence, h.DeliveryID, h.Correlation, c.surfaces.Stats().StagedFrames, roiPending, extractErr)
					}
					complete := false
					visitErr := result.Visit(0, func(a fd.Artifact, pixels []byte) error {
						if a.State != fa.Complete {
							return fmt.Errorf("native ROI incomplete origin=%d state=%s", h.OriginSequence, a.State)
						}
						if len(pixels) != (mode.width/2)*(mode.height/2)*3/2 {
							return fmt.Errorf("native ROI shape: %d", len(pixels))
						}
						complete = true
						return fd.VerifyDomainProof(a.Proof, c.binding.Fence())
					})
					result.Close()
					captureMust(t, visitErr)
					if complete {
						roiCompleted++
					}
				}
			}
			fps := float64(delivered.Load()-initial) / time.Since(started).Seconds()
			if fps < 18 || roiCompleted != 10 {
				t.Fatalf("analysis progress fps=%.3f native_roi=%d requested=%d unavailable=%v", fps, roiCompleted, roiRequested, roiUnavailable)
			}
			captureMust(t, cmd.Process.Signal(syscall.SIGTERM))
			select {
			case err := <-waitDone:
				waited = true
				captureMust(t, err)
			case <-time.After(10 * time.Second):
				t.Fatal("native checked stop deadline")
			}
			stop()
			<-done
			joined = true
			captureMust(t, logfile.Sync())
			logData, err := os.ReadFile(logfile.Name())
			captureMust(t, err)
			if !strings.Contains(string(logData), "selected=opencl") || !strings.Contains(string(logData), "sdk_release=confirmed exit_status=0 tone_path=checked") {
				t.Fatal("missing actual OpenCL/checked teardown evidence")
			}
			t.Logf("hardware camera=%s native=%dx%d@30 analysis_fps=%.3f native_roi=%d requested=%d unavailable=%v pending_attempts=%d epoch=%d release=confirmed", mode.codec, mode.width, mode.height, fps, roiCompleted, roiRequested, roiUnavailable, roiPending, c.SourceEpoch())
		}) {
			break
		}
	}
}
