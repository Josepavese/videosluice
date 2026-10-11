package broker

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	productconfig "github.com/Josepavese/videosluice/config"
	videoproviders "github.com/Josepavese/videosluice/internal/source"
)

func TestFanoutLiveControlFollowsAttestedModeAndTransport(t *testing.T) {
	for _, test := range []struct {
		name, mode                    string
		available, removed, supported bool
	}{
		{name: "dynamic", mode: videoproviders.WorkerFanoutRuntimePersistentEncoder, available: true, supported: true},
		{name: "missing_transport", mode: videoproviders.WorkerFanoutRuntimePersistentEncoder},
		{name: "native_passthrough", mode: videoproviders.WorkerFanoutRuntimeNativeStatic, available: true},
		{name: "removed", available: true, removed: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			runtime := &videoBrokerRuntime{
				childControlAvailable: test.available,
				childControlSupported: !test.supported,
			}
			role := &runtimeFanoutPublicationRole{
				key: fanoutPublicationRoleKey{role: "live"}, runtime: runtime,
				candidateRuntimeMode: test.mode, candidate: &productconfig.NamedStream{},
			}
			if test.removed {
				role.candidate = nil
			}
			activateFanoutCandidateRoleLocked(role, videoBrokerPlan{})
			if runtime.childControlSupported != test.supported {
				t.Fatalf("mode=%q available=%t retained stale control support=%t",
					runtime.childLiveControlMode, test.available, runtime.childControlSupported)
			}
		})
	}
}

func TestFanoutLiveControlAppliesDemandAndThermalCapsAfterSwitch(t *testing.T) {
	for _, test := range []struct {
		name                 string
		governor             resourceGovernorDecision
		viewer               bool
		fps, bitrate, subFPS int
	}{
		{name: "idle_nominal", governor: resourceGovernorDecision{Mode: "nominal", OptionalComputeAdmitted: true}, fps: 20, bitrate: 4096, subFPS: 20},
		{name: "idle_thermal_soft", governor: resourceGovernorDecision{Mode: "thermal_soft", MainFPSCap: 18, MainBitrateKbpsCap: 3000, SubFPS: 15}, fps: 18, bitrate: 3000, subFPS: 15},
		{name: "idle_thermal_hard", governor: resourceGovernorDecision{Mode: "thermal_hard", MainFPSCap: 15, MainBitrateKbpsCap: 1843, SubFPS: 10}, fps: 15, bitrate: 1843, subFPS: 10},
		{name: "viewer_thermal_hard", governor: resourceGovernorDecision{Mode: "thermal_hard", MainFPSCap: 15, MainBitrateKbpsCap: 1843, SubFPS: 10}, viewer: true, fps: 15, bitrate: 1843, subFPS: 10},
	} {
		t.Run(test.name, func(t *testing.T) {
			transition := fanoutTransitionForRoleTest(t, "live", 3)
			fixture := newWorkerFanoutBackendFixtureForTransition(t, transition, 3)
			installFanoutCurrentPublications(t, &fixture)
			fixture.backend.publications = newRuntimeFanoutPublicationCoordinator()
			from := fixture.handles[transition.Moves[0].From].Runtime
			to := fixture.handles[transition.Moves[0].To].Runtime
			seedFanoutLiveControl(from, true, test.governor)
			seedFanoutLiveControl(to, false, test.governor)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			prepared, err := fixture.backend.PrepareFanout(ctx, transition, fixture.handles)
			if err != nil {
				t.Fatal(err)
			}
			for _, phase := range []func(context.Context) error{
				prepared.Warmup, prepared.Admit, prepared.AwaitFirstFrame, prepared.Cutover,
			} {
				if err := phase(ctx); err != nil {
					t.Fatal(err)
				}
			}
			to.mu.Lock()
			if !to.childControlSupported || !to.routingLiveDemand {
				t.Errorf("switch did not arm control and verification demand: supported=%t demand=%t",
					to.childControlSupported, to.routingLiveDemand)
			}
			control := to.bridgeControlTargetsLocked()
			if !control.MainActive || control.MainFPS != test.fps ||
				control.MainBitrateKbps != test.bitrate || control.SubFPS != test.subFPS ||
				control.OptionalComputeAdmitted != test.governor.OptionalComputeAdmitted {
				t.Errorf("verification bypassed current governor: %+v", control)
			}
			assertFanoutQueuedControl(t, to, "fanout_live_cutover", control)
			if test.viewer {
				server, peer := net.Pipe()
				t.Cleanup(func() { server.Close(); peer.Close() })
				to.mainClients[101] = newBrokerClient(brokerStreamH264, server, time.Second, 20)
			}
			to.mu.Unlock()
			if !test.viewer {
				// A warmed hardware encoder may emit an IDR without repeating
				// codec configuration. Routing verification is real live demand
				// even before the first external viewer connects.
				to.mu.Lock()
				to.requestMainBootstrapRecoveryLocked(time.Now())
				control = to.bridgeControlTargetsLocked()
				assertFanoutQueuedControl(t, to, "live_bootstrap_missing_codec_configuration", control)
				if to.mainKeyframeRequestToken == 0 {
					t.Error("viewer-free verification did not request hardware headers plus IDR")
				}
				to.mu.Unlock()
			}
			attestFanoutCandidateFrame(t, to, "live")
			if err := prepared.Verify(ctx); err != nil {
				t.Fatal(err)
			}
			to.mu.Lock()
			control = to.bridgeControlTargetsLocked()
			if to.routingLiveDemand || control.MainActive != test.viewer || control.MainFPS != test.fps ||
				control.MainBitrateKbps != test.bitrate || control.SubFPS != test.subFPS ||
				control.OptionalComputeAdmitted != test.governor.OptionalComputeAdmitted {
				t.Errorf("verified switch retained demand or ignored caps: %+v demand=%t", control, to.routingLiveDemand)
			}
			assertFanoutQueuedControl(t, to, "fanout_live_verified", control)
			if !test.viewer {
				token := to.mainKeyframeRequestToken
				to.requestMainBootstrapRecoveryLocked(time.Now().Add(brokerMainBootstrapRecoveryEvery))
				if to.mainKeyframeRequestToken != token || len(to.childControl) != 0 {
					t.Error("completed viewer-free verification retained bootstrap recovery work")
				}
			}
			if test.viewer {
				to.detachClientLocked(brokerStreamH264, 101)
				control = to.bridgeControlTargetsLocked()
				if control.MainActive {
					t.Error("last viewer detached but encoder remains active")
				}
				assertFanoutQueuedControl(t, to, "h264_client_detached", control)
			}
			to.mu.Unlock()
			if err := prepared.Retire(ctx); err != nil {
				t.Fatal(err)
			}
			from.mu.Lock()
			if from.childControlSupported || from.childLiveControlMode != "disabled" ||
				from.bridgeControlTargetsLocked().MainActive {
				t.Error("retired source retained live control or demand")
			}
			from.mu.Unlock()
		})
	}
}

func TestFanoutLiveControlRollbackRestoresBothOwners(t *testing.T) {
	for _, boundary := range []string{"cutover", "verified", "verify_timeout", "lost_control"} {
		t.Run(boundary, func(t *testing.T) {
			testFanoutLiveControlRollback(t, boundary)
		})
	}
}

func testFanoutLiveControlRollback(t *testing.T, boundary string) {
	t.Helper()
	transition := fanoutTransitionForRoleTest(t, "live", 2)
	fixture := newWorkerFanoutBackendFixtureForTransition(t, transition, 3)
	installFanoutCurrentPublications(t, &fixture)
	fixture.backend.publications = newRuntimeFanoutPublicationCoordinator()
	from := fixture.handles[transition.Moves[0].From].Runtime
	to := fixture.handles[transition.Moves[0].To].Runtime
	governor := resourceGovernorDecision{Mode: "thermal_hard", MainFPSCap: 15, MainBitrateKbpsCap: 1843, SubFPS: 10}
	seedFanoutLiveControl(from, true, governor)
	seedFanoutLiveControl(to, false, governor)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	prepared, err := fixture.backend.PrepareFanout(ctx, transition, fixture.handles)
	if err != nil {
		t.Fatal(err)
	}
	for _, phase := range []func(context.Context) error{
		prepared.Warmup, prepared.Admit, prepared.AwaitFirstFrame, prepared.Cutover,
	} {
		if err := phase(ctx); err != nil {
			t.Fatal(err)
		}
	}
	switch boundary {
	case "verified":
		attestFanoutCandidateFrame(t, to, "live")
		if err := prepared.Verify(ctx); err != nil {
			t.Fatal(err)
		}
	case "verify_timeout":
		verifyCtx, cancelVerify := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancelVerify()
		if err := prepared.Verify(verifyCtx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("missing proof frame did not time out: %v", err)
		}
	case "lost_control":
		attestFanoutCandidateFrame(t, to, "live")
		to.mu.Lock()
		to.childControlSupported = false
		to.mu.Unlock()
		if err := prepared.Verify(ctx); err == nil || !strings.Contains(err.Error(), "runtime control differs") {
			t.Fatalf("lost runtime control was accepted: %v", err)
		}
	}
	if err := prepared.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	for _, runtime := range []*videoBrokerRuntime{from, to} {
		runtime.mu.Lock()
		wantSupported := runtime == from
		if runtime.childControlSupported != wantSupported || runtime.routingLiveDemand {
			t.Errorf("rollback control support=%t want=%t verification demand=%t",
				runtime.childControlSupported, wantSupported, runtime.routingLiveDemand)
		}
		control := runtime.bridgeControlTargetsLocked()
		if control.MainActive {
			t.Error("rollback left encoder active without viewers")
		}
		if wantSupported && (control.MainFPS != 15 || control.MainBitrateKbps != 1843 ||
			control.SubFPS != 10 || control.OptionalComputeAdmitted) {
			t.Errorf("rollback bypassed incumbent governor: %+v", control)
		}
		assertFanoutQueuedControl(t, runtime, "fanout_live_rollback", control)
		runtime.mu.Unlock()
	}
}

func seedFanoutLiveControl(runtime *videoBrokerRuntime, enabled bool, governor resourceGovernorDecision) {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	runtime.childControlAvailable = true
	runtime.childControlSupported = enabled
	runtime.childLiveControlMode = "disabled"
	if enabled {
		runtime.childLiveControlMode = videoproviders.WorkerFanoutRuntimePersistentEncoder
	}
	runtime.childControl = make(chan bridgeRuntimeControl, 1)
	runtime.governor = governor
}

func assertFanoutQueuedControl(t *testing.T, runtime *videoBrokerRuntime, reason string, want bridgeRuntimeControl) {
	t.Helper()
	select {
	case got := <-runtime.childControl:
		if got.Sequence == 0 || got.Reason != reason || got.MainActive != want.MainActive ||
			got.MainFPS != want.MainFPS || got.MainBitrateKbps != want.MainBitrateKbps ||
			got.SubFPS != want.SubFPS || got.OptionalComputeAdmitted != want.OptionalComputeAdmitted {
			t.Errorf("queued control=%+v want targets=%+v reason=%q", got, want, reason)
		}
	default:
		t.Errorf("no worker control queued for %s", reason)
	}
}
