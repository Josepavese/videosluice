package main

import (
	"bufio"
	"bytes"
	"context"
	"os"
	"strings"
	"testing"
	"time"

	videoproviders "github.com/Josepavese/videosluice/internal/source"
)

type oneByteWriter struct {
	bytes.Buffer
}

func (writer *oneByteWriter) Write(payload []byte) (int, error) {
	if len(payload) > 1 {
		payload = payload[:1]
	}
	return writer.Buffer.Write(payload)
}

func fakeGStreamerRole(
	role string,
	epoch uint64,
	cfg ingestConfig,
	elements gstElements,
) *isolatedGStreamerRole {
	stream := byte(2)
	codec := ""
	runtimeMode := ""
	if role == "live" {
		stream, codec = 1, cfg.liveCodec
		runtimeMode = videoproviders.WorkerFanoutRuntimeBackendStatic
	} else if role == "archive" {
		stream, codec = 3, cfg.archiveCodec
		runtimeMode = gstreamerArchiveRuntimeMode(cfg, elements)
	}
	return &isolatedGStreamerRole{
		role: role, stream: stream, codec: codec, runtimeMode: runtimeMode,
		branchEpoch: epoch, cfg: cfg,
		frameChanged: make(chan struct{}, 1), forwardDone: make(chan struct{}),
	}
}

func testGStreamerFanoutController(
	t *testing.T,
	cfg ingestConfig,
	elements gstElements,
	roles ...*isolatedGStreamerRole,
) (*gstreamerFanoutController, *gstreamerFrameRouter, *bytes.Buffer) {
	t.Helper()
	output := &bytes.Buffer{}
	router := newGStreamerFrameRouter(&muxWriter{out: output}, nil)
	if err := router.setInitial(roles); err != nil {
		t.Fatal(err)
	}
	controller, err := newGStreamerFanoutController(
		context.Background(), cfg, elements,
		gstreamerBusPaths{
			raw:       "/run/video-broker/test/raw.sock",
			sourceRaw: "/run/video-broker/test/source-raw.sock",
			native:    "/run/video-broker/test/native.sock",
		},
		router, roles,
	)
	if err != nil {
		t.Fatal(err)
	}
	controller.startRole = func(
		_ context.Context,
		candidate ingestConfig,
		candidateElements gstElements,
		_ gstreamerBusPaths,
		role string,
		epoch uint64,
	) (*isolatedGStreamerRole, error) {
		return fakeGStreamerRole(role, epoch, candidate, candidateElements), nil
	}
	controller.startForwarder = func(
		context.Context, *isolatedGStreamerRole, *gstreamerFrameRouter, time.Time,
	) {
	}
	return controller, router, output
}

func fanoutCommand(
	cfg ingestConfig,
	transaction string,
	sequence uint64,
	phase string,
	changes ...videoproviders.WorkerBranchChange,
) videoproviders.WorkerFanoutCommand {
	return videoproviders.WorkerFanoutCommand{
		Schema:        videoproviders.WorkerFanoutCommandSchema,
		TransactionID: transaction, Sequence: sequence,
		SourceID: cfg.sourceID, CapturePID: os.Getpid(), MediaEpoch: cfg.mediaEpoch,
		Phase: phase, Changes: changes,
	}
}

func TestGStreamerFanoutAnalysisRollbackHasNoEarlyOrLateFrames(t *testing.T) {
	cfg := ingestConfig{
		sourceID: "front", mediaEpoch: 17,
		sourceTransport: "v4l2", device: "/dev/video0", inputFormat: "nv12",
		analysisEnabled: true, orientation: "normal", aspectPolicy: "crop",
		captureWidth: 640, captureHeight: 360, captureFPS: 20,
		subWidth: 640, subHeight: 360, subFPS: 20, qualityStride: 8,
	}
	incumbent := fakeGStreamerRole("analysis", 1, cfg, gstElements{})
	controller, router, output := testGStreamerFanoutController(t, cfg, gstElements{}, incumbent)
	change := videoproviders.WorkerBranchChange{
		Role: "analysis", Action: videoproviders.WorkerFanoutActionReplace,
		Profile: &videoproviders.WorkerBranchProfile{
			PixelFormat: "nv12", Width: 320, Height: 180, FPS: 15,
		},
	}
	receipt := controller.handle(fanoutCommand(cfg, "switch-analysis", 1, "prepare", change))
	if receipt.Event != videoproviders.WorkerFanoutEventPrepared {
		t.Fatalf("prepare failed: %+v", receipt)
	}
	receipt = controller.handle(fanoutCommand(cfg, "switch-analysis", 2, "warmup"))
	if receipt.Event != videoproviders.WorkerFanoutEventWarmed {
		t.Fatalf("warmup failed: %+v", receipt)
	}
	candidate := controller.transaction.candidates["analysis"]
	if candidate == nil {
		t.Fatal("analysis candidate was not materialized")
	}
	if err := incumbent.observeAndDispatch(router, []byte{1}); err != nil {
		t.Fatal(err)
	}
	baselineBytes := output.Len()
	if err := candidate.observeAndDispatch(router, []byte{2}); err != nil {
		t.Fatal(err)
	}
	if output.Len() != baselineBytes {
		t.Fatal("candidate published before cutover")
	}
	if receipt = controller.handle(fanoutCommand(cfg, "switch-analysis", 3, "admit")); receipt.Event != videoproviders.WorkerFanoutEventAdmitted {
		t.Fatalf("admit failed: %+v", receipt)
	}
	if receipt = controller.handle(fanoutCommand(cfg, "switch-analysis", 4, "first_frame")); receipt.Event != videoproviders.WorkerFanoutEventFirstFrame || len(receipt.Branches) != 1 ||
		receipt.Branches[0].BranchEpoch != 2 || receipt.Branches[0].FrameID != 1 {
		t.Fatalf("first-frame evidence differs: %+v", receipt)
	}
	if receipt = controller.handle(fanoutCommand(cfg, "switch-analysis", 5, "cutover")); receipt.Event != videoproviders.WorkerFanoutEventCutover {
		t.Fatalf("cutover failed: %+v", receipt)
	}
	if err := incumbent.observeAndDispatch(router, []byte{3}); err != nil {
		t.Fatal(err)
	}
	if output.Len() != baselineBytes {
		t.Fatal("incumbent published after cutover")
	}
	if err := candidate.observeAndDispatch(router, []byte{4}); err != nil {
		t.Fatal(err)
	}
	if output.Len() == baselineBytes {
		t.Fatal("candidate did not publish after cutover")
	}
	if receipt = controller.handle(fanoutCommand(cfg, "switch-analysis", 6, "verify")); receipt.Event != videoproviders.WorkerFanoutEventVerified {
		t.Fatalf("verify failed: %+v", receipt)
	}
	cutoverBytes := output.Len()
	if receipt = controller.handle(fanoutCommand(cfg, "switch-analysis", 7, "rollback")); receipt.Event != videoproviders.WorkerFanoutEventRolledBack {
		t.Fatalf("rollback failed: %+v", receipt)
	}
	if err := candidate.observeAndDispatch(router, []byte{5}); err != nil {
		t.Fatal(err)
	}
	if output.Len() != cutoverBytes {
		t.Fatal("candidate published after rollback")
	}
	if err := incumbent.observeAndDispatch(router, []byte{6}); err != nil {
		t.Fatal(err)
	}
	if output.Len() == cutoverBytes || router.activeRole("analysis") != incumbent {
		t.Fatal("rollback did not restore the incumbent atomically")
	}
}

func TestGStreamerFanoutH265RequiresBootstrapAndRetiresIncumbent(t *testing.T) {
	originalInspect := inspectElement
	inspectElement = func(_ context.Context, element string) bool { return element == "x265enc" }
	t.Cleanup(func() { inspectElement = originalInspect })
	cfg := ingestConfig{
		sourceID: "front", mediaEpoch: 18,
		sourceTransport: "v4l2", device: "/dev/video0", inputFormat: "nv12",
		liveEnabled: true, liveCodec: "h265", encodeProvider: "software", allowSWFallback: true,
		orientation: "normal", aspectPolicy: "crop",
		captureWidth: 1280, captureHeight: 720, captureFPS: 20,
		mainWidth: 1280, mainHeight: 720, mainFPS: 20, liveBitrateKbps: 2048,
	}
	elements := gstElements{liveEncoder: "x265enc"}
	incumbent := fakeGStreamerRole("live", 1, cfg, elements)
	controller, router, _ := testGStreamerFanoutController(
		t, cfg, elements, incumbent)
	change := videoproviders.WorkerBranchChange{
		Role: "live", Action: videoproviders.WorkerFanoutActionReplace,
		Profile: &videoproviders.WorkerBranchProfile{
			Codec: "h265", Width: 1280, Height: 720, FPS: 20,
			BitrateKbps: 4096, Content: "processed", RateControl: "cbr",
		},
	}
	for sequence, phase := range []string{"prepare", "warmup", "admit"} {
		receipt := controller.handle(fanoutCommand(cfg, "switch-live", uint64(sequence+1), phase, func() []videoproviders.WorkerBranchChange {
			if phase == "prepare" {
				return []videoproviders.WorkerBranchChange{change}
			}
			return nil
		}()...))
		if receipt.Event == videoproviders.WorkerFanoutEventRejected {
			t.Fatalf("phase %s failed: %+v", phase, receipt)
		}
	}
	candidate := controller.transaction.candidates["live"]
	for _, payload := range [][]byte{
		{0, 0, 0, 1, 32 << 1, 1},
		{0, 0, 1, 33 << 1, 1},
		{0, 0, 0, 1, 34 << 1, 1},
	} {
		if err := candidate.observeAndDispatch(router, payload); err != nil {
			t.Fatal(err)
		}
	}
	if _, ready := candidate.evidence(); ready {
		t.Fatal("H265 candidate was admitted without a random-access picture")
	}
	if err := candidate.observeAndDispatch(router, []byte{0, 0, 0, 1, 19 << 1, 1}); err != nil {
		t.Fatal(err)
	}
	receipt := controller.handle(fanoutCommand(cfg, "switch-live", 4, "first_frame"))
	if receipt.Event != videoproviders.WorkerFanoutEventFirstFrame || len(receipt.Branches) != 1 ||
		receipt.Branches[0].CodecBootstrap != videoproviders.WorkerFanoutBootstrapComplete {
		t.Fatalf("H265 bootstrap evidence differs: %+v", receipt)
	}
	if receipt = controller.handle(fanoutCommand(cfg, "switch-live", 5, "cutover")); receipt.Event != videoproviders.WorkerFanoutEventCutover {
		t.Fatalf("cutover failed: %+v", receipt)
	}
	if err := candidate.observeAndDispatch(router, []byte{0, 0, 1, 19 << 1, 1}); err != nil {
		t.Fatal(err)
	}
	if receipt = controller.handle(fanoutCommand(cfg, "switch-live", 6, "verify")); receipt.Event != videoproviders.WorkerFanoutEventVerified {
		t.Fatalf("verify failed: %+v", receipt)
	}
	if receipt = controller.handle(fanoutCommand(cfg, "switch-live", 7, "retire")); receipt.Event != videoproviders.WorkerFanoutEventRetired {
		t.Fatalf("retire failed: %+v", receipt)
	}
	if controller.stable["live"] != candidate || controller.cfg.liveBitrateKbps != 4096 ||
		controller.nextEpoch["live"] != 2 {
		t.Fatal("retire did not promote the exact candidate")
	}
}

func TestGStreamerFanoutFenceAndSequenceAreFailClosed(t *testing.T) {
	cfg := ingestConfig{
		sourceID: "front", mediaEpoch: 19,
		sourceTransport: "v4l2", device: "/dev/video0", inputFormat: "nv12",
		analysisEnabled: true, orientation: "normal", aspectPolicy: "crop",
		captureWidth: 640, captureHeight: 360, captureFPS: 20,
		subWidth: 640, subHeight: 360, subFPS: 20,
	}
	incumbent := fakeGStreamerRole("analysis", 1, cfg, gstElements{})
	controller, _, _ := testGStreamerFanoutController(t, cfg, gstElements{}, incumbent)
	change := videoproviders.WorkerBranchChange{Role: "analysis", Action: videoproviders.WorkerFanoutActionDetach}
	wrong := fanoutCommand(cfg, "fence", 1, "prepare", change)
	wrong.CapturePID++
	if receipt := controller.handle(wrong); receipt.ErrorCode != "invalid_fence" {
		t.Fatalf("wrong PID was not fenced: %+v", receipt)
	}
	valid := fanoutCommand(cfg, "fence", 1, "prepare", change)
	if receipt := controller.handle(valid); receipt.Event != videoproviders.WorkerFanoutEventPrepared {
		t.Fatalf("valid fence after foreign command failed: %+v", receipt)
	}
	if receipt := controller.handle(valid); receipt.ErrorCode != "stale_sequence" {
		t.Fatalf("replayed sequence was not fenced: %+v", receipt)
	}
}

func TestGStreamerFanoutControlChannelRoundTripDetach(t *testing.T) {
	cfg := ingestConfig{
		sourceID: "front", mediaEpoch: 20,
		sourceTransport: "v4l2", device: "/dev/video0", inputFormat: "nv12",
		analysisEnabled: true, orientation: "normal", aspectPolicy: "crop",
		captureWidth: 640, captureHeight: 360, captureFPS: 20,
		subWidth: 640, subHeight: 360, subFPS: 20,
	}
	incumbent := fakeGStreamerRole("analysis", 1, cfg, gstElements{})
	controller, router, _ := testGStreamerFanoutController(t, cfg, gstElements{}, incumbent)
	change := videoproviders.WorkerBranchChange{Role: "analysis", Action: videoproviders.WorkerFanoutActionDetach}
	input := &bytes.Buffer{}
	for index, phase := range []string{
		"prepare", "warmup", "admit", "first_frame", "cutover", "verify", "retire",
	} {
		var changes []videoproviders.WorkerBranchChange
		if phase == "prepare" {
			changes = []videoproviders.WorkerBranchChange{change}
		}
		payload, err := videoproviders.EncodeWorkerFanoutCommand(
			fanoutCommand(cfg, "detach-analysis", uint64(index+1), phase, changes...))
		if err != nil {
			t.Fatal(err)
		}
		input.Write(payload)
	}
	output := &oneByteWriter{}
	err := runGStreamerFanoutControl(context.Background(), input, output, controller)
	if err == nil || !strings.Contains(err.Error(), "closed") {
		t.Fatalf("control EOF was not reported: %v", err)
	}
	scanner := bufio.NewScanner(&output.Buffer)
	events := make([]string, 0, 7)
	for scanner.Scan() {
		receipt, err := videoproviders.DecodeWorkerFanoutReceipt(
			append(append([]byte(nil), scanner.Bytes()...), '\n'))
		if err != nil {
			t.Fatal(err)
		}
		events = append(events, receipt.Event)
	}
	want := []string{"prepared", "warmed", "admitted", "first_frame", "cutover", "verified", "retired"}
	if strings.Join(events, ",") != strings.Join(want, ",") {
		t.Fatalf("control receipts=%v want=%v", events, want)
	}
	if router.activeRole("analysis") != nil || controller.stable["analysis"] != nil {
		t.Fatal("detach retire left an analysis role active")
	}
}

func TestGStreamerCodecBootstrapDistinguishesH264AndH265(t *testing.T) {
	for _, test := range []struct {
		codec   string
		payload []byte
	}{
		{codec: "h264", payload: []byte{
			0, 0, 0, 1, 7, 1, 0, 0, 1, 8, 1, 0, 0, 0, 1, 5, 1,
		}},
		{codec: "h265", payload: []byte{
			0, 0, 0, 1, 32 << 1, 1, 0, 0, 1, 33 << 1, 1,
			0, 0, 0, 1, 34 << 1, 1, 0, 0, 1, 19 << 1, 1,
		}},
	} {
		var bootstrap gstreamerCodecBootstrap
		bootstrap.observe(test.codec, test.payload)
		if !bootstrap.complete(test.codec) {
			t.Fatalf("%s bootstrap was not recognized", test.codec)
		}
	}
}
