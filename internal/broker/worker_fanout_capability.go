package broker

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/Josepavese/videosluice/capability"
	productconfig "github.com/Josepavese/videosluice/config"
	videoproviders "github.com/Josepavese/videosluice/internal/source"
)

func resolveWorkerFanoutCapabilityTransitionLocked(
	runtime *videoBrokerRuntime,
	candidate productconfig.SourceGraph,
	changes []videoproviders.WorkerBranchChange,
	inventory capability.Inventory,
) (capability.Plan, capability.Plan, string, error) {
	if runtime == nil {
		return capability.Plan{}, capability.Plan{}, "", errors.New("worker fan-out runtime is unavailable")
	}
	candidatePlan, archiveCodec, err := resolveWorkerFanoutGraphCapabilityLocked(runtime, candidate, inventory)
	if err != nil {
		return capability.Plan{}, capability.Plan{}, "", err
	}
	warmup, err := workerFanoutWarmupCapabilityPlan(runtime.capabilityPlan, candidatePlan, changes)
	if err != nil {
		return capability.Plan{}, capability.Plan{}, "", err
	}
	return candidatePlan, warmup, archiveCodec, nil
}

func workerFanoutWarmupCapabilityPlan(
	currentPlan capability.Plan,
	candidatePlan capability.Plan,
	changes []videoproviders.WorkerBranchChange,
) (capability.Plan, error) {
	if currentPlan.WorkerProvider == "" || candidatePlan.WorkerProvider != currentPlan.WorkerProvider {
		return capability.Plan{}, errors.New("worker fan-out cannot change the capture worker provider")
	}
	if !reflect.DeepEqual(
		workerFanoutCaptureStages(currentPlan),
		workerFanoutCaptureStages(candidatePlan),
	) {
		return capability.Plan{}, errors.New("worker fan-out cannot change capture or decode stages")
	}
	warmRoles := map[string]bool{}
	for _, change := range changes {
		if change.Action == videoproviders.WorkerFanoutActionAttach ||
			change.Action == videoproviders.WorkerFanoutActionReplace {
			warmRoles[change.Role] = true
		}
	}
	warmup := capability.Plan{
		WorkerProvider:        candidatePlan.WorkerProvider,
		EncodeProvider:        candidatePlan.EncodeProvider,
		ArchiveEncodeProvider: candidatePlan.ArchiveEncodeProvider,
		DecodeProvider:        candidatePlan.DecodeProvider,
		ToneMapBackend:        candidatePlan.ToneMapBackend,
		ResizeBackend:         candidatePlan.ResizeBackend,
	}
	for _, stage := range candidatePlan.Stages {
		if warmRoles[stage.Role] &&
			(stage.Stage == capability.StageEncode || stage.Stage == capability.StageResize) {
			warmup.Stages = append(warmup.Stages, stage)
		}
	}
	return warmup, nil
}

type workerFanoutCapabilitySet struct {
	Current       map[string]capability.Plan
	Candidate     map[string]capability.Plan
	Warmup        map[string]capability.Plan
	Changes       map[string][]videoproviders.WorkerBranchChange
	ArchiveCodecs map[string]string
}

func buildWorkerFanoutCapabilitySet(
	transition preservedOwnerFanout,
	handles map[string]sourceFanoutHandle,
	inventory capability.Inventory,
) (workerFanoutCapabilitySet, error) {
	set := workerFanoutCapabilitySet{
		Current:       make(map[string]capability.Plan, len(transition.SourceIDs)),
		Candidate:     make(map[string]capability.Plan, len(transition.SourceIDs)),
		Warmup:        make(map[string]capability.Plan, len(transition.SourceIDs)),
		Changes:       make(map[string][]videoproviders.WorkerBranchChange, len(transition.SourceIDs)),
		ArchiveCodecs: make(map[string]string, len(transition.SourceIDs)),
	}
	for _, sourceID := range transition.SourceIDs {
		handle, exists := handles[sourceID]
		if !exists || handle.SourceID != sourceID || handle.Runtime == nil ||
			!reflect.DeepEqual(handle.CurrentGraph, transition.CurrentGraphs[sourceID]) ||
			!reflect.DeepEqual(handle.CandidateGraph, transition.CandidateGraphs[sourceID]) {
			return workerFanoutCapabilitySet{}, fmt.Errorf(
				"worker fan-out source %q has no exact runtime handle", sourceID)
		}
		handle.Runtime.mu.Lock()
		currentPlan := handle.Runtime.capabilityPlan
		candidatePlan, archiveCodec, err := resolveWorkerFanoutGraphCapabilityLocked(
			handle.Runtime, handle.CandidateGraph, inventory)
		handle.Runtime.mu.Unlock()
		if err != nil {
			return workerFanoutCapabilitySet{}, fmt.Errorf(
				"worker fan-out source %q candidate capability: %w", sourceID, err)
		}
		changes, err := workerFanoutChangesForSource(transition, sourceID, archiveCodec)
		if err != nil {
			return workerFanoutCapabilitySet{}, err
		}
		warmup, err := workerFanoutWarmupCapabilityPlan(currentPlan, candidatePlan, changes)
		if err != nil {
			return workerFanoutCapabilitySet{}, fmt.Errorf(
				"worker fan-out source %q capability transition: %w", sourceID, err)
		}
		set.Current[sourceID] = cloneFanoutCapabilityPlan(currentPlan)
		set.Candidate[sourceID] = cloneFanoutCapabilityPlan(candidatePlan)
		if len(warmup.Stages) > 0 {
			set.Warmup[sourceID] = warmup
		}
		set.Changes[sourceID] = changes
		set.ArchiveCodecs[sourceID] = archiveCodec
	}
	if len(set.Current) != len(handles) {
		return workerFanoutCapabilitySet{}, errors.New("worker fan-out handles include sources outside the transition")
	}
	return set, nil
}

func resolveWorkerFanoutGraphCapabilityLocked(
	runtime *videoBrokerRuntime,
	graph productconfig.SourceGraph,
	inventory capability.Inventory,
) (capability.Plan, string, error) {
	if graph.SourceID != runtime.plan.SourceID || graph.SourceID == "" {
		return capability.Plan{}, "", errors.New("worker fan-out graph differs from the running source")
	}
	captureFormat := capability.NormalizeFormat(runtime.observedCaptureCodec)
	if captureFormat == "" {
		captureFormat = capability.NormalizeFormat(runtime.plan.CaptureFormat)
	}
	captureWidth := positiveOr(runtime.activeCaptureWidth, runtime.plan.CaptureWidth)
	captureHeight := positiveOr(runtime.activeCaptureHeight, runtime.plan.CaptureHeight)
	captureFPS := positiveOr(runtime.activeCaptureFPS, runtime.plan.CaptureFPS)
	if captureFormat == "" || captureWidth <= 0 || captureHeight <= 0 || captureFPS <= 0 {
		return capability.Plan{}, "", errors.New("worker fan-out capture contract is not admitted")
	}
	mux := videoproviders.BrokerMuxPlan{
		Transport: runtime.plan.Transport, CaptureFormat: captureFormat,
		CaptureWidth: captureWidth, CaptureHeight: captureHeight, CaptureFPS: captureFPS,
		VideoOrientation:      runtime.plan.VideoOrientation,
		ToneMappingEnabled:    runtime.plan.ToneMappingEnabled,
		EncodeProvider:        graph.Source.Acceleration.EncodeProvider,
		ArchiveEncodeProvider: graph.Source.Acceleration.EncodeProvider,
		DecodeProvider:        graph.Source.Acceleration.DecodeProvider,
		RequireHWCodec:        graph.Source.Acceleration.RequireHardwareCodec,
		AllowSWFallback:       graph.Source.Acceleration.AllowSoftwareFallback,
	}
	if graph.Live != nil {
		stream := graph.Live.Config
		mux.LiveEnabled = true
		mux.LiveCodec = stream.Codec
		mux.MainWidth, mux.MainHeight, mux.MainFPS = stream.Width, stream.Height, stream.FPS
		mux.LiveBitrateKbps = stream.BitrateKbps
	}
	if graph.Analysis != nil {
		stream := graph.Analysis.Config
		mux.AnalysisEnabled = true
		mux.SubWidth, mux.SubHeight, mux.SubFPS = stream.Width, stream.Height, stream.FPS
	}
	if graph.Archive != nil {
		stream := graph.Archive.Config
		mux.ArchiveEnabled = true
		mux.ArchiveCodec = stream.Codec
		mux.ArchiveContent = stream.Content
		mux.ArchiveRateControl = stream.RateControl
		mux.ArchiveWidth, mux.ArchiveHeight, mux.ArchiveFPS = stream.Width, stream.Height, stream.FPS
		mux.ArchiveBitrateKbps = stream.BitrateKbps
	}
	archiveCodec, err := resolveBrokerArchiveCodec(mux, inventory)
	if err != nil {
		return capability.Plan{}, "", err
	}
	mux.ArchiveCodec = archiveCodec
	liveCodec := ""
	if mux.LiveEnabled {
		liveCodec = capability.NormalizeFormat(mux.LiveCodec)
	}
	policy := capability.ProviderPolicy{
		EncodeProvider:        graph.Source.Acceleration.EncodeProvider,
		DecodeProvider:        graph.Source.Acceleration.DecodeProvider,
		ToneMapAccelerator:    graph.Source.Processing.ToneMapping.Accelerator,
		RequireHardwareCodec:  graph.Source.Acceleration.RequireHardwareCodec,
		AllowSoftwareFallback: graph.Source.Acceleration.AllowSoftwareFallback,
	}
	plan, err := capability.Resolve(inventory, policy, capability.Requirements{
		Transport: mux.Transport, CaptureFormat: mux.CaptureFormat,
		LiveCodec: liveCodec, LiveEncode: brokerLiveEncodeRequired(mux),
		ArchiveCodec: archiveCodec, ArchiveEncode: brokerArchiveEncodeRequired(mux),
		ToneMap: mux.ToneMappingEnabled, ResizeRoles: brokerPixelTransformRoles(mux),
	})
	if err != nil {
		return capability.Plan{}, "", err
	}
	for _, role := range []string{"live", "archive"} {
		stream := workerFanoutNamedStream(graph, role)
		if stream == nil {
			continue
		}
		codec := capability.NormalizeFormat(stream.Config.Codec)
		if role == "archive" {
			codec = archiveCodec
		}
		widthAlignment, heightAlignment := capabilityEncodeAlignmentForRole(plan, role, codec)
		if widthAlignment <= 1 {
			widthAlignment = 1
		}
		if heightAlignment <= 1 {
			heightAlignment = 1
		}
		if stream.Config.Width%widthAlignment != 0 || stream.Config.Height%heightAlignment != 0 {
			return capability.Plan{}, "", fmt.Errorf(
				"worker fan-out %s geometry %dx%d violates admitted alignment %dx%d",
				role, stream.Config.Width, stream.Config.Height, widthAlignment, heightAlignment)
		}
	}
	return plan, archiveCodec, nil
}

func workerFanoutCaptureStages(plan capability.Plan) []capability.StageSelection {
	stages := make([]capability.StageSelection, 0, 2)
	for _, stage := range plan.Stages {
		if stage.Stage != capability.StageCapture && stage.Stage != capability.StageDecode {
			continue
		}
		stage.State = ""
		stage.Fallback = strings.TrimSpace(stage.Fallback)
		stages = append(stages, stage)
	}
	sort.Slice(stages, func(left, right int) bool {
		if stages[left].Stage != stages[right].Stage {
			return stages[left].Stage < stages[right].Stage
		}
		if stages[left].Role != stages[right].Role {
			return stages[left].Role < stages[right].Role
		}
		return stages[left].Format < stages[right].Format
	})
	return stages
}
