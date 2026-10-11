package source

import (
	"bytes"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestWorkerFanoutPrepareCommandRoundTrip(t *testing.T) {
	command := WorkerFanoutCommand{
		Schema: WorkerFanoutCommandSchema, TransactionID: "route:01", Sequence: 7,
		SourceID: "camera-front", CapturePID: 4123, MediaEpoch: 99,
		Phase: WorkerFanoutPhasePrepare,
		Changes: []WorkerBranchChange{
			{
				Role: "analysis", Action: WorkerFanoutActionAttach,
				Profile: &WorkerBranchProfile{
					PixelFormat: "nv12", Width: 640, Height: 360, FPS: 20,
				},
			},
			{
				Role: "archive", Action: WorkerFanoutActionReplace,
				Profile: &WorkerBranchProfile{
					Codec: "h265", Width: 640, Height: 360, FPS: 15,
					BitrateKbps: 750, Content: "adaptive", RateControl: "avbr",
				},
			},
			{Role: "live", Action: WorkerFanoutActionDetach},
		},
	}
	payload, err := EncodeWorkerFanoutCommand(command)
	if err != nil {
		t.Fatal(err)
	}
	if len(payload) > WorkerFanoutMaxLineBytes || !bytes.HasSuffix(payload, []byte{'\n'}) ||
		bytes.Contains(payload, []byte("/dev/")) || bytes.Contains(payload, []byte("socket")) {
		t.Fatalf("worker command leaked forbidden transport fields: %s", payload)
	}
	decoded, err := DecodeWorkerFanoutCommand(payload)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded, command) {
		t.Fatalf("worker command round trip differs: got=%+v want=%+v", decoded, command)
	}
}

func TestWorkerFanoutPhaseCommandCarriesOnlyIdentityFence(t *testing.T) {
	for _, phase := range []string{
		WorkerFanoutPhaseWarmup, WorkerFanoutPhaseAdmit, WorkerFanoutPhaseFirstFrame,
		WorkerFanoutPhaseCutover, WorkerFanoutPhaseVerify, WorkerFanoutPhaseRollback,
		WorkerFanoutPhaseAbort, WorkerFanoutPhaseRetire,
	} {
		command := WorkerFanoutCommand{
			Schema: WorkerFanoutCommandSchema, TransactionID: "route-2", Sequence: 2,
			SourceID: "overview", CapturePID: 5, MediaEpoch: 6, Phase: phase,
		}
		payload, err := EncodeWorkerFanoutCommand(command)
		if err != nil {
			t.Fatalf("phase %s: %v", phase, err)
		}
		if _, err := DecodeWorkerFanoutCommand(payload); err != nil {
			t.Fatalf("phase %s round trip: %v", phase, err)
		}
	}
}

func TestWorkerFanoutReceiptMatchesOnlyExactPhase(t *testing.T) {
	tests := []struct {
		phase string
		event string
	}{
		{WorkerFanoutPhasePrepare, WorkerFanoutEventPrepared},
		{WorkerFanoutPhaseWarmup, WorkerFanoutEventWarmed},
		{WorkerFanoutPhaseAdmit, WorkerFanoutEventAdmitted},
		{WorkerFanoutPhaseFirstFrame, WorkerFanoutEventFirstFrame},
		{WorkerFanoutPhaseCutover, WorkerFanoutEventCutover},
		{WorkerFanoutPhaseVerify, WorkerFanoutEventVerified},
		{WorkerFanoutPhaseRollback, WorkerFanoutEventRolledBack},
		{WorkerFanoutPhaseAbort, WorkerFanoutEventAborted},
		{WorkerFanoutPhaseRetire, WorkerFanoutEventRetired},
	}
	for index, test := range tests {
		command := WorkerFanoutCommand{
			Schema: WorkerFanoutCommandSchema, TransactionID: "route-phase",
			Sequence: uint64(index + 1), SourceID: "front", CapturePID: 12,
			MediaEpoch: 34, Phase: test.phase,
		}
		if test.phase == WorkerFanoutPhasePrepare {
			command.Changes = []WorkerBranchChange{{Role: "live", Action: WorkerFanoutActionDetach}}
		}
		receipt := WorkerFanoutReceipt{
			Schema: WorkerFanoutReceiptSchema, TransactionID: command.TransactionID,
			Sequence: command.Sequence, SourceID: command.SourceID,
			CapturePID: command.CapturePID, MediaEpoch: command.MediaEpoch,
			Event: test.event,
		}
		if test.event == WorkerFanoutEventFirstFrame {
			receipt.Branches = []WorkerFanoutBranchReceipt{{
				Role: "analysis", BranchEpoch: 1, FrameID: 1,
				CodecBootstrap: WorkerFanoutBootstrapNotApplicable,
			}}
		}
		if err := MatchWorkerFanoutReceipt(command, receipt); err != nil {
			t.Fatalf("phase %s did not accept event %s: %v", test.phase, test.event, err)
		}
		wrong := receipt
		wrong.Event = tests[(index+1)%len(tests)].event
		wrong.Branches = nil
		if wrong.Event == WorkerFanoutEventFirstFrame {
			wrong.Branches = []WorkerFanoutBranchReceipt{{
				Role: "analysis", BranchEpoch: 1, FrameID: 1,
				CodecBootstrap: WorkerFanoutBootstrapNotApplicable,
			}}
		}
		if err := MatchWorkerFanoutReceipt(command, wrong); err == nil {
			t.Fatalf("phase %s accepted foreign event %s", test.phase, wrong.Event)
		}
	}
}

func TestWorkerFanoutCommandRejectsAliasesOrderingAndUnboundedInput(t *testing.T) {
	valid := WorkerFanoutCommand{
		Schema: WorkerFanoutCommandSchema, TransactionID: "route-3", Sequence: 1,
		SourceID: "front", CapturePID: 2, MediaEpoch: 3,
		Phase: WorkerFanoutPhasePrepare,
		Changes: []WorkerBranchChange{{
			Role: "live", Action: WorkerFanoutActionAttach,
			Profile: &WorkerBranchProfile{
				Codec: "h265", Width: 1280, Height: 720, FPS: 20,
				BitrateKbps: 4096, Content: "processed", RateControl: "cbr",
			},
		}},
	}
	for name, mutate := range map[string]func(*WorkerFanoutCommand){
		"codec alias": func(command *WorkerFanoutCommand) { command.Changes[0].Profile.Codec = "hevc" },
		"case alias":  func(command *WorkerFanoutCommand) { command.Changes[0].Profile.Codec = "H265" },
		"raw identity": func(command *WorkerFanoutCommand) {
			command.SourceID = "/dev/video0"
		},
		"zero pid": func(command *WorkerFanoutCommand) { command.CapturePID = 0 },
		"profile on detach": func(command *WorkerFanoutCommand) {
			command.Changes[0].Action = WorkerFanoutActionDetach
		},
		"changes outside prepare": func(command *WorkerFanoutCommand) {
			command.Phase = WorkerFanoutPhaseCutover
		},
	} {
		t.Run(name, func(t *testing.T) {
			candidate := cloneWorkerFanoutCommand(valid)
			mutate(&candidate)
			if _, err := EncodeWorkerFanoutCommand(candidate); err == nil {
				t.Fatal("invalid worker fan-out command was accepted")
			}
		})
	}

	unsorted := cloneWorkerFanoutCommand(valid)
	unsorted.Changes = []WorkerBranchChange{
		{Role: "live", Action: WorkerFanoutActionDetach},
		{Role: "analysis", Action: WorkerFanoutActionDetach},
	}
	if _, err := EncodeWorkerFanoutCommand(unsorted); err == nil || !strings.Contains(err.Error(), "sorted") {
		t.Fatalf("unsorted roles were accepted: %v", err)
	}
	canonical := CanonicalizeWorkerFanoutChanges(unsorted.Changes)
	if canonical[0].Role != "analysis" || canonical[1].Role != "live" || unsorted.Changes[0].Role != "live" {
		t.Fatal("branch canonicalization mutated input or returned the wrong order")
	}

	validPayload, err := EncodeWorkerFanoutCommand(valid)
	if err != nil {
		t.Fatal(err)
	}
	unknown := bytes.Replace(validPayload, []byte(`"phase":"prepare"`),
		[]byte(`"unknown":true,"phase":"prepare"`), 1)
	for name, payload := range map[string][]byte{
		"unknown field":   unknown,
		"concatenated":    append(append([]byte(nil), validPayload...), validPayload...),
		"missing newline": bytes.TrimSuffix(validPayload, []byte{'\n'}),
		"crlf":            append(bytes.TrimSuffix(validPayload, []byte{'\n'}), '\r', '\n'),
		"oversize":        append(bytes.Repeat([]byte{'x'}, WorkerFanoutMaxLineBytes), '\n'),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeWorkerFanoutCommand(payload); err == nil {
				t.Fatal("non-canonical worker envelope was accepted")
			}
		})
	}
}

func TestWorkerFanoutFirstFrameReceiptRoundTrip(t *testing.T) {
	receipt := WorkerFanoutReceipt{
		Schema: WorkerFanoutReceiptSchema, TransactionID: "route-4", Sequence: 5,
		SourceID: "overview", CapturePID: 123, MediaEpoch: 456,
		Event: WorkerFanoutEventFirstFrame,
		Branches: []WorkerFanoutBranchReceipt{
			{
				Role: "analysis", BranchEpoch: 8, FrameID: 100,
				CodecBootstrap: WorkerFanoutBootstrapNotApplicable,
			},
			{
				Role: "live", BranchEpoch: 9, FrameID: 101,
				CodecBootstrap: WorkerFanoutBootstrapComplete,
				RuntimeMode:    WorkerFanoutRuntimePersistentEncoder,
			},
		},
	}
	payload, err := EncodeWorkerFanoutReceipt(receipt)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeWorkerFanoutReceipt(payload)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded, receipt) {
		t.Fatalf("worker receipt round trip differs: got=%+v want=%+v", decoded, receipt)
	}
	command := WorkerFanoutCommand{
		Schema: WorkerFanoutCommandSchema, TransactionID: receipt.TransactionID,
		Sequence: receipt.Sequence, SourceID: receipt.SourceID,
		CapturePID: receipt.CapturePID, MediaEpoch: receipt.MediaEpoch,
		Phase: WorkerFanoutPhaseFirstFrame,
	}
	if err := MatchWorkerFanoutReceipt(command, receipt); err != nil {
		t.Fatalf("exact first-frame receipt did not satisfy its phase fence: %v", err)
	}
	prepared := []WorkerBranchChange{
		{Role: "analysis", Action: WorkerFanoutActionAttach, Profile: &WorkerBranchProfile{
			PixelFormat: "nv12", Width: 640, Height: 360, FPS: 20,
		}},
		{Role: "live", Action: WorkerFanoutActionReplace, Profile: &WorkerBranchProfile{
			Codec: "h265", Width: 1280, Height: 720, FPS: 20,
			BitrateKbps: 4096, Content: "processed", RateControl: "cbr",
		}},
	}
	if err := ValidateWorkerFanoutFirstFrameEvidence(receipt, prepared); err != nil {
		t.Fatalf("candidate branch evidence was rejected: %v", err)
	}
	for name, mutate := range map[string]func(*WorkerFanoutReceipt){
		"transaction": func(candidate *WorkerFanoutReceipt) { candidate.TransactionID = "route-other" },
		"sequence":    func(candidate *WorkerFanoutReceipt) { candidate.Sequence++ },
		"source":      func(candidate *WorkerFanoutReceipt) { candidate.SourceID = "other" },
		"pid":         func(candidate *WorkerFanoutReceipt) { candidate.CapturePID++ },
		"epoch":       func(candidate *WorkerFanoutReceipt) { candidate.MediaEpoch++ },
	} {
		t.Run("fence_"+name, func(t *testing.T) {
			candidate := cloneWorkerFanoutReceipt(receipt)
			mutate(&candidate)
			if err := MatchWorkerFanoutReceipt(command, candidate); err == nil {
				t.Fatal("foreign worker receipt satisfied the command fence")
			}
		})
	}
	wrongPhase := command
	wrongPhase.Phase = WorkerFanoutPhaseCutover
	if err := MatchWorkerFanoutReceipt(wrongPhase, receipt); err == nil ||
		!strings.Contains(err.Error(), "cannot satisfy") {
		t.Fatalf("first-frame receipt satisfied cutover phase: %v", err)
	}
}

func TestWorkerFanoutFirstFrameEvidenceAllowsDetachOnlyAndRequiresCandidates(t *testing.T) {
	receipt := WorkerFanoutReceipt{
		Schema: WorkerFanoutReceiptSchema, TransactionID: "route-detach", Sequence: 8,
		SourceID: "front", CapturePID: 123, MediaEpoch: 456,
		Event: WorkerFanoutEventFirstFrame,
	}
	detachOnly := []WorkerBranchChange{{Role: "live", Action: WorkerFanoutActionDetach}}
	if err := ValidateWorkerFanoutFirstFrameEvidence(receipt, detachOnly); err != nil {
		t.Fatalf("detach-only source required impossible candidate evidence: %v", err)
	}
	attach := []WorkerBranchChange{{
		Role: "live", Action: WorkerFanoutActionAttach,
		Profile: &WorkerBranchProfile{
			Codec: "h265", Width: 1280, Height: 720, FPS: 20,
			BitrateKbps: 4096, Content: "processed", RateControl: "cbr",
		},
	}}
	if err := ValidateWorkerFanoutFirstFrameEvidence(receipt, attach); err == nil ||
		!strings.Contains(err.Error(), "differs from candidate") {
		t.Fatalf("attach passed without first-frame evidence: %v", err)
	}
	receipt.Branches = []WorkerFanoutBranchReceipt{{
		Role: "live", BranchEpoch: 9, FrameID: 10,
		CodecBootstrap: WorkerFanoutBootstrapComplete,
		RuntimeMode:    WorkerFanoutRuntimePersistentEncoder,
	}}
	if err := ValidateWorkerFanoutFirstFrameEvidence(receipt, attach); err != nil {
		t.Fatalf("exact attach evidence was rejected: %v", err)
	}
}

func TestWorkerFanoutReceiptRejectsFalseReadinessAndFreeTextErrors(t *testing.T) {
	firstFrame := WorkerFanoutReceipt{
		Schema: WorkerFanoutReceiptSchema, TransactionID: "route-5", Sequence: 6,
		SourceID: "front", CapturePID: 123, MediaEpoch: 456,
		Event: WorkerFanoutEventFirstFrame,
		Branches: []WorkerFanoutBranchReceipt{{
			Role: "live", BranchEpoch: 1, FrameID: 2,
			CodecBootstrap: WorkerFanoutBootstrapComplete,
			RuntimeMode:    WorkerFanoutRuntimePersistentEncoder,
		}},
	}
	for name, mutate := range map[string]func(*WorkerFanoutReceipt){
		"zero branch epoch": func(receipt *WorkerFanoutReceipt) { receipt.Branches[0].BranchEpoch = 0 },
		"zero frame":        func(receipt *WorkerFanoutReceipt) { receipt.Branches[0].FrameID = 0 },
		"encoded no bootstrap": func(receipt *WorkerFanoutReceipt) {
			receipt.Branches[0].CodecBootstrap = WorkerFanoutBootstrapNotApplicable
		},
		"missing runtime mode": func(receipt *WorkerFanoutReceipt) {
			receipt.Branches[0].RuntimeMode = ""
		},
		"success with error": func(receipt *WorkerFanoutReceipt) { receipt.ErrorCode = "bad_state" },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := cloneWorkerFanoutReceipt(firstFrame)
			mutate(&candidate)
			if _, err := EncodeWorkerFanoutReceipt(candidate); err == nil {
				t.Fatal("false worker readiness was accepted")
			}
		})
	}

	rejected := WorkerFanoutReceipt{
		Schema: WorkerFanoutReceiptSchema, TransactionID: "route-6", Sequence: 7,
		SourceID: "front", CapturePID: 123, MediaEpoch: 456,
		Event: WorkerFanoutEventRejected, ErrorCode: "capacity_exhausted",
	}
	if _, err := EncodeWorkerFanoutReceipt(rejected); err != nil {
		t.Fatal(err)
	}
	command := WorkerFanoutCommand{
		Schema: WorkerFanoutCommandSchema, TransactionID: rejected.TransactionID,
		Sequence: rejected.Sequence, SourceID: rejected.SourceID,
		CapturePID: rejected.CapturePID, MediaEpoch: rejected.MediaEpoch,
		Phase: WorkerFanoutPhaseWarmup,
	}
	var rejection *WorkerFanoutRejectionError
	if err := MatchWorkerFanoutReceipt(command, rejected); !errors.As(err, &rejection) || rejection.ErrorCode != "capacity_exhausted" {
		t.Fatalf("worker rejection was not returned as a typed failure: %v", err)
	}
	rejected.ErrorCode = "device /dev/video0 failed"
	if _, err := EncodeWorkerFanoutReceipt(rejected); err == nil {
		t.Fatal("free-text worker error was accepted")
	}
	if got := WorkerFanoutSanitizedErrorCode(" Device /dev/video0 failed "); got != "worker_rejected" {
		t.Fatalf("worker error sanitization leaked free text: %q", got)
	}
}

func cloneWorkerFanoutCommand(input WorkerFanoutCommand) WorkerFanoutCommand {
	cloned := input
	cloned.Changes = append([]WorkerBranchChange(nil), input.Changes...)
	for index := range cloned.Changes {
		if input.Changes[index].Profile != nil {
			profile := *input.Changes[index].Profile
			cloned.Changes[index].Profile = &profile
		}
	}
	return cloned
}

func cloneWorkerFanoutReceipt(input WorkerFanoutReceipt) WorkerFanoutReceipt {
	cloned := input
	cloned.Branches = append([]WorkerFanoutBranchReceipt(nil), input.Branches...)
	return cloned
}
