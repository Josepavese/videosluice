package broker

import (
	"context"
	"strings"
	"testing"

	videoproviders "github.com/Josepavese/videosluice/internal/source"
)

func TestWorkerFanoutPhaseSetCoordinatesExactOwnersConcurrently(t *testing.T) {
	first, firstReader, stopFirst := workerFanoutControlFixtureFor(t, "camera-a", 62001, 31)
	defer stopFirst()
	second, secondReader, stopSecond := workerFanoutControlFixtureFor(t, "camera-b", 62002, 32)
	defer stopSecond()
	targets := map[string]workerFanoutPhaseTarget{
		"camera-b": {Runtime: second, CapturePID: 62002, MediaEpoch: 32},
		"camera-a": {Runtime: first, CapturePID: 62001, MediaEpoch: 31},
	}
	type phaseOutcome struct {
		receipts map[string]videoproviders.WorkerFanoutReceipt
		err      error
	}
	done := make(chan phaseOutcome, 1)
	go func() {
		receipts, err := sendWorkerFanoutPhaseSet(
			context.Background(), "route-set", videoproviders.WorkerFanoutPhaseWarmup, targets)
		done <- phaseOutcome{receipts: receipts, err: err}
	}()
	firstCommand := readWorkerFanoutCommand(t, firstReader)
	secondCommand := readWorkerFanoutCommand(t, secondReader)
	if firstCommand.SourceID != "camera-a" || firstCommand.CapturePID != 62001 || firstCommand.MediaEpoch != 31 ||
		secondCommand.SourceID != "camera-b" || secondCommand.CapturePID != 62002 || secondCommand.MediaEpoch != 32 ||
		firstCommand.TransactionID != secondCommand.TransactionID ||
		firstCommand.Phase != videoproviders.WorkerFanoutPhaseWarmup ||
		secondCommand.Phase != videoproviders.WorkerFanoutPhaseWarmup {
		t.Fatalf("phase set lost frozen ownership: first=%+v second=%+v", firstCommand, secondCommand)
	}
	acceptWorkerFanoutTestReceipt(t, first, firstCommand, 31, 62001, videoproviders.WorkerFanoutEventWarmed, "")
	acceptWorkerFanoutTestReceipt(t, second, secondCommand, 32, 62002, videoproviders.WorkerFanoutEventWarmed, "")
	outcome := <-done
	if outcome.err != nil || len(outcome.receipts) != 2 {
		t.Fatalf("phase set failed receipts=%+v err=%v", outcome.receipts, outcome.err)
	}
}

func TestWorkerFanoutPhaseSetReturnsDeterministicPartialFailure(t *testing.T) {
	first, firstReader, stopFirst := workerFanoutControlFixtureFor(t, "camera-a", 63001, 41)
	defer stopFirst()
	second, secondReader, stopSecond := workerFanoutControlFixtureFor(t, "camera-b", 63002, 42)
	defer stopSecond()
	targets := map[string]workerFanoutPhaseTarget{
		"camera-a": {Runtime: first, CapturePID: 63001, MediaEpoch: 41},
		"camera-b": {Runtime: second, CapturePID: 63002, MediaEpoch: 42},
	}
	done := make(chan error, 1)
	go func() {
		_, err := sendWorkerFanoutPhaseSet(
			context.Background(), "route-failure", videoproviders.WorkerFanoutPhaseAdmit, targets)
		done <- err
	}()
	firstCommand := readWorkerFanoutCommand(t, firstReader)
	secondCommand := readWorkerFanoutCommand(t, secondReader)
	acceptWorkerFanoutTestReceipt(t, first, firstCommand, 41, 63001, videoproviders.WorkerFanoutEventAdmitted, "")
	acceptWorkerFanoutTestReceipt(t, second, secondCommand, 42, 63002, videoproviders.WorkerFanoutEventRejected, "capacity_changed")
	err := <-done
	if err == nil || !strings.Contains(err.Error(), `source "camera-b" phase "admit"`) ||
		!strings.Contains(err.Error(), "capacity_changed") {
		t.Fatalf("phase-set rejection lost deterministic source context: %v", err)
	}
}

func TestWorkerFanoutPhaseSetPreflightsEveryOwnerBeforeAnyWrite(t *testing.T) {
	first, _, stopFirst := workerFanoutControlFixtureFor(t, "camera-a", 64001, 51)
	defer stopFirst()
	_, err := sendWorkerFanoutPhaseSet(
		context.Background(), "route-preflight", videoproviders.WorkerFanoutPhaseVerify,
		map[string]workerFanoutPhaseTarget{
			"camera-a": {Runtime: first, CapturePID: 64001, MediaEpoch: 51},
			"camera-b": {Runtime: nil, CapturePID: 64002, MediaEpoch: 52},
		},
	)
	if err == nil || !strings.Contains(err.Error(), `source "camera-b" runtime is unavailable`) {
		t.Fatalf("invalid phase set passed preflight: %v", err)
	}
	first.mu.Lock()
	sequence, waiters := first.workerFanoutSequence, len(first.workerFanoutWaiters)
	first.mu.Unlock()
	if sequence != 0 || waiters != 0 {
		t.Fatalf("preflight failure wrote to a valid peer: sequence=%d waiters=%d", sequence, waiters)
	}
}

func TestWorkerFanoutPhaseSetFirstFrameMatchesCandidateBranchesPerSource(t *testing.T) {
	from, fromReader, stopFrom := workerFanoutControlFixtureFor(t, "camera-a", 65001, 61)
	defer stopFrom()
	to, toReader, stopTo := workerFanoutControlFixtureFor(t, "camera-b", 65002, 62)
	defer stopTo()
	profile := &videoproviders.WorkerBranchProfile{
		Codec: "h265", Width: 1280, Height: 720, FPS: 20,
		BitrateKbps: 4096, Content: "processed", RateControl: "cbr",
	}
	done := make(chan error, 1)
	go func() {
		_, err := sendWorkerFanoutPhaseSet(
			context.Background(), "route-first-frame", videoproviders.WorkerFanoutPhaseFirstFrame,
			map[string]workerFanoutPhaseTarget{
				"camera-a": {
					Runtime: from, CapturePID: 65001, MediaEpoch: 61,
					PreparedChanges: []videoproviders.WorkerBranchChange{{
						Role: "live", Action: videoproviders.WorkerFanoutActionDetach,
					}},
				},
				"camera-b": {
					Runtime: to, CapturePID: 65002, MediaEpoch: 62,
					PreparedChanges: []videoproviders.WorkerBranchChange{{
						Role: "live", Action: videoproviders.WorkerFanoutActionAttach, Profile: profile,
					}},
				},
			},
		)
		done <- err
	}()
	fromCommand := readWorkerFanoutCommand(t, fromReader)
	toCommand := readWorkerFanoutCommand(t, toReader)
	acceptWorkerFanoutTestReceipt(t, from, fromCommand, 61, 65001, videoproviders.WorkerFanoutEventFirstFrame, "")
	receipt := workerFanoutReceiptFor(toCommand, videoproviders.WorkerFanoutEventFirstFrame)
	receipt.Branches = []videoproviders.WorkerFanoutBranchReceipt{{
		Role: "live", BranchEpoch: 1, FrameID: 1,
		CodecBootstrap: videoproviders.WorkerFanoutBootstrapComplete,
		RuntimeMode:    videoproviders.WorkerFanoutRuntimePersistentEncoder,
	}}
	line, err := videoproviders.EncodeWorkerFanoutReceipt(receipt)
	if err != nil {
		t.Fatal(err)
	}
	if !to.acceptWorkerFanoutReceipt(string(line), 62, 65002) {
		t.Fatal("candidate first-frame receipt was not recognized")
	}
	if err := <-done; err != nil {
		t.Fatalf("source-specific first-frame evidence was rejected: %v", err)
	}
}

func acceptWorkerFanoutTestReceipt(
	t *testing.T,
	runtime *videoBrokerRuntime,
	command videoproviders.WorkerFanoutCommand,
	mediaEpoch uint64,
	childPID int,
	event string,
	errorCode string,
) {
	t.Helper()
	receipt := workerFanoutReceiptFor(command, event)
	receipt.ErrorCode = errorCode
	line, err := videoproviders.EncodeWorkerFanoutReceipt(receipt)
	if err != nil {
		t.Fatal(err)
	}
	if !runtime.acceptWorkerFanoutReceipt(string(line), mediaEpoch, childPID) {
		t.Fatal("worker fan-out receipt was not recognized")
	}
}
