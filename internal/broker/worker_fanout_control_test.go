package broker

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"testing"
	"time"

	videoproviders "github.com/Josepavese/videosluice/internal/source"
)

func TestWorkerFanoutRequiresExplicitWorkerAttestation(t *testing.T) {
	runtime := &videoBrokerRuntime{
		plan:         videoBrokerPlan{SourceID: "front"},
		childCmd:     &exec.Cmd{Process: &os.Process{Pid: 61001}},
		childRunning: true, mediaEpoch: 17,
	}
	_, err := runtime.sendWorkerFanoutPhase(
		context.Background(), "route-unsupported", "front", 61001, 17,
		videoproviders.WorkerFanoutPhaseWarmup, nil,
	)
	if err == nil || err.Error() != "capture worker does not attest dynamic fan-out control" {
		t.Fatalf("unsupported worker was not rejected fail-closed: %v", err)
	}
}

func TestWorkerFanoutRejectsAChangedFrozenCaptureOwnerBeforeWrite(t *testing.T) {
	runtime, _, stop := workerFanoutControlFixture(t)
	defer stop()
	_, err := runtime.sendWorkerFanoutPhase(
		context.Background(), "route-owner", "front", 61002, 17,
		videoproviders.WorkerFanoutPhaseWarmup, nil,
	)
	if err == nil || err.Error() != "worker fan-out capture owner differs from frozen transaction" {
		t.Fatalf("changed capture owner was not rejected: %v", err)
	}
	runtime.mu.Lock()
	sequence := runtime.workerFanoutSequence
	runtime.mu.Unlock()
	if sequence != 0 {
		t.Fatalf("rejected owner consumed worker sequence=%d", sequence)
	}
}

func TestWorkerFanoutPhaseRoundTripIsBoundToOwnedChild(t *testing.T) {
	runtime, reader, stop := workerFanoutControlFixture(t)
	defer stop()

	result := make(chan workerFanoutReceiptResult, 1)
	go func() {
		receipt, err := runtime.sendWorkerFanoutPhase(
			context.Background(), "route-1", "front", 61001, 17, videoproviders.WorkerFanoutPhasePrepare,
			[]videoproviders.WorkerBranchChange{{
				Role: "live", Action: videoproviders.WorkerFanoutActionAttach,
				Profile: &videoproviders.WorkerBranchProfile{
					Codec: "h265", Width: 1280, Height: 720, FPS: 20,
					BitrateKbps: 4096, Content: "processed", RateControl: "cbr",
				},
			}},
		)
		result <- workerFanoutReceiptResult{receipt: receipt, err: err}
	}()

	command := readWorkerFanoutCommand(t, reader)
	if command.SourceID != "front" || command.CapturePID != 61001 || command.MediaEpoch != 17 ||
		command.Sequence == 0 || command.Phase != videoproviders.WorkerFanoutPhasePrepare {
		t.Fatalf("command lost runtime-owned fence: %+v", command)
	}
	receipt := workerFanoutReceiptFor(command, videoproviders.WorkerFanoutEventPrepared)
	line, err := videoproviders.EncodeWorkerFanoutReceipt(receipt)
	if err != nil {
		t.Fatal(err)
	}
	if !runtime.acceptWorkerFanoutReceipt(string(line), 17, 61001) {
		t.Fatal("worker receipt line was not recognized")
	}
	select {
	case got := <-result:
		if got.err != nil || got.receipt.Sequence != command.Sequence {
			t.Fatalf("round trip failed receipt=%+v err=%v", got.receipt, got.err)
		}
	case <-time.After(time.Second):
		t.Fatal("worker fan-out phase did not complete")
	}
}

func TestWorkerFanoutIgnoresReceiptFromWrongProcessWithoutConsumingWaiter(t *testing.T) {
	runtime, reader, stop := workerFanoutControlFixture(t)
	defer stop()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := runtime.sendWorkerFanoutPhase(
			ctx, "route-2", "front", 61001, 17, videoproviders.WorkerFanoutPhaseWarmup, nil)
		result <- err
	}()
	command := readWorkerFanoutCommand(t, reader)
	stale := workerFanoutReceiptFor(command, videoproviders.WorkerFanoutEventWarmed)
	stale.CapturePID++
	line, err := videoproviders.EncodeWorkerFanoutReceipt(stale)
	if err != nil {
		t.Fatal(err)
	}
	runtime.acceptWorkerFanoutReceipt(string(line), 17, 61001)

	current := workerFanoutReceiptFor(command, videoproviders.WorkerFanoutEventWarmed)
	line, err = videoproviders.EncodeWorkerFanoutReceipt(current)
	if err != nil {
		t.Fatal(err)
	}
	runtime.acceptWorkerFanoutReceipt(string(line), 17, 61001)
	if err := <-result; err != nil {
		t.Fatalf("stale receipt consumed the current waiter: %v", err)
	}
}

func TestWorkerFanoutCancellationRemovesPendingFence(t *testing.T) {
	runtime, reader, stop := workerFanoutControlFixture(t)
	defer stop()
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := runtime.sendWorkerFanoutPhase(
			ctx, "route-3", "front", 61001, 17, videoproviders.WorkerFanoutPhaseVerify, nil)
		result <- err
	}()
	_ = readWorkerFanoutCommand(t, reader)
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled phase returned %v", err)
	}
	runtime.mu.Lock()
	pending := len(runtime.workerFanoutWaiters)
	runtime.mu.Unlock()
	if pending != 0 {
		t.Fatalf("canceled phase leaked %d receipt waiters", pending)
	}
}

func TestWorkerFanoutRejectedReceiptReturnsCanonicalError(t *testing.T) {
	runtime, reader, stop := workerFanoutControlFixture(t)
	defer stop()
	result := make(chan error, 1)
	go func() {
		_, err := runtime.sendWorkerFanoutPhase(
			context.Background(), "route-4", "front", 61001, 17, videoproviders.WorkerFanoutPhaseCutover, nil)
		result <- err
	}()
	command := readWorkerFanoutCommand(t, reader)
	receipt := workerFanoutReceiptFor(command, videoproviders.WorkerFanoutEventRejected)
	receipt.ErrorCode = "capacity_changed"
	line, err := videoproviders.EncodeWorkerFanoutReceipt(receipt)
	if err != nil {
		t.Fatal(err)
	}
	runtime.acceptWorkerFanoutReceipt(string(line), 17, 61001)
	var rejection *videoproviders.WorkerFanoutRejectionError
	if err := <-result; !errors.As(err, &rejection) || rejection.ErrorCode != "capacity_changed" {
		t.Fatalf("worker rejection was not preserved canonically: %v", err)
	}
}

func TestWorkerFanoutReceiptAcceptsCanonicalSchemaInAnyJSONFieldOrder(t *testing.T) {
	runtime, reader, stop := workerFanoutControlFixture(t)
	defer stop()
	result := make(chan error, 1)
	go func() {
		_, err := runtime.sendWorkerFanoutPhase(
			context.Background(), "route-order", "front", 61001, 17, videoproviders.WorkerFanoutPhaseVerify, nil)
		result <- err
	}()
	command := readWorkerFanoutCommand(t, reader)
	line := []byte(`{"event":"verified","media_epoch":17,"capture_pid":61001,"source_id":"front","sequence":` +
		fmt.Sprint(command.Sequence) + `,"transaction_id":"route-order","schema":"video-broker.worker-fanout-receipt.v1"}` + "\n")
	if !runtime.acceptWorkerFanoutReceipt(string(line), 17, 61001) {
		t.Fatal("receipt with reordered JSON fields was not recognized")
	}
	if err := <-result; err != nil {
		t.Fatalf("receipt with reordered JSON fields was rejected: %v", err)
	}
}

func TestWorkerFanoutMalformedKnownSchemaIsConsumedWithoutAWaiter(t *testing.T) {
	runtime := &videoBrokerRuntime{}
	line := `{"schema":"video-broker.worker-fanout-receipt.v1","transaction_id":`
	if !runtime.acceptWorkerFanoutReceipt(line, 17, 61001) {
		t.Fatal("malformed line carrying the worker receipt schema was not consumed")
	}
}

func TestWorkerFanoutWorkerExitUnblocksPendingPhase(t *testing.T) {
	runtime, reader, stop := workerFanoutControlFixture(t)
	defer stop()
	result := make(chan error, 1)
	go func() {
		_, err := runtime.sendWorkerFanoutPhase(
			context.Background(), "route-exit", "front", 61001, 17, videoproviders.WorkerFanoutPhaseVerify, nil)
		result <- err
	}()
	_ = readWorkerFanoutCommand(t, reader)
	runtime.mu.Lock()
	runtime.failWorkerFanoutWaitersLocked(errors.New("worker exited"))
	runtime.mu.Unlock()
	select {
	case err := <-result:
		if err == nil || err.Error() != "worker exited" {
			t.Fatalf("worker exit returned %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("worker exit did not unblock pending fan-out phase")
	}
}

func workerFanoutControlFixture(t *testing.T) (*videoBrokerRuntime, *bufio.Reader, func()) {
	return workerFanoutControlFixtureFor(t, "front", 61001, 17)
}

func workerFanoutControlFixtureFor(
	t *testing.T,
	sourceID string,
	childPID int,
	mediaEpoch uint64,
) (*videoBrokerRuntime, *bufio.Reader, func()) {
	t.Helper()
	reader, writer := io.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	writes := make(chan workerFanoutWrite)
	runtime := &videoBrokerRuntime{
		plan:         videoBrokerPlan{SourceID: sourceID},
		childCmd:     &exec.Cmd{Process: &os.Process{Pid: childPID}},
		childRunning: true, childFanoutSupported: true, childFanoutControl: writes,
		mediaEpoch: mediaEpoch,
	}
	go runtime.writeBridgeControls(ctx, writer, nil, writes)
	return runtime, bufio.NewReader(reader), func() {
		cancel()
		_ = reader.Close()
	}
}

func readWorkerFanoutCommand(t *testing.T, reader *bufio.Reader) videoproviders.WorkerFanoutCommand {
	t.Helper()
	line, err := reader.ReadBytes('\n')
	if err != nil {
		t.Fatal(err)
	}
	command, err := videoproviders.DecodeWorkerFanoutCommand(line)
	if err != nil {
		t.Fatal(err)
	}
	return command
}

func workerFanoutReceiptFor(
	command videoproviders.WorkerFanoutCommand,
	event string,
) videoproviders.WorkerFanoutReceipt {
	return videoproviders.WorkerFanoutReceipt{
		Schema:        videoproviders.WorkerFanoutReceiptSchema,
		TransactionID: command.TransactionID, Sequence: command.Sequence,
		SourceID: command.SourceID, CapturePID: command.CapturePID,
		MediaEpoch: command.MediaEpoch, Event: event,
	}
}
