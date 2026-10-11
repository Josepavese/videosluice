package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"io"
	"strings"
	"testing"
	"time"
)

func launchArgs() []string {
	return []string{"--frame-launch-socket-fd", "4", "--frame-launch-controller-fd", "5", "--frame-launch-controller-metadata", "ab01"}
}

func TestFrameBootstrapArgumentsAreExactAndOwned(t *testing.T) {
	flags := flag.NewFlagSet("fixture", flag.ContinueOnError)
	options := registerFrameBootstrapFlags(flags)
	if err := flags.Parse(launchArgs()); err != nil {
		t.Fatal(err)
	}
	input, requested, err := options.validate()
	if err != nil || !requested || input.SocketFD != 4 || input.ControllerFD != 5 || !bytes.Equal(input.ControllerMetadata, []byte{0xab, 1}) {
		t.Fatal(input, requested, err)
	}
	input.ControllerMetadata[0] = 0
	again, _, err := options.validate()
	if err != nil || again.ControllerMetadata[0] != 0xab {
		t.Fatal("metadata aliased mutable caller storage")
	}
}

func TestDaemonRejectsBadBootstrapBeforeOpeningProvider(t *testing.T) {
	cases := [][]string{
		{"--frame-launch-socket-fd", "4"},
		{"--frame-launch-socket-fd", "4", "--frame-launch-controller-fd", "4", "--frame-launch-controller-metadata", "ab"},
		append(launchArgs(), "--frame-launch-socket-fd", "6"),
		append(launchArgs(), "--check-config"),
		append(launchArgs(), "--discover-capabilities"),
		append(launchArgs(), "unexpected-positional"),
		append(launchArgs(), "--unknown-option"),
	}
	for _, raw := range []string{"0", "2", "04", "+4", "-4", " 4", "4 ", "1000000", ""} {
		args := launchArgs()
		args[1] = raw
		cases = append(cases, args)
	}
	for _, raw := range []string{"", "a", "AB", "zz", strings.Repeat("a", 514)} {
		args := launchArgs()
		args[5] = raw
		cases = append(cases, args)
	}
	for i, args := range cases {
		calls := 0
		err := runDaemon(context.Background(), args, io.Discard, func(context.Context, frameLaunchInput) (frameBootstrapOwner, error) {
			calls++
			return nil, nil
		})
		if err == nil || calls != 0 {
			t.Fatalf("case %d admitted invalid arguments: calls=%d err=%v", i, calls, err)
		}
	}
}

type bootstrapClose func() error

func (close bootstrapClose) Close() error            { return close() }
func (bootstrapClose) Run(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }

func TestDaemonRetainsPartialBootstrapOwnerAndCancelsBeforeClose(t *testing.T) {
	for _, mode := range []string{"partial", "config_failure", "missing_owner"} {
		t.Run(mode, func(t *testing.T) {
			calls, closes := 0, 0
			startErr, closeErr := errors.New("create phase failed"), errors.New("release unconfirmed")
			err := runDaemon(context.Background(), launchArgs(), io.Discard, func(ctx context.Context, input frameLaunchInput) (frameBootstrapOwner, error) {
				calls++
				if mode == "missing_owner" {
					return nil, nil
				}
				owner := bootstrapClose(func() error {
					closes++
					if ctx.Err() == nil {
						t.Error("bootstrap closed before cancellation")
					}
					return closeErr
				})
				if mode == "partial" {
					return owner, startErr
				}
				return owner, nil
			})
			if err == nil || calls != 1 {
				t.Fatal(calls, closes, err)
			}
			if mode == "missing_owner" {
				if closes != 0 {
					t.Fatal(closes)
				}
				return
			}
			if closes != 1 || !errors.Is(err, closeErr) {
				t.Fatal("cleanup failure lost", closes, err)
			}
			if mode == "partial" && !errors.Is(err, startErr) {
				t.Fatal("creation error lost", err)
			}
			if mode == "config_failure" && !strings.Contains(err.Error(), "load configuration") {
				t.Fatal("configuration failure lost", err)
			}
		})
	}
}

func TestDaemonUnqualifiedProviderNeverFallsBackToNonFAPStart(t *testing.T) {
	err := runDaemon(context.Background(), launchArgs(), io.Discard, func(context.Context, frameLaunchInput) (frameBootstrapOwner, error) {
		return nil, errFrameProviderUnavailable
	})
	if !errors.Is(err, errFrameProviderUnavailable) || strings.Contains(err.Error(), "load configuration") {
		t.Fatal(err)
	}
	// Without inherited bootstrap, the existing standalone input contract applies.
	err = runDaemon(context.Background(), nil, io.Discard, nil)
	if err == nil || !strings.Contains(err.Error(), "load configuration") {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = runDaemon(ctx, launchArgs(), io.Discard, func(context.Context, frameLaunchInput) (frameBootstrapOwner, error) {
		t.Fatal("cancelled startup opened provider")
		return nil, nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestDaemonWaitCancelsAndJoinsServiceOnAPIFailure(t *testing.T) {
	for _, mode := range []string{"status", "routing", "runtime", "normal"} {
		t.Run(mode, func(t *testing.T) {
			ctx, stop := context.WithCancel(context.Background())
			defer stop()
			wait := make(chan error, 1)
			var status, routing chan error
			workerErr, apiErr := errors.New("worker failure"), errors.New("api failure")
			joined := make(chan struct{})
			if mode == "runtime" {
				close(joined)
				wait <- workerErr
			} else {
				go func() {
					<-ctx.Done()
					close(joined)
					if mode == "normal" {
						wait <- nil
					} else {
						wait <- workerErr
					}
				}()
				if mode == "routing" {
					routing = make(chan error, 1)
					routing <- apiErr
				} else if mode == "normal" {
					status = make(chan error, 1)
					status <- nil
				} else {
					status = make(chan error, 1)
					status <- apiErr
				}
			}
			done := make(chan error, 1)
			go func() { done <- waitDaemon(stop, wait, status, routing, nil) }()
			select {
			case err := <-done:
				if ctx.Err() == nil {
					t.Fatal("daemon did not cancel")
				}
				select {
				case <-joined:
				default:
					t.Fatal("returned before service joined")
				}
				if mode == "normal" {
					if err != nil {
						t.Fatal(err)
					}
					return
				}
				if !errors.Is(err, workerErr) {
					t.Fatal("worker error lost", err)
				}
				if mode != "runtime" && !errors.Is(err, apiErr) {
					t.Fatal("api error lost", err)
				}
			case <-time.After(time.Second):
				t.Fatal("daemon waited twice or failed to cancel")
			}
		})
	}
}

func TestDaemonWaitJoinsBothAPIsBeforeReturning(t *testing.T) {
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	wait, status, routing := make(chan error, 1), make(chan error, 1), make(chan error, 1)
	wait <- nil
	statusCancelled, routingCancelled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	go func() { <-ctx.Done(); close(statusCancelled); <-release; status <- nil }()
	go func() { <-ctx.Done(); close(routingCancelled); <-release; routing <- nil }()
	done := make(chan error, 1)
	go func() { done <- waitDaemon(stop, wait, status, routing, nil) }()
	for _, entered := range []<-chan struct{}{statusCancelled, routingCancelled} {
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("API not cancelled")
		}
	}
	select {
	case <-done:
		t.Fatal("returned before API teardown")
	default:
	}
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("API teardown not joined")
	}
}

func TestDaemonFrameLoopFailureStopsAndJoinsOtherServices(t *testing.T) {
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	worker, status, routing, frame := make(chan error, 1), make(chan error, 1), make(chan error, 1), make(chan error, 1)
	failure := errors.New("frame fixture failed")
	frame <- failure
	for _, channel := range []chan error{worker, status, routing} {
		go func() { <-ctx.Done(); channel <- context.Canceled }()
	}
	done := make(chan error, 1)
	go func() { done <- waitDaemon(stop, worker, status, routing, frame) }()
	select {
	case err := <-done:
		if !errors.Is(err, failure) || ctx.Err() == nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("frame failure did not drain daemon")
	}
}

func TestDaemonWaitRetainsBootstrapUntilFrameLoopJoins(t *testing.T) {
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	worker, frame := make(chan error, 1), make(chan error, 1)
	worker <- nil
	entered, release := make(chan struct{}), make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	go func() { <-ctx.Done(); close(entered); <-release; frame <- context.Canceled }()
	done := make(chan error, 1)
	go func() { done <- waitDaemon(stop, worker, nil, nil, frame) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("frame loop was not canceled")
	}
	select {
	case <-done:
		t.Fatal("bootstrap may close before frame loop")
	default:
	}
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("frame loop join stuck")
	}
}
