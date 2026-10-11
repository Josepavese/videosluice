package ingress_test

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	in "github.com/Josepavese/videosluice/frameingress"
	peer "github.com/Josepavese/videosluice/framepeer"
	wire "github.com/Josepavese/videosluice/framewire"
)

func TestSourceReceiverFailureExplainsOperationWithoutChangingCause(t *testing.T) {
	for _, mode := range []string{"header", "partial", "integrity"} {
		t.Run(mode, func(t *testing.T) {
			f := setup(t)
			packet := f.surface(t, 1)
			s := sourceConnection(t, packet)
			phase, stage, cause := "header", "source_receive", peer.ErrDeadline
			causeName := "deadline"
			records, received := 0, 0
			s.hook = func(_ context.Context, index int) error {
				if index == 0 {
					return &peer.Failure{Stage: "fixture_receive", Cause: errors.Join(cause, errors.New("not-for-logs-payload"))}
				}
				return nil
			}
			if mode == "partial" {
				causeName = "unavailable"
				phase, cause, records, received = "payload", peer.ErrUnavailable, 2, 64
				s.hook = func(_ context.Context, index int) error {
					if index == 2 {
						return &peer.Failure{Stage: "fixture_receive", Cause: errors.Join(cause, errors.New("not-for-logs-payload"))}
					}
					return nil
				}
			} else if mode == "integrity" {
				causeName = "unclassified"
				phase, stage, cause = "integrity", "source_integrity", wire.ErrInvalid
				records, received = len(s.records), len(packet)-wire.HeaderSize
				s.hook = nil
				last := s.records[len(s.records)-1]
				last[len(last)-1] ^= 1
			}
			r := receiveSource(t, f, s)
			err := r.Receive(context.Background(), nil)
			if !errors.Is(err, cause) {
				t.Fatalf("original cause was lost: %v", err)
			}
			var original *in.Failure
			if !errors.As(err, &original) || original.Stage != stage {
				t.Fatalf("original failure stage was lost: %v", err)
			}
			if strings.Contains(err.Error(), "not-for-logs-payload") {
				t.Fatal("diagnosis exposed the provider's private cause")
			}
			for _, text := range []string{
				"source_receive_progress phase=" + phase,
				fmt.Sprintf("accepted_records=%d ", records),
				fmt.Sprintf("assembled_payload_bytes=%d ", received),
				"accepted_wire_bytes=", "declared_payload_bytes=", "elapsed_us=", "header_us=",
				"assembly_us=", "integrity_us=", "publication_us=", "budget_us=",
				"record_kind=", "record_source_epoch=", "record_origin_sequence=",
				"peer_cause=" + causeName, "peer_no_record=false", "context_canceled=false",
			} {
				if !strings.Contains(err.Error(), text) {
					t.Fatalf("missing diagnostic %q in %v", text, err)
				}
			}
			reads := s.reads.Load()
			want(t, r.Receive(context.Background(), nil), peer.ErrRetired)
			if s.reads.Load() != reads || r.Stats().Failures != 1 {
				t.Fatal("diagnosis changed retirement or retried partial transfer")
			}
		})
	}
}

func TestSourceReceiverFailureProgressExcludesPriorFrames(t *testing.T) {
	f := setup(t)
	s := sourceConnection(t, f.surface(t, 1))
	firstRecords := len(s.records)
	second := sourceConnection(t, f.surface(t, 2))
	s.records = append(s.records, second.records...)
	s.hook = func(_ context.Context, index int) error {
		if index == firstRecords+2 {
			return peer.ErrDeadline
		}
		return nil
	}
	r := receiveSource(t, f, s)
	must(t, r.Receive(context.Background(), nil))
	err := r.Receive(context.Background(), nil)
	for _, text := range []string{"accepted_records=2 ", "assembled_payload_bytes=64 ", "record_origin_sequence=2 "} {
		if err == nil || !strings.Contains(err.Error(), text) {
			t.Fatalf("previous frame leaked into failure progress: %v (want %s)", err, text)
		}
	}
	if !errors.Is(err, peer.ErrDeadline) || r.Stats().Frames != 1 || r.Stats().Failures != 1 {
		t.Fatal("diagnosis changed successful frame or failure semantics", err, r.Stats())
	}
}

func TestSourceReceiverLateConsumerCannotBecomeSuccessfulPublication(t *testing.T) {
	for _, ownFailure := range []bool{false, true} {
		t.Run(fmt.Sprintf("consumer_error=%t", ownFailure), func(t *testing.T) {
			f := setup(t)
			must(t, f.i.AcceptSurface(context.Background(), f.surface(t, 1), 100, 200))
			s := sourceConnection(t, f.output(t, 1, 1))
			r, err := in.NewSourceReceiver(context.Background(), f.i, s, "source.worker", f.c,
				uint64(50*time.Millisecond), 100, sha256.Sum256, 0)
			must(t, err)
			defer r.Close()
			consumerFailure := errors.New("consumer failed after its deadline")
			calls := 0
			err = r.Receive(context.Background(), func(ctx context.Context, _ wire.Header, _ []byte) error {
				calls++
				<-ctx.Done()
				if ownFailure {
					return consumerFailure
				}
				return nil
			})
			cause := error(context.DeadlineExceeded)
			stage := "output_consumer_deadline"
			if ownFailure {
				cause, stage = consumerFailure, "output_consumer"
			}
			want(t, err, cause)
			var failure *in.Failure
			if !errors.As(err, &failure) || failure.Stage != stage ||
				!strings.Contains(err.Error(), "phase=publication ") {
				t.Fatalf("late consumer lost its phase or original cause: %v", err)
			}
			if calls != 1 || r.Stats().Outputs != 0 || r.Stats().Failures != 1 || r.Stats().Drops != 0 {
				t.Fatal("late publication counted as success/drop or retried", calls, r.Stats())
			}
			reads := s.reads.Load()
			want(t, r.Receive(context.Background(), nil), peer.ErrRetired)
			if reads != s.reads.Load() {
				t.Fatal("retired consumer retried a possibly published output")
			}
		})
	}
}
