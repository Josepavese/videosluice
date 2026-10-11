package framedomain_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	fd "github.com/Josepavese/videosluice/framedomain"
	"github.com/Josepavese/videosluice/framedomain/monotime"
)

func TestResourceBorrowFailureIdentifiesConsumerWithoutLosingCause(t *testing.T) {
	r := bufferRuntime(t, runtimeLimits())
	defer r.Revoke()
	var released atomic.Int32
	o, err := fd.OwnRuntimeResource(context.Background(), r, intPlan(16, &released))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { o.Close(); awaitResource(t, o) }()
	cause := errors.New("bounded callback checkpoint")
	err = o.Borrow(context.Background(), func(context.Context, int) error { return cause })
	if !errors.Is(err, cause) {
		t.Fatalf("callback cause lost: %v", err)
	}
	for _, field := range []string{"resource_borrow_progress phase=consumer", "begin_us=", "admission_us=", "consumer_us=", "current_us=", "finish_us=", "elapsed_us="} {
		if !strings.Contains(err.Error(), field) {
			t.Fatalf("missing bounded diagnostic %q: %v", field, err)
		}
	}
}

func TestResourceBorrowRetirementRetainsCallbackCheckpoint(t *testing.T) {
	r := bufferRuntime(t, runtimeLimits())
	defer r.Revoke()
	var released atomic.Int32
	o, err := fd.OwnRuntimeResource(context.Background(), r, intPlan(16, &released))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { o.Close(); awaitResource(t, o) }()
	cause := errors.New("bounded pre-retirement checkpoint")
	err = o.Borrow(context.Background(), func(context.Context, int) error {
		o.Close()
		return cause
	})
	if !errors.Is(err, fd.ErrRetired) || !errors.Is(err, cause) {
		t.Fatalf("retirement erased callback checkpoint: %v", err)
	}
	awaitResource(t, o)
	if released.Load() != 1 || r.Stats().LiveResources != 0 {
		t.Fatal("retirement changed physical ownership accounting", r.Stats())
	}
}

func TestResourceBorrowRevokedProcessRetainsCallbackCheckpoint(t *testing.T) {
	a := auth()
	r, err := fd.New(a, testEntropy{}, monotime.New(), runtimeLimits())
	if err != nil {
		t.Fatal(err)
	}
	defer r.Revoke()
	var released atomic.Int32
	o, err := fd.OwnRuntimeResource(context.Background(), r, intPlan(16, &released))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { o.Close(); awaitResource(t, o) }()
	cause := errors.New("bounded pre-revocation checkpoint")
	err = o.Borrow(context.Background(), func(context.Context, int) error {
		a.value.Store("unavailable")
		return cause
	})
	if !errors.Is(err, fd.ErrProcessChanged) || !errors.Is(err, cause) {
		t.Fatalf("process fence erased callback checkpoint: %v", err)
	}
}

func TestResourceBorrowPreConsumerFailureIsNotPixelWork(t *testing.T) {
	for _, phase := range []string{"begin", "admission"} {
		t.Run(phase, func(t *testing.T) {
			a := auth()
			r, err := fd.New(a, testEntropy{}, monotime.New(), runtimeLimits())
			if err != nil {
				t.Fatal(err)
			}
			defer r.Revoke()
			var released atomic.Int32
			o, err := fd.OwnRuntimeResource(context.Background(), r, intPlan(16, &released))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { o.Close(); awaitResource(t, o) }()
			want := fd.ErrProcessChanged
			if phase == "begin" {
				a.value.Store("unavailable")
			} else {
				o.Close()
				awaitResource(t, o)
				want = fd.ErrRetired
			}
			err = o.Borrow(context.Background(), func(context.Context, int) error {
				t.Fatal("failed authority/admission reached consumer")
				return nil
			})
			if !errors.Is(err, want) {
				t.Fatalf("failed admission no longer fences: %v", err)
			}
			for _, field := range []string{"resource_borrow_progress phase=" + phase, "consumer_us=-1"} {
				if !strings.Contains(err.Error(), field) {
					t.Fatalf("pre-consumer failure misattributed %q: %v", field, err)
				}
			}
		})
	}
}

func BenchmarkResourceBorrow(b *testing.B) {
	r, err := fd.New(auth(), testEntropy{}, monotime.New(), runtimeLimits())
	if err != nil {
		b.Fatal(err)
	}
	var released atomic.Int32
	ctx := context.Background()
	o, err := fd.OwnRuntimeResource(ctx, r, intPlan(16, &released))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { o.Close(); awaitResource(b, o); r.Revoke() })
	visit := func(context.Context, int) error { return nil }
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := o.Borrow(ctx, visit); err != nil {
			b.Fatal(err)
		}
	}
}
