package frameaccess_test

import (
	"bytes"
	"context"
	"testing"

	fa "github.com/Josepavese/videosluice/frameaccess"
	"github.com/Josepavese/videosluice/frameaccess/copyport"
)

func TestExactDeliveryIsRetentionIndependent(t *testing.T) {
	st, c, s := fixture(t, copyport.CPU{})
	one, live := delivery(s, "analysis", 1), delivery(s, "live", 1)
	for _, ref := range []fa.DeliveryRef{one, live} {
		if err := st.Bind(origin(s, 7), fa.DeliveryBinding{Ref: ref, Transform: identity(s.Geometry)}); err != nil {
			t.Fatalf("exact output depends on native pixel retention: %v", err)
		}
	}
	a, b := resolve(t, st, one), resolve(t, st, live)
	if got := fa.Correlate(a, b); got != fa.ExactSameOrigin {
		t.Fatal("attested origins were lost without pixels", got)
	}
	_, err := st.Extract(context.Background(), request(s, one, c))
	requireCode(t, err, fa.Expired)
	if got := st.Stats(); got.Frames != 0 || got.UsedBytes != 0 || got.Bindings != 2 {
		t.Fatal("metadata reserved pixel storage", got)
	}
	// A newer surface is not a substitute for this capture.
	if err := st.TryPublishSurface(context.Background(), origin(s, 8), sourcePixels(s.Geometry, 8), c.NowNS(), c.NowNS()+100); err != nil {
		t.Fatal(err)
	}
	_, err = st.Extract(context.Background(), request(s, one, c))
	requireCode(t, err, fa.Expired)
	// Exact, late native completion makes the existing delivery extractable.
	pixels := sourcePixels(s.Geometry, 7)
	if err := st.TryPublishSurface(context.Background(), origin(s, 7), pixels, c.NowNS(), c.NowNS()+100); err != nil {
		t.Fatal(err)
	}
	q := request(s, one, c)
	q.Regions = []fa.Region{{ID: "full", FullFrame: true}}
	result, err := st.Extract(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	if err = result.Visit(0, func(a fa.Artifact, got []byte) error {
		if a.Proof.Origin != origin(s, 7) || a.Proof.Delivery != one || !bytes.Equal(got, pixels) {
			t.Fatal("late surface changed the attested identity or pixels")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	result.Close()
	// Delivery identity is write-once even before/after pixel arrival.
	requireCode(t, st.Bind(origin(s, 8), fa.DeliveryBinding{Ref: one, Transform: identity(s.Geometry)}), fa.InvalidContract)
	c.set(1100)
	if got := fa.Correlate(a, b); got != fa.Expired {
		t.Fatal("metadata survived its unchanged retention budget", got)
	}
}

func TestMissingSurfaceBindingsRemainBounded(t *testing.T) {
	st, c, s := fixture(t, copyport.CPU{})
	for id := uint64(1); id <= 100; id++ {
		if err := st.Bind(origin(s, id), fa.DeliveryBinding{Ref: delivery(s, "analysis", id), Transform: identity(s.Geometry)}); err != nil {
			t.Fatal(err)
		}
		if got := st.Stats(); got.Bindings > limits().MaxBindings || got.UsedBytes != 0 || got.Frames != 0 {
			t.Fatal("missing surfaces grew the bounded ledger", got)
		}
	}
	_, err := st.Resolve(delivery(s, "analysis", 1))
	requireCode(t, err, fa.Expired)
	requireCode(t, st.Bind(origin(s, 1), fa.DeliveryBinding{Ref: delivery(s, "analysis", 1), Transform: identity(s.Geometry)}), fa.InvalidContract)
	c.set(1100)
	if got := st.Stats(); got.Bindings != 0 || got.UsedBytes != 0 {
		t.Fatal("missing-surface metadata did not expire", got)
	}
}

func TestExactMetadataProgressesWhileSurfaceCopyIsBlocked(t *testing.T) {
	copy := blocker()
	st, c, s := fixture(t, copy)
	copy.active.Store(true)
	done := make(chan error, 1)
	go func() {
		done <- st.TryPublishSurface(context.Background(), origin(s, 1), sourcePixels(s.Geometry, 1), c.NowNS(), c.NowNS()+100)
	}()
	wait(t, copy.entered)
	ref := delivery(s, "analysis", 1)
	err := st.Bind(origin(s, 1), fa.DeliveryBinding{Ref: ref, Transform: identity(s.Geometry)})
	resolved, resolveErr := st.Resolve(ref)
	code := fa.Correlate(resolved, resolved)
	close(copy.release)
	if err != nil || resolveErr != nil || code != fa.ExactSameOrigin {
		t.Fatal("optional native copy blocked causal metadata", err, resolveErr, code)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	result, err := st.Extract(context.Background(), request(s, ref, c))
	if err != nil {
		t.Fatal("late exact surface did not become extractable", err)
	}
	result.Close()
}

func TestExpiredPixelCopyCannotUseStillCurrentMetadata(t *testing.T) {
	copy := blocker()
	st, c, s := fixture(t, copy)
	if err := st.TryPublishSurface(context.Background(), origin(s, 1), sourcePixels(s.Geometry, 1), 100, 200); err != nil {
		t.Fatal(err)
	}
	c.set(1050)
	ref := delivery(s, "analysis", 1)
	if err := st.Bind(origin(s, 1), fa.DeliveryBinding{Ref: ref, Transform: identity(s.Geometry)}); err != nil {
		t.Fatal(err)
	}
	resolved := resolve(t, st, ref)
	copy.active.Store(true)
	done := make(chan error, 1)
	go func() {
		result, err := st.Extract(context.Background(), request(s, ref, c))
		if result != nil {
			result.Close()
		}
		done <- err
	}()
	wait(t, copy.entered)
	c.set(1100) // Pixel age expired; request deadline1150 and metadata age remain valid.
	close(copy.release)
	requireCode(t, <-done, fa.Expired)
	if got := fa.Correlate(resolved, resolved); got != fa.ExactSameOrigin {
		t.Fatal("pixel expiration invalidated current causal metadata", got)
	}
	if got := st.Stats(); got.UsedBytes != 0 || got.OpenResults != 0 || got.Bindings != 1 {
		t.Fatal("failed copy leaked memory or erased metadata", got)
	}
}
