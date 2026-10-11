package frameaccess_test

import (
	"context"
	"testing"

	fa "github.com/Josepavese/videosluice/frameaccess"
	"github.com/Josepavese/videosluice/frameaccess/copyport"
)

func TestFragmentedArenaRejectsBoundedlyAndRecoversAfterResultClose(t *testing.T) {
	c := &fakeClock{}
	c.set(100)
	s := scope("front")
	n, err := s.Geometry.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	l := limits()
	l.MaxFrames, l.MaxBindings, l.MaxResults = 16, 256, 16
	l.PoolBytes, l.MaxFrameBytes, l.MaxArtifactBytes = 18*n, n, 2*n
	l.MaxOutputPerRequest, l.MaxFullFrameBytes = n, n
	st, err := fa.NewStore(s, l, c, copyport.CPU{}, admission(s), fa.CopyPolicy{AllowSoftware: true}, portableIntegrity())
	if err != nil {
		t.Fatal(err)
	}
	var d fa.DeliveryRef
	for seq := uint64(1); seq <= 16; seq++ {
		d = publish(t, st, c, s, seq)
	}
	q := request(s, d, c)
	q.Regions = []fa.Region{{ID: "small", BBox: [4]float64{0, 0, 2, 2}}}
	small, err := st.Extract(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	q.ID, q.Regions = requestID(2), []fa.Region{{ID: "full", FullFrame: true}}
	full, err := st.Extract(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	small.Close()
	before := st.Stats()
	if before.PoolBytes-before.UsedBytes != n || before.ArtifactBytes != n || before.OpenResults != 1 {
		t.Fatalf("unexpected bounded arena setup: %+v", before)
	}
	q.ID = requestID(3)
	second, err := st.Extract(context.Background(), q)
	if second != nil {
		second.Close()
		t.Fatal("first-fit arena unexpectedly compacted immutable allocations")
	}
	requireCode(t, err, fa.CapacityRejected)
	after := st.Stats()
	if after.UsedBytes != before.UsedBytes || after.ArtifactBytes != n || after.OpenResults != 1 {
		t.Fatalf("rejected allocation leaked or exceeded bounds: before=%+v after=%+v", before, after)
	}
	// The producer still progresses while a client holds its detached result.
	d = publish(t, st, c, s, 17)
	full.Close()
	q.ID, q.Frame = requestID(4), d
	recovered, err := st.Extract(context.Background(), q)
	if err != nil {
		t.Fatalf("capacity did not recover after client release: %v", err)
	}
	recovered.Close()
	if st.Stats().OpenResults != 0 || st.Stats().ArtifactBytes != 0 {
		t.Fatal("result release leaked", st.Stats())
	}
}
