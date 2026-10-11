package ingress_test

import (
	"context"
	"errors"
	wire "github.com/Josepavese/videosluice/framewire"
	"github.com/Josepavese/videosluice/framewire/surfaceport"
	in "github.com/Josepavese/videosluice/frameingress"
	fa "github.com/Josepavese/videosluice/frameaccess"
	fd "github.com/Josepavese/videosluice/framedomain"
	"testing"
	"time"
)

type fakeMappingProvider struct {
	bytes               uint64
	calls               int
	createErr, closeErr error
	partial             bool
}

func (p *fakeMappingProvider) Footprint(wire.Mapping) (uint64, error) { return p.bytes, nil }
func (p *fakeMappingProvider) Create(wire.Mapping) (surfaceport.Allocation, error) {
	p.calls++
	if p.createErr != nil && !p.partial {
		return nil, p.createErr
	}
	return fakeMapping{p.closeErr}, p.createErr
}

type fakeMapping struct{ err error }

func (fakeMapping) Publish(context.Context, []byte) error             { return nil }
func (fakeMapping) SnapshotInto(context.Context, []byte) (int, error) { return 0, nil }
func (m fakeMapping) Close() error                                    { return m.err }
func TestMappingRejectsForeignDomainAndUnadmittedFootprintBeforeCreate(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	m := wire.Mapping{Namespace: f.b.Fence().Namespace, Generation: f.b.Scope().MediaEpoch, Capacity: wire.HeaderSize + 384}
	p := &fakeMappingProvider{bytes: 4096}
	wrong := m
	wrong.Namespace[0] ^= 1
	_, err := in.OwnMapping(ctx, f.b, p, wrong)
	want(t, err, fd.ErrDomainMismatch)
	wrong = m
	wrong.Generation++
	_, err = in.OwnMapping(ctx, f.b, p, wrong)
	want(t, err, fd.ErrDomainMismatch)
	for _, bytes := range []uint64{0, 1<<30 + 1} {
		p.bytes = bytes
		_, err = in.OwnMapping(ctx, f.b, p, m)
		want(t, err, fd.ErrInvalid)
	}
	p.bytes = 1 << 30
	_, err = in.OwnMapping(ctx, f.b, p, m)
	want(t, err, fa.CapacityRejected)
	if p.calls != 0 || f.r.Stats().ReservedResourceBytes != 0 {
		t.Fatal(p.calls, f.r.Stats())
	}
}
func TestMappingCreationAndPhysicalReleaseErrorsRemainDistinct(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	m := wire.Mapping{Namespace: f.b.Fence().Namespace, Generation: f.b.Scope().MediaEpoch, Capacity: wire.HeaderSize + 384}
	p := &fakeMappingProvider{bytes: 4096, createErr: surfaceport.ErrUnavailable}
	_, err := in.OwnMapping(ctx, f.b, p, m)
	want(t, err, surfaceport.ErrUnavailable)
	if f.r.Stats().ReservedResourceBytes != 0 {
		t.Fatal(f.r.Stats())
	}
	p.createErr = nil
	p.closeErr = surfaceport.ErrRelease
	o, err := in.OwnMapping(ctx, f.b, p, m)
	must(t, err)
	o.Close()
	deadline, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	err = o.WaitClosed(deadline)
	want(t, err, surfaceport.ErrRelease)
	var failure *fd.ResourceFailure
	if !errors.As(err, &failure) || failure.Stage != "release_failed" {
		t.Fatal(err)
	}
	if s := f.r.Stats(); s.ReservedResourceBytes != 4096 || s.ResourceFailures != 1 {
		t.Fatal(s)
	}
}

func TestMappingPartialCreationIsNeverPublishedOrUnchargedOnFailedCleanup(t *testing.T) {
	f := setup(t)
	m := wire.Mapping{Namespace: f.b.Fence().Namespace, Generation: f.b.Scope().MediaEpoch, Capacity: wire.HeaderSize + 384}
	p := &fakeMappingProvider{bytes: 4096, partial: true, createErr: surfaceport.ErrUnavailable, closeErr: surfaceport.ErrRelease}
	o, err := in.OwnMapping(context.Background(), f.b, p, m)
	want(t, err, surfaceport.ErrUnavailable)
	if o != nil {
		t.Fatal("partial allocation exposed")
	}
	f.b.Close()
	deadline := time.Now().Add(time.Second)
	for f.r.Stats().ResourceFailures == 0 {
		if time.Now().After(deadline) {
			t.Fatal("missing release failure")
		}
		time.Sleep(time.Millisecond)
	}
	if s := f.r.Stats(); s.ReservedResourceBytes != 4096 || s.LiveResources != 1 || s.LiveGenerations != 1 {
		t.Fatal(s)
	}
}
