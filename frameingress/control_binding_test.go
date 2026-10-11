package ingress_test

import (
	"context"
	fa "github.com/Josepavese/videosluice/frameaccess"
	"github.com/Josepavese/videosluice/frameaccess/copyport"
	fd "github.com/Josepavese/videosluice/framedomain"
	"testing"
)

func TestControlBindingReferenceNeverFollowsSourceRestart(t *testing.T) {
	f, control, _, _ := controlFixture(t, 3)
	old := f.b.Reference()
	b, err := f.r.ResolveBinding(old)
	must(t, err)
	if b != f.b {
		t.Fatal("wrong binding resolved")
	}
	foreign := old
	foreign.Fence.Namespace[0] ^= 1
	_, err = f.r.ResolveBinding(foreign)
	want(t, err, fd.ErrDomainMismatch)
	missing := old
	missing.Source = "other-camera"
	_, err = f.r.ResolveBinding(missing)
	want(t, err, fd.ErrRetired)
	f.b.Close()
	current, err := f.r.Open(context.Background(), f.layout, fa.Limits{MaxFrames: 2, MaxBindings: 8, PoolBytes: 8192, MaxFrameBytes: 384, MaxArtifactBytes: 4096, MaxOutputPerRequest: 1024, MaxFullFrameBytes: 384, MaxRegions: 4, MaxResults: 4, MaxAgeNS: 1000, MaxCopyNS: 100}, copyport.CPU{}, admission{}, fa.CopyPolicy{AllowSoftware: true}, portableIntegrity())
	must(t, err)
	defer current.Close()
	if current.Reference().Epoch <= old.Epoch {
		t.Fatal("source generation reused")
	}
	_, err = f.r.ResolveBinding(old)
	want(t, err, fd.ErrRetired)
	one := consumer()
	r, err := control.Grant(controllerPrincipal(), command(f, 1, 0), old, one, controlPolicy(one.generation))
	outcome(t, r, err, "invalid")
	if r.Stage != "control_binding_reference" || one.closed.Load() != 0 {
		t.Fatal("stale command adopted capability", r)
	}
	must(t, one.Close())
	two := consumer()
	r, err = control.Grant(controllerPrincipal(), command(f, 2, 0), current.Reference(), two, controlPolicy(two.generation))
	outcome(t, r, err, "installed")
}
