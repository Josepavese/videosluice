package frameaccess_test

import (
	"context"
	"crypto/sha256"
	"testing"

	fa "github.com/Josepavese/videosluice/frameaccess"
	"github.com/Josepavese/videosluice/frameaccess/copyport"
)

func stagedFixture(t *testing.T, sum func([]byte) [32]byte) (*fa.Store, *fakeClock, fa.Scope) {
	t.Helper()
	c := &fakeClock{}
	c.set(100)
	s := scope("front")
	st, err := fa.NewStore(s, limits(), c, copyport.CPU{}, admission(s), fa.CopyPolicy{AllowSoftware: true}, fa.PayloadIntegrity{Sum256: sum, WorkspaceBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	return st, c, s
}

func stage(t *testing.T, st *fa.Store, c *fakeClock, s fa.Scope, n uint64, pixels []byte, digest [32]byte) fa.DeliveryRef {
	t.Helper()
	if err := st.TryStageSurface(context.Background(), origin(s, n), pixels, fa.Digest(digest), c.NowNS(), c.NowNS()+100); err != nil {
		t.Fatal(err)
	}
	d := delivery(s, "analysis", n)
	if err := st.Bind(origin(s, n), fa.DeliveryBinding{Ref: d, Transform: identity(s.Geometry)}); err != nil {
		t.Fatal(err)
	}
	return d
}

func TestStagedIntegrityVerifiesFullDetachedSourceOnceBeforeAnyPixels(t *testing.T) {
	calls, fullCalls := 0, 0
	st, c, s := stagedFixture(t, func(p []byte) [32]byte {
		calls++
		if len(p) == 96 {
			fullCalls++
		}
		return sha256.Sum256(p)
	})
	p := sourcePixels(s.Geometry, 1)
	expected := sha256.Sum256(p)
	d := stage(t, st, c, s, 1, p, expected)
	if calls != 0 {
		t.Fatal("unused staged surface was hashed")
	}
	p[0] ^= 255 // Caller no longer owns the Store's detached input.
	for i := 0; i < 2; i++ {
		r, err := st.Extract(context.Background(), request(s, d, c))
		if err != nil {
			t.Fatal(err)
		}
		if err = r.Visit(0, func(a fa.Artifact, p []byte) error {
			if a.State != fa.Complete || a.Proof.PayloadSHA256 != fa.Digest(sha256.Sum256(p)) {
				t.Fatal("unverified artifact")
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		r.Close()
	}
	if fullCalls != 1 || calls != 3 {
		t.Fatalf("whole-source=%d all=%d", fullCalls, calls)
	}
	if x := st.Stats(); x.IntegrityWorkspaceBytes != 1024 || x.OpenResults != 0 || x.ArtifactBytes != 0 || x.CopyBusy {
		t.Fatal(x)
	}
}

func TestStagedIntegrityRejectsCorruptionOutsideRequestedROIWithoutRetries(t *testing.T) {
	calls := 0
	st, c, s := stagedFixture(t, func(p []byte) [32]byte { calls++; return sha256.Sum256(p) })
	p := sourcePixels(s.Geometry, 1)
	expected := sha256.Sum256(p)
	p[0] ^= 1
	d := stage(t, st, c, s, 1, p, expected)
	for i := 0; i < 2; i++ {
		r, err := st.Extract(context.Background(), request(s, d, c))
		requireCode(t, err, fa.InvalidContract)
		if r != nil {
			r.Close()
			t.Fatal("corrupt source exposed a result")
		}
	}
	if calls != 1 {
		t.Fatalf("corrupt frame repeatedly hashed or artifact hashed: %d", calls)
	}
	if x := st.Stats(); x.OpenResults != 0 || x.ArtifactBytes != 0 || x.CopyBusy {
		t.Fatal(x)
	}
}

func TestStagedIntegrityDeadlineAndCancellationNeverExposePixels(t *testing.T) {
	for _, mode := range []string{"deadline", "cancel", "rebind"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var st *fa.Store
			var c *fakeClock
			var s fa.Scope
			calls := 0
			st, c, s = stagedFixture(t, func(p []byte) [32]byte {
				calls++
				if calls == 1 {
					switch mode {
					case "deadline":
						c.set(200)
					case "cancel":
						cancel()
					case "rebind":
						next := s
						next.MediaEpoch++
						if err := st.Rebind(next, admission(next)); err != nil {
							t.Fatal(err)
						}
					}
				}
				return sha256.Sum256(p)
			})
			p := sourcePixels(s.Geometry, 1)
			d := stage(t, st, c, s, 1, p, sha256.Sum256(p))
			r, err := st.Extract(ctx, request(s, d, c))
			if err == nil || r != nil {
				if r != nil {
					r.Close()
				}
				t.Fatal("stale or late source exposed pixels", err)
			}
			if x := st.Stats(); x.OpenResults != 0 || x.ArtifactBytes != 0 || x.CopyBusy {
				t.Fatal(x)
			}
		})
	}
}

func TestUnusedStagedSurfacesExpireWithoutHashing(t *testing.T) {
	calls := 0
	st, c, s := stagedFixture(t, func(p []byte) [32]byte { calls++; return sha256.Sum256(p) })
	p := sourcePixels(s.Geometry, 1)
	d := stage(t, st, c, s, 1, p, sha256.Sum256(p))
	c.set(1200)
	r, err := st.Extract(context.Background(), request(s, d, c))
	requireCode(t, err, fa.Expired)
	if r != nil || calls != 0 {
		t.Fatal("expired source exposed or hashed", calls)
	}
}
