package frameaccess_test

import (
	"context"
	"crypto/sha256"
	"testing"

	fa "github.com/Josepavese/videosluice/frameaccess"
	"github.com/Josepavese/videosluice/frameaccess/copyport"
)

func portableIntegrity() fa.PayloadIntegrity {
	return fa.PayloadIntegrity{Sum256: sha256.Sum256}
}

func TestExtractUsesAdmittedPayloadIntegrityAndKeepsDeadline(t *testing.T) {
	for _, expire := range []bool{false, true} {
		c := &fakeClock{}
		c.set(100)
		s := scope("front")
		calls := 0
		integrity := fa.PayloadIntegrity{WorkspaceBytes: 1024, Sum256: func(p []byte) [32]byte {
			calls++
			if expire {
				c.set(200)
			}
			return sha256.Sum256(p)
		}}
		st, err := fa.NewStore(s, limits(), c, copyport.CPU{}, admission(s), fa.CopyPolicy{AllowSoftware: true}, integrity)
		if err != nil {
			t.Fatal(err)
		}
		d := publish(t, st, c, s, 1)
		result, err := st.Extract(context.Background(), request(s, d, c))
		if calls != 1 {
			t.Fatalf("provider invocations=%d", calls)
		}
		if expire {
			requireCode(t, err, fa.DeadlineExceeded)
			if result != nil {
				t.Fatal("late digest exposed a result")
			}
		} else {
			if err != nil {
				t.Fatal(err)
			}
			if err := result.Visit(0, func(a fa.Artifact, p []byte) error {
				if a.Proof.PayloadSHA256 != fa.Digest(sha256.Sum256(p)) {
					t.Fatal("changed SHA contract")
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			result.Close()
		}
		stats := st.Stats()
		if stats.IntegrityWorkspaceBytes != 1024 || stats.PoolBytes != limits().PoolBytes || stats.OpenResults != 0 || stats.ArtifactBytes != 0 || stats.CopyBusy {
			t.Fatalf("incorrect provider reservation/cleanup: %+v", stats)
		}
	}
}

func TestIntegrityMemoryQuoteRejectsInvalidProvider(t *testing.T) {
	for _, p := range []fa.PayloadIntegrity{{}, {Sum256: sha256.Sum256, WorkspaceBytes: -1}, {Sum256: sha256.Sum256, WorkspaceBytes: 1<<20 + 1}} {
		_, err := fa.StoreMemoryBytes(limits(), p)
		requireCode(t, err, fa.InvalidContract)
	}
	n, err := fa.StoreMemoryBytes(limits(), fa.PayloadIntegrity{Sum256: sha256.Sum256, WorkspaceBytes: 1 << 20})
	if err != nil || n != limits().PoolBytes+1<<20 {
		t.Fatalf("quote=%d err=%v", n, err)
	}
}

func TestIntegrityProviderSharesOneCopySlot(t *testing.T) {
	c := &fakeClock{}
	c.set(100)
	s := scope("front")
	entered, release := make(chan struct{}), make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	p := fa.PayloadIntegrity{WorkspaceBytes: 1024, Sum256: func(pixels []byte) [32]byte {
		close(entered)
		<-release
		return sha256.Sum256(pixels)
	}}
	st, err := fa.NewStore(s, limits(), c, copyport.CPU{}, admission(s), fa.CopyPolicy{AllowSoftware: true}, p)
	if err != nil {
		t.Fatal(err)
	}
	d := publish(t, st, c, s, 1)
	done := make(chan error, 1)
	go func() {
		r, err := st.Extract(context.Background(), request(s, d, c))
		if r != nil {
			r.Close()
		}
		done <- err
	}()
	wait(t, entered)
	_, err = st.Extract(context.Background(), request(s, d, c))
	requireCode(t, err, fa.CapacityRejected)
	err = st.TryPublish(context.Background(), origin(s, 2), []fa.DeliveryBinding{{Ref: delivery(s, "analysis", 2), Transform: identity(s.Geometry)}}, sourcePixels(s.Geometry, 2), 100, 200)
	requireCode(t, err, fa.CapacityRejected)
	if st.Stats().IntegrityWorkspaceBytes != 1024 {
		t.Fatal("parallel workspace escaped reservation")
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
