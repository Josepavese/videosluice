package framedomain_test

import (
	"context"
	"crypto/sha256"
	"testing"

	fa "github.com/Josepavese/videosluice/frameaccess"
	"github.com/Josepavese/videosluice/frameaccess/copyport"
	fd "github.com/Josepavese/videosluice/framedomain"
)

func portableIntegrity() fa.PayloadIntegrity { return fa.PayloadIntegrity{Sum256: sha256.Sum256} }

func TestIntegrityWorkspaceCountsTowardSourceAndGlobalLimits(t *testing.T) {
	for _, global := range []bool{false, true} {
		limits := runtimeLimits()
		if global {
			limits.MaxMemoryBytes = coreLimits().PoolBytes
			limits.MaxSourceBytes = limits.MaxMemoryBytes
		} else {
			limits.MaxSourceBytes = coreLimits().PoolBytes
		}
		r, err := fd.New(auth(), testEntropy{}, clock{}, limits)
		if err != nil {
			t.Fatal(err)
		}
		integrity := portableIntegrity()
		integrity.WorkspaceBytes = 1
		b, err := r.Open(context.Background(), layout("front", "analysis"), coreLimits(), copyport.CPU{}, admission{}, fa.CopyPolicy{AllowSoftware: true}, integrity)
		if err != fa.CapacityRejected || b != nil {
			t.Fatalf("unaccounted workspace: binding=%v err=%v", b, err)
		}
		if r.Stats().ReservedMemoryBytes != 0 {
			t.Fatal("rejected workspace retained charge")
		}
	}
}

func TestIntegrityWorkspaceRetainedUntilResultCloses(t *testing.T) {
	r, _ := fixture(t)
	integrity := portableIntegrity()
	integrity.WorkspaceBytes = 1024
	b, err := r.Open(context.Background(), layout("front", "analysis"), coreLimits(), copyport.CPU{}, admission{}, fa.CopyPolicy{AllowSoftware: true}, integrity)
	if err != nil {
		t.Fatal(err)
	}
	if r.Stats().ReservedMemoryBytes != coreLimits().PoolBytes+1024 {
		t.Fatal("provider not reserved")
	}
	ticket := publish(t, b, "analysis", 1)
	result, err := r.Extract(context.Background(), query(b, ticket))
	if err != nil {
		t.Fatal(err)
	}
	b.Close()
	if r.Stats().ReservedMemoryBytes != coreLimits().PoolBytes+1024 {
		t.Fatal("retired provider released before result")
	}
	result.Close()
	result.Close()
	if r.Stats().ReservedMemoryBytes != 0 {
		t.Fatal("provider charge not released")
	}
}
