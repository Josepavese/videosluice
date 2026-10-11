package framedomain_test

import (
	"context"
	"errors"
	fa "github.com/Josepavese/videosluice/frameaccess"
	"github.com/Josepavese/videosluice/frameaccess/copyport"
	fd "github.com/Josepavese/videosluice/framedomain"
	"testing"
)

func TestBindingSnapshotSortedOwnedAndNeverFollowsSwitch(t *testing.T) {
	r, a := fixture(t)
	defer r.Revoke()
	rear := open(t, r, "rear", "archive", copyport.CPU{})
	defer rear.Close()
	front := open(t, r, "front", "analysis", copyport.CPU{})
	var storage [fd.MaxBindingReferences]fd.BindingRef
	storage[5].Source = "untouched"
	n, err := r.SnapshotBindingsInto(storage[:])
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 || storage[0].Source != "front" || storage[1].Source != "rear" || storage[5].Source != "untouched" {
		t.Fatal(n, storage)
	}
	old := storage[0]
	front.Close()
	front = open(t, r, "front", "analysis", copyport.CPU{})
	defer front.Close()
	if _, err = r.ResolveBinding(old); !errors.Is(err, fd.ErrRetired) {
		t.Fatal("snapshot followed replacement", err)
	}
	n, err = r.SnapshotBindingsInto(storage[:])
	if err != nil || n != 2 || storage[0].Epoch <= old.Epoch {
		t.Fatal(n, err, storage[0])
	}
	if _, err = r.SnapshotBindingsInto(storage[:1]); !errors.Is(err, fd.ErrInvalid) {
		t.Fatal("short storage accepted", err)
	}
	a.value.Store("different-execution")
	n, err = r.SnapshotBindingsInto(storage[:])
	if !errors.Is(err, fd.ErrProcessChanged) || n != 0 {
		t.Fatal("retired execution published", n, err)
	}
	for _, ref := range storage[:4] {
		if ref != (fd.BindingRef{}) {
			t.Fatal("failure retained usable references")
		}
	}
}

func TestBindingSnapshotOmitsAdmissionInProgress(t *testing.T) {
	r, _ := fixture(t)
	defer r.Revoke()
	entered, release := make(chan struct{}, 1), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		_, err := r.Open(context.Background(), layout("front", "analysis"), coreLimits(), copyport.CPU{}, admission{fail: true, entered: entered, release: release}, fa.CopyPolicy{AllowSoftware: true}, portableIntegrity())
		done <- err
	}()
	<-entered
	var storage [fd.MaxBindingReferences]fd.BindingRef
	n, err := r.SnapshotBindingsInto(storage[:])
	close(release)
	admissionErr := <-done
	if err != nil || n != 0 || admissionErr == nil {
		t.Fatal("pending source published", n, err, admissionErr)
	}
	front := open(t, r, "front", "analysis", copyport.CPU{})
	defer front.Close()
	n, err = r.SnapshotBindingsInto(storage[:])
	if err != nil || n != 1 || storage[0].Epoch != 2 {
		t.Fatal("failed epoch reused", n, err, storage[0])
	}
}

func TestBindingSnapshotConcurrentSourceReplacement(t *testing.T) {
	r, _ := fixture(t)
	defer r.Revoke()
	rear := open(t, r, "rear", "archive", copyport.CPU{})
	defer rear.Close()
	front := open(t, r, "front", "analysis", copyport.CPU{})
	done := make(chan error, 1)
	go func() {
		defer func() {
			if front != nil {
				front.Close()
			}
		}()
		for i := 0; i < 64; i++ {
			front.Close()
			var err error
			front, err = r.Open(context.Background(), layout("front", "analysis"), coreLimits(), copyport.CPU{}, admission{}, fa.CopyPolicy{AllowSoftware: true}, portableIntegrity())
			if err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	finished := false
	defer func() {
		if !finished {
			r.Revoke()
			<-done
		}
	}()
	var refs [fd.MaxBindingReferences]fd.BindingRef
	for i := 0; i < 100; i++ {
		n, err := r.SnapshotBindingsInto(refs[:])
		if err != nil {
			t.Fatal(err)
		}
		if n < 1 || n > 2 || refs[n-1] != rear.Reference() {
			t.Fatal("snapshot mixed source state", n, refs[:n])
		}
		for j, ref := range refs[:n] {
			if ref.Fence != r.Fence() || ref.Epoch == 0 || j > 0 && ref.Source <= refs[j-1].Source {
				t.Fatal("invalid concurrent snapshot", refs[:n])
			}
		}
	}
	err := <-done
	finished = true
	if err != nil {
		t.Fatal(err)
	}
}
