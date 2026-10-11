package framedomain_test

import (
	"context"
	"errors"
	fa "github.com/Josepavese/videosluice/frameaccess"
	"github.com/Josepavese/videosluice/frameaccess/copyport"
	fd "github.com/Josepavese/videosluice/framedomain"
	"testing"
)

func TestReferenceLookupRejectsPendingAdmissionAndFailedEpoch(t *testing.T) {
	r, _ := fixture(t)
	defer r.Revoke()
	entered, release := make(chan struct{}, 1), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		_, err := r.Open(context.Background(), layout("front", "analysis"), coreLimits(), copyport.CPU{}, admission{fail: true, entered: entered, release: release}, fa.CopyPolicy{AllowSoftware: true}, portableIntegrity())
		done <- err
	}()
	<-entered
	ref := fd.BindingRef{Fence: r.Fence(), Source: "front", Epoch: 1}
	_, err := r.ResolveBinding(ref)
	// Always release the owned admission job before a failing assertion.
	close(release)
	admissionErr := <-done
	if !errors.Is(err, fd.ErrRetired) {
		t.Fatal("pending binding admitted", err)
	}
	if admissionErr == nil {
		t.Fatal("failure fixture unexpectedly admitted")
	}
	current := open(t, r, "front", "analysis", copyport.CPU{})
	defer current.Close()
	if current.Reference().Epoch != 2 {
		t.Fatal("failed epoch reused")
	}
	_, err = r.ResolveBinding(ref)
	if !errors.Is(err, fd.ErrRetired) {
		t.Fatal("failed reference followed new binding", err)
	}
	got, err := r.ResolveBinding(current.Reference())
	if err != nil || got != current {
		t.Fatal("current reference rejected", err)
	}
}
