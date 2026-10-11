//go:build linux

package linux

import (
	peer "github.com/Josepavese/videosluice/framepeer"
	"os"
	"testing"
)

func TestPendingCapabilityTransfersExactOwnerOnlyOnSuccess(t *testing.T) {
	r, _, _ := receiver(t)
	file, identity, err := r.lease.Export()
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := EncodeProcessIdentity(identity)
	if err != nil {
		file.Close()
		t.Fatal(err)
	}
	pending, err := OwnReceivedCapability(file)
	if err != nil {
		file.Close()
		t.Fatal(err)
	}
	defer pending.Close()
	_, err = pending.Import("unavailable-provider", metadata)
	want(t, err, peer.ErrUnavailable)
	_, err = pending.Import(ProcessCapabilityType, []byte("bad metadata"))
	want(t, err, peer.ErrInvalid)
	capability, err := pending.Import(ProcessCapabilityType, metadata)
	if err != nil {
		t.Fatal(err)
	}
	defer capability.Close()
	if err := pending.Close(); err != nil {
		t.Fatal(err)
	}
	if got, err := capability.CurrentGeneration(); err != nil || got != identity.Generation {
		t.Fatal("raw close invalidated transferred owner", got, err)
	}
	_, err = pending.Import(ProcessCapabilityType, metadata)
	want(t, err, peer.ErrRetired)
	if err := capability.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := r.lease.CurrentGeneration(); err != nil {
		t.Fatal("original launcher handle closed", err)
	}
}

func TestPendingRejectClosesRawFileOnce(t *testing.T) {
	file, err := os.Open("/dev/null")
	if err != nil {
		t.Fatal(err)
	}
	pending, err := OwnReceivedCapability(file)
	if err != nil {
		file.Close()
		t.Fatal(err)
	}
	metadata, err := EncodeProcessIdentity(ProcessIdentity{PID: os.Getpid(), Generation: "test"})
	if err != nil {
		pending.Close()
		t.Fatal(err)
	}
	_, err = pending.Import(ProcessCapabilityType, metadata)
	want(t, err, peer.ErrInvalid)
	if err = pending.Close(); err != nil {
		t.Fatal(err)
	}
	if err = pending.Close(); err != nil {
		t.Fatal("raw close retried", err)
	}
	if _, err = file.Stat(); err == nil {
		t.Fatal("rejected raw file still open")
	}
	_, err = pending.Import(ProcessCapabilityType, metadata)
	want(t, err, peer.ErrRetired)
}
