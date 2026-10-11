package ingress_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	in "github.com/Josepavese/videosluice/frameingress"
	"fmt"
	fd "github.com/Josepavese/videosluice/framedomain"
	peer "github.com/Josepavese/videosluice/framepeer"
	"math"
	"os/exec"
	"strings"
	"testing"
)

func discoveryFixture() in.DiscoveryCatalog {
	fence := fd.Fence{Namespace: [16]byte{2}, ProcessGeneration: "broker.1"}
	return in.DiscoveryCatalog{Challenge: [16]byte{1}, Broker: fence, References: []fd.BindingRef{
		{Fence: fence, Source: "front", Epoch: 1<<53 + 1}, {Fence: fence, Source: "rear", Epoch: math.MaxUint64},
	}}
}

func TestDiscoveryCodecExactEpochsGoldenAndOwnedMetadata(t *testing.T) {
	catalog := discoveryFixture()
	var packet [in.MaxDiscoveryBytes]byte
	n, err := in.EncodeDiscoveryCatalogInto(packet[:], catalog)
	must(t, err)
	output, err := exec.Command("/usr/bin/python3", "-I", "-B", "discovery_golden.py", hex.EncodeToString(packet[:n])).CombinedOutput()
	if err != nil || strings.TrimSpace(string(output)) != "discovery-golden-ok" {
		t.Fatal("independent golden failed", err, string(output))
	}
	var storage [fd.MaxBindingReferences]fd.BindingRef
	decoded, err := in.DecodeDiscoveryCatalogInto(packet[:n], storage[:])
	must(t, err)
	if len(decoded.References) != 2 || decoded.References[0].Epoch != 1<<53+1 || decoded.References[1].Epoch != math.MaxUint64 {
		t.Fatal(decoded)
	}
	must(t, in.ValidateDiscoveryCatalog(decoded, catalog.Challenge, "broker.1", peer.Principal{Generation: "broker.1"}))
	clear(packet[:])
	if decoded.References[0].Source != "front" || decoded.Broker.ProcessGeneration != "broker.1" {
		t.Fatal("decoded metadata aliases packet")
	}
}

func TestDiscoveryRejectsReplayForeignExecutionAndMalformedStorage(t *testing.T) {
	c := discoveryFixture()
	for _, entry := range []struct {
		challenge        [16]byte
		expected, sender string
	}{
		{[16]byte{3}, "broker.1", "broker.1"}, {c.Challenge, "replacement", "broker.1"}, {c.Challenge, "broker.1", "foreign"},
	} {
		if err := in.ValidateDiscoveryCatalog(c, entry.challenge, entry.expected, peer.Principal{Generation: entry.sender}); !errors.Is(err, peer.ErrUnauthorized) {
			t.Fatal("foreign/replayed catalog accepted", err)
		}
	}
	var packet [in.MaxDiscoveryBytes]byte
	n, err := in.EncodeDiscoveryCatalogInto(packet[:], c)
	must(t, err)
	var storage [fd.MaxBindingReferences]fd.BindingRef
	for length := 0; length < n; length++ {
		storage[0] = c.References[0]
		decoded, err := in.DecodeDiscoveryCatalogInto(packet[:length], storage[:])
		if err == nil || len(decoded.References) != 0 || storage[0] != (fd.BindingRef{}) {
			t.Fatalf("truncation %d leaked result", length)
		}
	}
	if _, err = in.DecodeDiscoveryCatalogInto(packet[:n], storage[:1]); err == nil {
		t.Fatal("short storage accepted")
	}
	for _, mutate := range []func([]byte){func(p []byte) { p[4] = 2 }, func(p []byte) { p[5] = 1 }, func(p []byte) { clear(p[n-8 : n]) }} {
		bad := append([]byte(nil), packet[:n]...)
		mutate(bad)
		if _, err = in.DecodeDiscoveryCatalogInto(bad, storage[:]); err == nil {
			t.Fatal("malformed catalog accepted")
		}
	}
	for _, mode := range []string{"duplicate", "unsorted", "foreign_fence", "zero_epoch", "too_many"} {
		bad := discoveryFixture()
		switch mode {
		case "duplicate":
			bad.References[1].Source = "front"
		case "unsorted":
			bad.References[0], bad.References[1] = bad.References[1], bad.References[0]
		case "foreign_fence":
			bad.References[0].Fence.Namespace[0] = 3
		case "zero_epoch":
			bad.References[0].Epoch = 0
		case "too_many":
			bad.References = make([]fd.BindingRef, 65)
		}
		before := packet
		if _, err = in.EncodeDiscoveryCatalogInto(packet[:], bad); err == nil || packet != before {
			t.Fatal("invalid encode modified output", mode, err)
		}
	}
}

func TestDiscoveryMaximumAndQueryContract(t *testing.T) {
	c := discoveryFixture()
	c.Broker.ProcessGeneration = strings.Repeat("x", 64)
	c.References = make([]fd.BindingRef, fd.MaxBindingReferences)
	for i := range c.References {
		c.References[i] = fd.BindingRef{Fence: c.Broker, Source: fmt.Sprintf("%064d", i), Epoch: math.MaxUint64}
	}
	var packet [in.MaxDiscoveryBytes]byte
	n, err := in.EncodeDiscoveryCatalogInto(packet[:], c)
	must(t, err)
	if n >= 5000 {
		t.Fatal("catalog exceeds stated bound", n)
	}
	var refs [fd.MaxBindingReferences]fd.BindingRef
	decoded, err := in.DecodeDiscoveryCatalogInto(packet[:n], refs[:])
	must(t, err)
	if len(decoded.References) != 64 {
		t.Fatal(len(decoded.References))
	}
	n, err = in.EncodeDiscoveryQueryInto(packet[:], c.Challenge)
	must(t, err)
	challenge, err := in.DecodeDiscoveryQuery(packet[:n])
	must(t, err)
	if challenge != c.Challenge {
		t.Fatal(challenge)
	}
	for length := 0; length < n; length++ {
		if _, err = in.DecodeDiscoveryQuery(packet[:length]); err == nil {
			t.Fatal(length)
		}
	}
	if _, err = in.EncodeDiscoveryQueryInto(packet[:], [16]byte{}); err == nil {
		t.Fatal("zero challenge accepted")
	}
}

func TestDiscoveryUsesPinnedControlAuthorityWithoutMutatingGrants(t *testing.T) {
	f, control, authority, _ := controlFixture(t, 2)
	defer f.r.Revoke()
	var request [in.DiscoveryQueryBytes]byte
	_, err := in.EncodeDiscoveryQueryInto(request[:], [16]byte{1})
	must(t, err)
	var storage [fd.MaxBindingReferences]fd.BindingRef
	before, err := control.Status(controllerPrincipal())
	must(t, err)
	catalog, err := in.BuildDiscoveryCatalog(control, controllerPrincipal(), request[:], storage[:])
	must(t, err)
	if len(catalog.References) != 1 || catalog.References[0] != f.b.Reference() {
		t.Fatal(catalog)
	}
	after, err := control.Status(controllerPrincipal())
	must(t, err)
	if before.Receipt != after.Receipt || after.Active != before.Active {
		t.Fatal("discovery consumed mutation sequence")
	}
	if _, err = in.BuildDiscoveryCatalog(control, peer.Principal{Generation: "another"}, request[:], storage[:]); !errors.Is(err, peer.ErrUnauthorized) {
		t.Fatal(err)
	}
	f.b.Close()
	catalog, err = in.BuildDiscoveryCatalog(control, controllerPrincipal(), request[:], storage[:])
	must(t, err)
	if len(catalog.References) != 0 {
		t.Fatal("retired binding published")
	}
	authority.generation.Store("restarted-controller")
	if _, err = in.BuildDiscoveryCatalog(control, controllerPrincipal(), request[:], storage[:]); err == nil {
		t.Fatal("old controller still allowed discovery")
	}
}

func FuzzDiscoveryDecode(f *testing.F) {
	var buffer [in.MaxDiscoveryBytes]byte
	n, _ := in.EncodeDiscoveryCatalogInto(buffer[:], discoveryFixture())
	f.Add(append([]byte(nil), buffer[:n]...))
	f.Fuzz(func(t *testing.T, packet []byte) {
		var storage [fd.MaxBindingReferences]fd.BindingRef
		catalog, err := in.DecodeDiscoveryCatalogInto(packet, storage[:])
		if err != nil {
			return
		}
		var encoded [in.MaxDiscoveryBytes]byte
		n, err := in.EncodeDiscoveryCatalogInto(encoded[:], catalog)
		must(t, err)
		if !bytes.Equal(packet, encoded[:n]) {
			t.Fatal("noncanonical packet accepted")
		}
	})
}

func TestDiscoveryDispatcherOwnsRejectionsAndSharesMutationSlot(t *testing.T) {
	f, control, _, _ := controlFixture(t, 2)
	defer f.r.Revoke()
	d, err := in.NewControlDispatcher(control)
	must(t, err)
	var query [in.DiscoveryQueryBytes]byte
	_, err = in.EncodeDiscoveryQueryInto(query[:], [16]byte{1})
	must(t, err)
	var refs [fd.MaxBindingReferences]fd.BindingRef
	entered, release := make(chan struct{}), make(chan struct{})
	pending := &pendingFixture{owner: consumer(), entered: entered, unblock: release}
	done := make(chan error, 1)
	go func() {
		_, _, err := d.Apply(context.Background(), controllerPrincipal(), dispatchPacket(t, f, in.ControlGrant, 1, 0), pending)
		done <- err
	}()
	<-entered
	probe := &pendingFixture{}
	_, accepted, err := d.Discover(context.Background(), controllerPrincipal(), query[:], refs[:], probe)
	close(release)
	mutationErr := <-done
	must(t, mutationErr)
	if accepted || !errors.Is(err, peer.ErrBusy) || probe.closed.Load() != 0 {
		t.Fatal("busy discovery stole ownership", accepted, err)
	}
	must(t, probe.Close())
	probe = &pendingFixture{closeErr: errors.New("native close unconfirmed")}
	_, accepted, err = d.Discover(context.Background(), controllerPrincipal(), query[:], refs[:], probe)
	if !accepted || err == nil || probe.imports.Load() != 0 || probe.closed.Load() != 1 || !d.State().Closed || d.State().ReleaseFailures != 1 {
		t.Fatal("capability rejection not retained", accepted, err, d.State())
	}
	d.Close()
	if probe.closed.Load() != 1 {
		t.Fatal("failed close retried")
	}
}
