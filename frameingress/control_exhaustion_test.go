package ingress

import (
	"errors"
	fd "github.com/Josepavese/videosluice/framedomain"
	peer "github.com/Josepavese/videosluice/framepeer"
	"testing"
)

type controlFixtureAuthority struct{}

func (controlFixtureAuthority) CurrentGeneration() (string, error) { return "controller", nil }

type controlFixtureClock struct{}

func (controlFixtureClock) NowNS() uint64 { return 1 }

type controlFixtureEntropy struct{}

func (controlFixtureEntropy) Fill(b []byte) error { b[0] = 1; return nil }

func TestControlSequenceExhaustionNeverWrapsOrReopens(t *testing.T) {
	r, err := fd.New(controlFixtureAuthority{}, controlFixtureEntropy{}, controlFixtureClock{}, fd.Limits{MaxSources: 1, MaxStreams: 1, MaxSourceGenerations: 1, MaxMemoryBytes: 1, MaxSourceBytes: 1, MaxBuffers: 1, MaxBufferBytes: 1, MaxResources: 1, MaxResourceOperations: 1})
	if err != nil {
		t.Fatal(err)
	}
	c, err := NewGrantControl(r, controlFixtureAuthority{}, "controller", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.last.Sequence = ^uint64(0) - 1 // deterministic boundary fixture, no runtime hook
	h := ControlHeader{Target: r.Fence(), Sequence: ^uint64(0), ID: [16]byte{1}, Grant: "reader"}
	p := peer.Principal{Generation: "controller"}
	receipt, err := c.Revoke(p, h)
	if err != nil || receipt.Sequence != ^uint64(0) || receipt.Outcome != "absent" {
		t.Fatal(receipt, err)
	}
	h.Sequence = 0
	_, err = c.Revoke(p, h)
	if !errors.Is(err, ErrControlSequence) {
		t.Fatal(err)
	}
	h.Sequence = 1
	_, err = c.Revoke(p, h)
	if !errors.Is(err, peer.ErrRetired) {
		t.Fatal(err)
	}
	s, _ := c.Status(p)
	if !s.Closed || s.Receipt.Sequence != ^uint64(0) {
		t.Fatal(s)
	}
}
