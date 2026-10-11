package ingress

import (
	"encoding/binary"
	"errors"
	peer "github.com/Josepavese/videosluice/framepeer"
	"testing"
)

func TestDiscoveryChallengeExhaustionAndZeroSkip(t *testing.T) {
	var nonce [16]byte
	binary.BigEndian.PutUint64(nonce[8:], 1)
	challenge, sequence, err := nextDiscoveryChallenge(nonce, 0)
	if err != nil || sequence != 2 || challenge == ([16]byte{}) {
		t.Fatal(challenge, sequence, err)
	}
	nonce[0] = 7
	challenge, sequence, err = nextDiscoveryChallenge(nonce, ^uint64(0)-1)
	if err != nil || sequence != ^uint64(0) || challenge == ([16]byte{}) {
		t.Fatal(challenge, sequence, err)
	}
	challenge, sequence, err = nextDiscoveryChallenge(nonce, sequence)
	if !errors.Is(err, peer.ErrRetired) || sequence != ^uint64(0) || challenge != ([16]byte{}) {
		t.Fatal("counter wrapped", sequence, err)
	}
	nonce = [16]byte{}
	binary.BigEndian.PutUint64(nonce[8:], ^uint64(0))
	challenge, sequence, err = nextDiscoveryChallenge(nonce, ^uint64(0)-1)
	if !errors.Is(err, peer.ErrRetired) || sequence != ^uint64(0) || challenge != ([16]byte{}) {
		t.Fatal("zero terminal challenge escaped", sequence, err)
	}
	_, _, err = nextDiscoveryChallenge([16]byte{}, 0)
	if !errors.Is(err, peer.ErrInvalid) {
		t.Fatal(err)
	}
}
