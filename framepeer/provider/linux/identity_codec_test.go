//go:build linux

package linux

import (
	"encoding/binary"
	peer "github.com/Josepavese/videosluice/framepeer"
	"strings"
	"testing"
)

func TestProcessIdentityWireBoundsAndOwnedGeneration(t *testing.T) {
	i := ProcessIdentity{PID: 0x7fffffff, UID: ^uint32(0), GID: ^uint32(0), Generation: strings.Repeat("x", 64)}
	p, err := EncodeProcessIdentity(i)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeProcessIdentity(p)
	if err != nil || got != i {
		t.Fatal(got, err)
	}
	for n := 0; n < len(p); n++ {
		_, err := DecodeProcessIdentity(p[:n])
		want(t, err, peer.ErrInvalid)
	}
	for _, pid := range []uint32{0, 0x80000000, ^uint32(0)} {
		bad := append([]byte(nil), p...)
		binary.BigEndian.PutUint32(bad[5:9], pid)
		_, err := DecodeProcessIdentity(bad)
		want(t, err, peer.ErrInvalid)
	}
	for _, pos := range []int{0, 4, 17, 18} {
		bad := append([]byte(nil), p...)
		bad[pos] = 0xff
		_, err := DecodeProcessIdentity(bad)
		want(t, err, peer.ErrInvalid)
	}
	_, err = DecodeProcessIdentity(append(p, 0))
	want(t, err, peer.ErrInvalid)
	for n := range p {
		p[n] = 0
	}
	if got != i {
		t.Fatal("identity aliases receive buffer")
	}
}
