package ingress

import (
	"encoding/binary"
	fd "github.com/Josepavese/videosluice/framedomain"
	peer "github.com/Josepavese/videosluice/framepeer"
)

const DiscoveryQueryBytes = 24
const MaxDiscoveryBytes = 8192

// DiscoveryCatalog is an authenticated point-in-time binding view, NOT frame
// availability, an ACL, or a promise that epochs will survive a later switch.
// References aliases caller-admitted slots; decoded strings are owned and
// bounded (at most 64 source names of 64B plus a 64B process generation).
type DiscoveryCatalog struct {
	Challenge  [16]byte
	Broker     fd.Fence
	References []fd.BindingRef
}

func EncodeDiscoveryQueryInto(buffer []byte, challenge [16]byte) (int, error) {
	if len(buffer) < DiscoveryQueryBytes || challenge == ([16]byte{}) {
		return 0, fail("discovery_query", peer.ErrInvalid)
	}
	copy(buffer[:4], "FAPB")
	buffer[4] = 1
	buffer[5] = 1
	binary.BigEndian.PutUint16(buffer[6:8], DiscoveryQueryBytes)
	copy(buffer[8:24], challenge[:])
	return DiscoveryQueryBytes, nil
}

func DecodeDiscoveryQuery(packet []byte) ([16]byte, error) {
	var challenge [16]byte
	if len(packet) != DiscoveryQueryBytes || string(packet[:4]) != "FAPB" || packet[4] != 1 || packet[5] != 1 || binary.BigEndian.Uint16(packet[6:8]) != DiscoveryQueryBytes {
		return challenge, fail("discovery_query", peer.ErrInvalid)
	}
	copy(challenge[:], packet[8:24])
	if challenge == ([16]byte{}) {
		return challenge, fail("discovery_query", peer.ErrInvalid)
	}
	return challenge, nil
}

// BuildDiscoveryCatalog requires the Principal from an authenticated native
// control receiver. The GrantControl's pinned launcher authority is checked
// before and after taking the snapshot. No capability may accompany this RPC;
// the transport dispatcher owns rejection/cleanup of unsolicited handles.
func BuildDiscoveryCatalog(control *GrantControl, principal peer.Principal, query []byte, storage []fd.BindingRef) (DiscoveryCatalog, error) {
	challenge, err := DecodeDiscoveryQuery(query)
	if err != nil {
		return DiscoveryCatalog{}, err
	}
	fence, n, err := control.Discover(principal, storage)
	if err != nil {
		return DiscoveryCatalog{}, err
	}
	return DiscoveryCatalog{Challenge: challenge, Broker: fence, References: storage[:n]}, nil
}

func (c DiscoveryCatalog) validate() error {
	if c.Challenge == ([16]byte{}) || c.Broker.Namespace == ([16]byte{}) || !controlToken(c.Broker.ProcessGeneration) || len(c.References) > fd.MaxBindingReferences {
		return fail("discovery_catalog", peer.ErrInvalid)
	}
	for i, ref := range c.References {
		if ref.Fence != c.Broker || !controlToken(ref.Source) || ref.Epoch == 0 || i > 0 && ref.Source <= c.References[i-1].Source {
			return fail("discovery_binding", peer.ErrInvalid)
		}
	}
	return nil
}

// EncodeDiscoveryCatalogInto uses an admitted packet, no growable writer.
// Validated maxima occupy < 5000B. Invalid input leaves storage unchanged.
func EncodeDiscoveryCatalogInto(buffer []byte, c DiscoveryCatalog) (int, error) {
	if err := c.validate(); err != nil {
		return 0, err
	}
	if len(buffer) < MaxDiscoveryBytes {
		return 0, fail("discovery_buffer", peer.ErrInvalid)
	}
	w := controlWriter{data: buffer[:MaxDiscoveryBytes], n: 8}
	w.bytes(c.Challenge[:])
	w.bytes(c.Broker.Namespace[:])
	w.text(c.Broker.ProcessGeneration)
	w.byte(byte(len(c.References)))
	for _, ref := range c.References {
		w.text(ref.Source)
		w.u64(ref.Epoch)
	}
	copy(buffer[:4], "FAPB")
	buffer[4] = 1
	buffer[5] = 2
	binary.BigEndian.PutUint16(buffer[6:8], uint16(w.n))
	return w.n, nil
}

func DecodeDiscoveryCatalogInto(packet []byte, storage []fd.BindingRef) (catalog DiscoveryCatalog, err error) {
	// Storage, packet capacity, decoded metadata and transport resources must
	// be admitted by the caller. This codec is not a native Footprint quote.
	storage = storage[:min(len(storage), fd.MaxBindingReferences)]
	clear(storage)
	defer func() {
		if err != nil {
			clear(storage)
			catalog = DiscoveryCatalog{}
		}
	}()
	invalid := func() error { return fail("discovery_decode", peer.ErrInvalid) }
	if len(packet) < 8 || len(packet) > MaxDiscoveryBytes || string(packet[:4]) != "FAPB" || packet[4] != 1 || packet[5] != 2 || int(binary.BigEndian.Uint16(packet[6:8])) != len(packet) {
		return catalog, invalid()
	}
	r := controlReader{data: packet, n: 8}
	copy(catalog.Challenge[:], r.bytes(16))
	copy(catalog.Broker.Namespace[:], r.bytes(16))
	catalog.Broker.ProcessGeneration = r.text()
	n := int(r.byte())
	if n > len(storage) {
		return catalog, invalid()
	}
	for i := 0; i < n; i++ {
		storage[i] = fd.BindingRef{Fence: catalog.Broker, Source: r.text(), Epoch: r.u64()}
	}
	catalog.References = storage[:n]
	if r.bad || r.n != len(packet) {
		return catalog, invalid()
	}
	if err = catalog.validate(); err != nil {
		return catalog, err
	}
	return catalog, nil
}

// ValidateDiscoveryCatalog follows transport authentication and a fresh
// nonzero caller challenge. Expected execution is launcher-owned, never read
// from the response. A structurally valid body alone proves no identity.
func ValidateDiscoveryCatalog(c DiscoveryCatalog, challenge [16]byte, expectedExecution string, sender peer.Principal) error {
	if err := c.validate(); err != nil {
		return err
	}
	if challenge == ([16]byte{}) || !controlToken(expectedExecution) || c.Challenge != challenge || c.Broker.ProcessGeneration != expectedExecution || sender.Generation != expectedExecution {
		return fail("discovery_correlation", peer.ErrUnauthorized)
	}
	return nil
}
