package envelope

import "crypto/sha256"

// PayloadSHA256 is a trusted composition port, never a packet-selected
// algorithm. It must synchronously hash every supplied byte as SHA-256, without
// mutation or retention. Native callers select their qualified implementation;
// portable callers can supply crypto/sha256.Sum256. Framing never imports one.
type PayloadSHA256 func([]byte) [sha256.Size]byte

// SourceAssembly owns no pixel allocation. It borrows an exact-sized admitted
// header+pixel workspace for ONE authenticated source surface or output. FAPD is shared
// transport syntax, but this manifest is VBCF, never a consumer FAPM proof.
// No partial bytes are usable; any framing/order/hash error is terminal.
type SourceAssembly struct {
	header   Header
	manifest [32]byte
	storage  []byte
	next     int
	terminal bool
	digest   PayloadSHA256
}

func NewSourceAssembly(header, storage []byte, digest PayloadSHA256) (*SourceAssembly, error) {
	h, err := ParseHeader(header)
	if err != nil || digest == nil || (h.Kind != Surface && h.Kind != SourceOutput) {
		return nil, ErrInvalid
	}
	if len(storage) != HeaderSize+int(h.PayloadBytes) {
		return nil, ErrBuffer
	}
	id := sha256.Sum256(header)
	copy(storage[:HeaderSize], header)
	return &SourceAssembly{header: h, manifest: id, storage: storage[:len(storage):len(storage)], digest: digest}, nil
}

func (a *SourceAssembly) Abort() {
	if a != nil {
		a.terminal = true
		a.storage = nil
	}
}

func (a *SourceAssembly) Append(packet []byte) error {
	if a == nil || a.terminal {
		return ErrInvalid
	}
	d, err := ParseDataChunk(packet)
	if err != nil || d.Manifest != a.manifest || uint64(d.Offset) != uint64(a.next) || len(d.Payload) > int(a.header.PayloadBytes)-a.next {
		a.Abort()
		return ErrInvalid
	}
	copy(a.storage[HeaderSize+a.next:], d.Payload)
	a.next += len(d.Payload)
	return nil
}

func (a *SourceAssembly) Complete() bool {
	return a != nil && !a.terminal && a.next == int(a.header.PayloadBytes)
}

// Finish returns the same borrowed contiguous VBCF packet, exactly once. The
// caller must keep storage immutable through ingress consumption. No free is
// implied by Finish/Abort; admission lasts until actual caller/transport release.
func (a *SourceAssembly) Finish() (Header, []byte, error) {
	if !a.Complete() {
		a.Abort()
		return Header{}, nil, ErrInvalid
	}
	defer a.Abort()
	if a.digest(a.storage[HeaderSize:]) != a.header.PayloadSHA {
		return Header{}, nil, ErrInvalid
	}
	return a.header, a.storage, nil
}
