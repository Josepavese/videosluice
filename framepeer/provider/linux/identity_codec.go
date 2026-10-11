//go:build linux

package linux

import (
	"encoding/binary"
	peer "github.com/Josepavese/videosluice/framepeer"
)

const ProcessCapabilityType = "linux.pidfd"

// EncodeProcessIdentity serializes metadata only, never authority. It travels
// on the authenticated control channel with the corresponding pinned pidfd.
func EncodeProcessIdentity(identity ProcessIdentity) ([]byte, error) {
	if identity.PID < 1 || uint64(identity.PID) > 0x7fffffff || !validGeneration(identity.Generation) {
		return nil, failure("capability_wire_identity", peer.ErrInvalid)
	}
	p := make([]byte, 18+len(identity.Generation))
	copy(p[:4], "LPFD")
	p[4] = 1
	binary.BigEndian.PutUint32(p[5:9], uint32(identity.PID))
	binary.BigEndian.PutUint32(p[9:13], identity.UID)
	binary.BigEndian.PutUint32(p[13:17], identity.GID)
	p[17] = byte(len(identity.Generation))
	copy(p[18:], identity.Generation)
	return p, nil
}

func DecodeProcessIdentity(p []byte) (ProcessIdentity, error) {
	invalid := func() (ProcessIdentity, error) {
		return ProcessIdentity{}, failure("capability_wire_identity", peer.ErrInvalid)
	}
	if len(p) < 19 || len(p) > 82 || string(p[:4]) != "LPFD" || p[4] != 1 || int(p[17]) != len(p)-18 {
		return invalid()
	}
	pid := binary.BigEndian.Uint32(p[5:9])
	if pid == 0 || pid > 0x7fffffff {
		return invalid()
	}
	identity := ProcessIdentity{PID: int(pid), UID: binary.BigEndian.Uint32(p[9:13]), GID: binary.BigEndian.Uint32(p[13:17]), Generation: string(p[18:])}
	if !validGeneration(identity.Generation) {
		return invalid()
	}
	return identity, nil
}
