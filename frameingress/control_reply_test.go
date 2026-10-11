package ingress_test

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	in "github.com/Josepavese/videosluice/frameingress"
	"fmt"
	peer "github.com/Josepavese/videosluice/framepeer"
	"os/exec"
	"reflect"
	"strings"
	"testing"
)

func goldenReply(t testing.TB) in.ControlReply {
	t.Helper()
	command := goldenControl()
	reply, err := in.BuildControlReply(command.Header.Target, in.ControlResult{ID: command.Header.ID, Kind: command.Kind,
		Receipt: in.ControlReceipt{Sequence: command.Header.Sequence, ID: command.Header.ID, Grant: command.Header.Grant, Outcome: "installed", Stage: "control_installed"}}, nil)
	must(t, err)
	return reply
}
func TestControlReplyGoldenAndExactCorrelation(t *testing.T) {
	reply := goldenReply(t)
	command := goldenControl()
	packet, err := in.EncodeControlReply(reply)
	must(t, err)
	golden, err := exec.Command("python3", "-I", "-B", "control_reply_golden.py").Output()
	must(t, err)
	if hex.EncodeToString(packet) != strings.TrimSpace(string(golden)) {
		t.Fatal("Go/Python reply mismatch")
	}
	got, err := in.DecodeControlReply(packet)
	must(t, err)
	if !reflect.DeepEqual(reply, got) {
		t.Fatal("reply roundtrip lost exact fields")
	}
	must(t, in.ValidateControlReply(got, command))
	for _, change := range []func(*in.ControlReply){
		func(r *in.ControlReply) { r.Broker.Namespace[0] ^= 1 },
		func(r *in.ControlReply) { r.Broker.ProcessGeneration = "restarted-broker" },
		func(r *in.ControlReply) { r.ID[0] ^= 1; r.Receipt.ID = r.ID },
		func(r *in.ControlReply) { r.Receipt.Sequence-- },
		func(r *in.ControlReply) { r.Receipt.Grant = "different-grant" },
	} {
		changed := reply
		change(&changed)
		if in.ValidateControlReply(changed, command) == nil {
			t.Fatal("foreign reply accepted")
		}
	}
	for i := range packet {
		packet[i] = 0
	}
	if !reflect.DeepEqual(reply, got) {
		t.Fatal("reply aliases receive buffer")
	}
}
func TestControlReplyBoundsAndSnapshotIntegrity(t *testing.T) {
	command := goldenControl()
	stats := in.ControlStats{Active: 64, Receipt: goldenReply(t).Receipt}
	stats.Grants = make([]in.GrantRevision, 64)
	for i := range stats.Grants {
		stats.Grants[i] = in.GrantRevision{Name: fmt.Sprintf("%02d%s", i, strings.Repeat("x", 62)), Consumer: strings.Repeat("c", 64), Sequence: 9007199254740993 + uint64(i)}
	}
	reply, err := in.BuildControlReply(command.Header.Target, in.ControlResult{ID: command.Header.ID, Kind: in.ControlStatus, Stats: stats}, nil)
	must(t, err)
	packet, err := in.EncodeControlReply(reply)
	must(t, err)
	if len(packet) >= 10000 {
		t.Fatal("maximum status exceeds reserved wire bound", len(packet))
	}
	stats.Grants[0].Name = "mutated"
	if reply.Stats.Grants[0].Name == "mutated" {
		t.Fatal("builder aliased mutable stats")
	}
	got, err := in.DecodeControlReply(packet)
	must(t, err)
	encoded, err := in.EncodeControlReply(got)
	must(t, err)
	if !bytes.Equal(packet, encoded) {
		t.Fatal("status not canonical")
	}
	for _, change := range []func(*in.ControlStats){
		func(s *in.ControlStats) { s.Active = 65 },
		func(s *in.ControlStats) { s.Retiring = 1 },
		func(s *in.ControlStats) { s.ReleaseFailures = 1 },
		func(s *in.ControlStats) { s.Closed = true },
		func(s *in.ControlStats) { s.Grants[1].Name = s.Grants[0].Name },
		func(s *in.ControlStats) { s.Grants[0].Sequence = ^uint64(0) },
	} {
		bad := *reply.Stats
		bad.Grants = append([]in.GrantRevision(nil), reply.Stats.Grants...)
		change(&bad)
		copy := reply
		copy.Stats = &bad
		_, err := in.EncodeControlReply(copy)
		want(t, err, peer.ErrInvalid)
	}
}
func TestControlReplyRejectsAllTruncationsAndUnknownCodes(t *testing.T) {
	p, err := in.EncodeControlReply(goldenReply(t))
	must(t, err)
	for n := 0; n < len(p); n++ {
		bad := append([]byte(nil), p[:n]...)
		if n >= 8 {
			binary.BigEndian.PutUint16(bad[6:8], uint16(n))
		}
		got, err := in.DecodeControlReply(bad)
		want(t, err, peer.ErrInvalid)
		if !reflect.DeepEqual(got, in.ControlReply{}) {
			t.Fatal("partial reply returned")
		}
	}
	bad := append(append([]byte(nil), p...), 0)
	binary.BigEndian.PutUint16(bad[6:8], uint16(len(bad)))
	_, err = in.DecodeControlReply(bad)
	want(t, err, peer.ErrInvalid)
	reply := goldenReply(t)
	reply.Code = 255
	_, err = in.EncodeControlReply(reply)
	want(t, err, peer.ErrInvalid)
	result := in.ControlResult{ID: goldenControl().Header.ID, Kind: in.ControlGrant}
	rejected, err := in.BuildControlReply(goldenControl().Header.Target, result, errors.New("private provider detail"))
	must(t, err)
	p, err = in.EncodeControlReply(rejected)
	must(t, err)
	if bytes.Contains(p, []byte("private")) || rejected.Code != in.ReplyInternal {
		t.Fatal("arbitrary error detail exposed")
	}
}
func FuzzControlReply(f *testing.F) {
	p, err := in.EncodeControlReply(goldenReply(f))
	if err != nil {
		f.Fatal(err)
	}
	f.Add(p)
	f.Add([]byte("FAPR"))
	f.Fuzz(func(t *testing.T, p []byte) {
		r, err := in.DecodeControlReply(p)
		if err != nil {
			if !reflect.DeepEqual(r, in.ControlReply{}) {
				t.Fatal("partial reply")
			}
			return
		}
		encoded, err := in.EncodeControlReply(r)
		must(t, err)
		if !bytes.Equal(p, encoded) {
			t.Fatal("accepted noncanonical reply")
		}
	})
}
