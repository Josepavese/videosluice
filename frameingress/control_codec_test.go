package ingress_test

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	in "github.com/Josepavese/videosluice/frameingress"
	"fmt"
	fd "github.com/Josepavese/videosluice/framedomain"
	peer "github.com/Josepavese/videosluice/framepeer"
	"os/exec"
	"reflect"
	"strings"
	"testing"
)

func goldenControl() in.ControlCommand {
	fence := fd.Fence{Namespace: [16]byte{1}, ProcessGeneration: "broker-1"}
	return in.ControlCommand{
		Kind:           in.ControlGrant,
		Header:         in.ControlHeader{Target: fence, ID: [16]byte{2}, Sequence: ^uint64(0) - 1, Previous: 9007199254740993, Grant: "reader"},
		Binding:        fd.BindingRef{Fence: fence, Source: "front", Epoch: 9007199254740995},
		Policy:         in.AccessPolicy{Generation: "consumer-1", Streams: []string{"analysis", "live"}, MaxRegions: 16, MaxRequests: 64, MaxResults: 32, AllowFullFrame: true},
		CapabilityType: "test.capability", CapabilityMetadata: []byte{0, 0xff, 1},
	}
}

func TestControlCodecRoundtripGoldenAndSnapshot(t *testing.T) {
	c := goldenControl()
	packet, err := in.EncodeControl(c)
	must(t, err)
	encoded, err := exec.Command("python3", "-I", "-B", "control_golden.py").Output()
	must(t, err)
	if hex.EncodeToString(packet) != strings.TrimSpace(string(encoded)) {
		t.Fatal("Python/Go control wire disagree")
	}
	decoded, err := in.DecodeControl(packet)
	must(t, err)
	if !reflect.DeepEqual(c, decoded) {
		t.Fatalf("roundtrip lost exact identity or uint64: %+v", decoded)
	}
	for i := range packet {
		packet[i] = 0
	}
	if !reflect.DeepEqual(c, decoded) {
		t.Fatal("decoded metadata aliases receive buffer")
	}
	for _, kind := range []in.ControlKind{in.ControlRevoke, in.ControlStatus} {
		command := in.ControlCommand{Kind: kind, Header: c.Header}
		if kind == in.ControlStatus {
			command.Header.Sequence = 0
			command.Header.Previous = 0
			command.Header.Grant = ""
		}
		p, err := in.EncodeControl(command)
		must(t, err)
		got, err := in.DecodeControl(p)
		must(t, err)
		if !reflect.DeepEqual(command, got) {
			t.Fatal("control variant mismatch")
		}
	}
}

func TestControlCodecRejectsEveryTruncationAndInvalidMetadata(t *testing.T) {
	packet, err := in.EncodeControl(goldenControl())
	must(t, err)
	for i := 0; i < len(packet); i++ {
		p := append([]byte(nil), packet[:i]...)
		if len(p) >= 8 {
			binary.BigEndian.PutUint16(p[6:8], uint16(len(p)))
		}
		got, err := in.DecodeControl(p)
		want(t, err, peer.ErrInvalid)
		if !reflect.DeepEqual(got, in.ControlCommand{}) {
			t.Fatal("partial command escaped")
		}
	}
	for _, pos := range []int{0, 4, 5} {
		p := append([]byte(nil), packet...)
		p[pos] = 0xff
		_, err := in.DecodeControl(p)
		want(t, err, peer.ErrInvalid)
	}
	p := append(append([]byte(nil), packet...), 0)
	binary.BigEndian.PutUint16(p[6:8], uint16(len(p)))
	_, err = in.DecodeControl(p)
	want(t, err, peer.ErrInvalid)
	for _, mutate := range []func(*in.ControlCommand){
		func(c *in.ControlCommand) { c.Kind = 99 },
		func(c *in.ControlCommand) { c.Header.Sequence = 0 },
		func(c *in.ControlCommand) { c.Header.ID = [16]byte{} },
		func(c *in.ControlCommand) { c.Binding.Epoch = 0 },
		func(c *in.ControlCommand) { c.Binding.Fence.Namespace[0] ^= 1 },
		func(c *in.ControlCommand) { c.Binding.Source = "*" },
		func(c *in.ControlCommand) { c.Policy.Streams = []string{"live", "analysis"} },
		func(c *in.ControlCommand) { c.Policy.Streams = []string{"analysis", "analysis"} },
		func(c *in.ControlCommand) { c.Policy.Streams = make([]string, 17) },
		func(c *in.ControlCommand) { c.Policy.MaxResults = 65 },
		func(c *in.ControlCommand) { c.Policy.Generation = strings.Repeat("x", 65) },
		func(c *in.ControlCommand) { c.CapabilityMetadata = make([]byte, 257) },
		func(c *in.ControlCommand) { c.Kind = in.ControlRevoke },
	} {
		c := goldenControl()
		mutate(&c)
		p, err := in.EncodeControl(c)
		want(t, err, peer.ErrInvalid)
		if p != nil {
			t.Fatal("invalid command encoded")
		}
	}
}

func TestControlCodecMaximumBoundAndCanonicalEncoding(t *testing.T) {
	c := goldenControl()
	token := strings.Repeat("x", 64)
	c.Header.Target.ProcessGeneration = token
	c.Header.Grant = token
	c.Binding.Fence = c.Header.Target
	c.Binding.Source = token
	c.Policy.Generation = token
	c.CapabilityType = token
	c.CapabilityMetadata = make([]byte, 256)
	c.Policy.Streams = make([]string, 16)
	for i := range c.Policy.Streams {
		c.Policy.Streams[i] = fmt.Sprintf("%02d%s", i, strings.Repeat("x", 62))
	}
	p, err := in.EncodeControl(c)
	must(t, err)
	if len(p) >= 1800 {
		t.Fatal("validated maximum exceeded fixed-buffer proof", len(p))
	}
	decoded, err := in.DecodeControl(p)
	must(t, err)
	reencoded, err := in.EncodeControl(decoded)
	must(t, err)
	if !bytes.Equal(p, reencoded) {
		t.Fatal("noncanonical control encoding")
	}
}

func FuzzControlCodec(f *testing.F) {
	p, err := in.EncodeControl(goldenControl())
	if err != nil {
		f.Fatal(err)
	}
	f.Add(p)
	f.Add([]byte("FAPC"))
	f.Fuzz(func(t *testing.T, p []byte) {
		c, err := in.DecodeControl(p)
		if err != nil {
			if !reflect.DeepEqual(c, in.ControlCommand{}) {
				t.Fatal("partial result")
			}
			return
		}
		encoded, err := in.EncodeControl(c)
		must(t, err)
		if !bytes.Equal(p, encoded) {
			t.Fatal("accepted noncanonical packet")
		}
	})
}
