package envelope

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"math"
	"os/exec"
	"reflect"
	"testing"
)

func fixture() Header {
	return Header{Kind: Delivery, Codec: NV12, Surface: Delivered, Correlation: 1,
		Flags: CaptureTime | MediaTime, Width: 4, Height: 2, StrideY: 8, StrideUV: 8,
		SourceEpoch: math.MaxUint64 - 3, OriginSequence: math.MaxUint64 - 2,
		PublicationEpoch: math.MaxUint64 - 1, DeliveryID: math.MaxUint64, CaptureNS: 123456,
		MediaPTS: -2345, Namespace: [16]byte{1}, Graph: sha256.Sum256([]byte("graph")),
		Transform: sha256.Sum256([]byte("transform")), Source: "front", Stream: "analysis", Process: "broker-1"}
}
func vectors(t *testing.T) [][]byte {
	t.Helper()
	h := fixture()
	payload := make([]byte, 24)
	for i := range payload {
		payload[i] = byte(i)
	}
	pack := func(h Header, p []byte) []byte {
		b, e := Marshal(h, p)
		if e != nil {
			t.Fatal(e, h)
		}
		return b
	}
	v := [][]byte{pack(h, payload)}
	h.Kind = Surface
	h.Surface = ProcessedFull
	h.Stream = ""
	h.PublicationEpoch = 0
	h.DeliveryID = 0
	v = append(v, pack(h, payload))
	h = fixture()
	h.Codec = H265
	h.StrideY = 0
	h.StrideUV = 0
	h.OriginSequence = 0
	h.Correlation = 0
	h.Flags = MediaTime | Keyframe | CodecConfig | StreamReset
	h.CaptureNS = 0
	v = append(v, pack(h, []byte{0, 0, 0, 1, 64, 1}))
	h = fixture()
	h.Kind = Epoch
	h.Codec = 0
	h.Surface = 0
	h.Correlation = 0
	h.Flags = 0
	h.Width = 0
	h.Height = 0
	h.StrideY = 0
	h.StrideUV = 0
	h.OriginSequence = 0
	h.DeliveryID = 0
	h.CaptureNS = 0
	h.MediaPTS = 0
	h.Transform = [32]byte{}
	v = append(v, pack(h, nil))
	h = fixture()
	h.Codec = H264
	h.StrideY = 0
	h.StrideUV = 0
	h.Flags |= Keyframe
	h.MediaPTS = math.MinInt64
	h.Source = string(bytes.Repeat([]byte{'S'}, 64))
	h.Stream = string(bytes.Repeat([]byte{'T'}, 64))
	h.Process = string(bytes.Repeat([]byte{'P'}, 64))
	v = append(v, pack(h, []byte{0, 0, 0, 1, 103}))
	h = fixture()
	h.Kind, h.OutputSequence, h.PublicationEpoch, h.DeliveryID = SourceOutput, math.MaxUint64, 0, 0
	v = append(v, pack(h, payload))
	h.Correlation, h.OriginSequence, h.CaptureNS, h.Flags = 0, 0, 0, MediaTime
	v = append(v, pack(h, payload))
	return v
}

func TestSourceOutputIsNotDeliveryAuthority(t *testing.T) {
	h, p, err := Parse(vectors(t)[5])
	if err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*Header){
		func(h *Header) { h.OutputSequence = 0 },
		func(h *Header) { h.PublicationEpoch = 1 },
		func(h *Header) { h.DeliveryID = 1 },
		func(h *Header) { h.Kind = Delivery; h.PublicationEpoch = 1; h.DeliveryID = 1 },
		func(h *Header) { h.Stream = "" },
	} {
		bad := h
		change(&bad)
		if _, err := Marshal(bad, p); err == nil {
			t.Fatal("output minted its own delivery", bad)
		}
	}
}

func TestRoundTrip(t *testing.T) {
	for _, b := range vectors(t) {
		h, p, e := Parse(b)
		if e != nil {
			t.Fatal(e)
		}
		rebuilt, e := Marshal(h, p)
		if e != nil || !bytes.Equal(b, rebuilt) {
			t.Fatal("noncanonical roundtrip", e)
		}
	}
}
func TestKnownOffsetsAndSignedness(t *testing.T) {
	b := vectors(t)[0]
	if string(b[:4]) != "VBCF" || binary.BigEndian.Uint16(b[6:8]) != 448 || binary.BigEndian.Uint64(b[64:72]) != math.MaxUint64 || int64(binary.BigEndian.Uint64(b[80:88])) != -2345 || string(b[200:205]) != "front" {
		t.Fatal("layout")
	}
}
func TestRejectInvalidIdentityAndLayout(t *testing.T) {
	cases := []func(*Header){func(h *Header) { h.Correlation = 0 }, func(h *Header) { h.SourceEpoch = 0 }, func(h *Header) { h.PublicationEpoch = 0 }, func(h *Header) { h.DeliveryID = 0 }, func(h *Header) { h.Namespace = [16]byte{} }, func(h *Header) { h.Graph = [32]byte{} }, func(h *Header) { h.Transform = [32]byte{} }, func(h *Header) { h.Source = "x\x00y" }, func(h *Header) { h.Source = "é" }, func(h *Header) { h.Process = string(bytes.Repeat([]byte{'x'}, 65)) }, func(h *Header) { h.StrideY = 2 }, func(h *Header) { h.Width = 3 }, func(h *Header) { h.Height = 3 }, func(h *Header) { h.StrideY = math.MaxUint32 - 1 }, func(h *Header) { h.Flags |= 32 }, func(h *Header) { h.Flags = 0 }, func(h *Header) { h.Flags |= Keyframe }, func(h *Header) { h.Surface = SourceDecoded }}
	for i, f := range cases {
		h := fixture()
		f(&h)
		if _, e := Marshal(h, make([]byte, 24)); e == nil {
			t.Fatal("accepted", i)
		}
	}
}
func TestRejectFramingAndPayload(t *testing.T) {
	b := vectors(t)[0]
	for i := 0; i < len(b); i++ {
		if _, _, e := Parse(b[:i]); e == nil {
			t.Fatal("truncated", i)
		}
	}
	for _, offset := range []int{0, 4, 6, 8, 9, 10, 11, 12, 16, 36, 37, 38, 39, 206, 270, 337, 392, 447, 448} {
		c := bytes.Clone(b)
		c[offset] ^= 128
		if _, _, e := Parse(c); e == nil {
			t.Fatal("corruption accepted", offset)
		}
	}
	if _, _, e := Parse(append(bytes.Clone(b), 0)); e == nil {
		t.Fatal("trailing")
	}
}

func TestPythonIndependentVectorsAndMutationParity(t *testing.T) {
	originals := vectors(t)
	inputs := []string{}
	want := []bool{}
	for _, b := range originals {
		inputs = append(inputs, hex.EncodeToString(b))
		want = append(want, true)
	}
	// Every header byte mutation is independently classified by both parsers.
	// Graph/namespace changes can remain structurally valid: these are not auth.
	for _, base := range originals {
		for i := 0; i < len(base); i++ {
			b := bytes.Clone(base)
			b[i] ^= 128
			_, _, e := Parse(b)
			inputs = append(inputs, hex.EncodeToString(b))
			want = append(want, e == nil)
		}
	}
	wire, _ := json.Marshal(inputs)
	cmd := exec.Command("python3", "-B", "crosscheck.py")
	cmd.Stdin = bytes.NewReader(wire)
	out, e := cmd.CombinedOutput()
	if e != nil {
		t.Fatalf("python: %v %s", e, out)
	}
	var got struct {
		Accepted []bool   `json:"accepted"`
		Vectors  []string `json:"vectors"`
	}
	if e = json.Unmarshal(out, &got); e != nil {
		t.Fatal(e)
	}
	if !reflect.DeepEqual(want, got.Accepted) {
		for i := range want {
			if i >= len(got.Accepted) || want[i] != got.Accepted[i] {
				t.Fatalf("parser divergence %d", i)
			}
		}
		t.Fatal("count mismatch")
	}
	if len(got.Vectors) != len(originals) {
		t.Fatal("vector count")
	}
	for i, s := range got.Vectors {
		if s != hex.EncodeToString(originals[i]) {
			t.Fatal("independent encode mismatch", i)
		}
	}
}
func FuzzParse(f *testing.F) {
	h := fixture()
	b, _ := Marshal(h, make([]byte, 24))
	f.Add(b)
	f.Add([]byte("NMB2"))
	f.Fuzz(func(t *testing.T, b []byte) {
		h, p, e := Parse(b)
		if e == nil {
			r, e := Marshal(h, p)
			if e != nil || !bytes.Equal(r, b) {
				t.Fatal("canonical mismatch")
			}
		}
	})
}

// A frame that carries pixels always carries their digest. An all-zero
// PayloadSHA is never a real payload digest, and accepting it would let a
// digest computation that could not run match a frame claiming zero — which
// would publish an unverified frame. Marshal derives the digest from the bytes
// it packs, so this can only be reached by a forged header.
func TestZeroPayloadDigestIsRejected(t *testing.T) {
	h := fixture()
	h.PayloadBytes = 24
	h.PayloadSHA = [32]byte{}
	if err := h.Validate(); err == nil {
		t.Fatal("an all-zero payload digest was accepted")
	}
	h.PayloadSHA = sha256.Sum256([]byte{0x01})
	if err := h.Validate(); err != nil {
		t.Fatalf("a real payload digest was rejected: %v", err)
	}
}
