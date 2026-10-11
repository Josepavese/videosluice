package ingress_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"os/exec"
	"reflect"
	"testing"

	in "github.com/Josepavese/videosluice/frameingress"
	fa "github.com/Josepavese/videosluice/frameaccess"
	"github.com/Josepavese/videosluice/frameaccess/copyport"
	fd "github.com/Josepavese/videosluice/framedomain"
	peer "github.com/Josepavese/videosluice/framepeer"
)

func frameOutputFixture(t *testing.T) (in.FrameRequest, in.FrameManifest, []byte, in.ExpectedFrameEvidence) {
	t.Helper()
	f, a, _, q := accessFixture(t)
	result, err := a.Extract(context.Background(), principal(), q)
	must(t, err)
	defer result.Close()
	var manifest in.FrameManifest
	var payload []byte
	must(t, result.Visit(0, func(artifact fd.Artifact, p []byte) error {
		manifest = in.FrameManifest{ID: q.ID, Fence: q.Frame.Fence, Index: 0, Bytes: len(p), Artifact: artifact}
		payload = append([]byte(nil), p...)
		return nil
	}))
	expected := in.ExpectedFrameEvidence{Origin: fa.OriginRef{Source: f.layout.Source, MediaEpoch: f.b.Scope().MediaEpoch, Sequence: 1, Graph: f.layout.Graph}, Transform: f.tr, Admission: admitted(f.b.Scope())}
	return in.FrameRequest{ID: q.ID, Frame: q.Frame, Source: q.Source, Surface: q.Surface, Regions: q.Regions, BudgetNS: 100}, manifest, payload, expected
}

func manifestPacket(t testing.TB, m in.FrameManifest) []byte {
	t.Helper()
	buffer := make([]byte, in.MaxFrameManifestBytes)
	n, err := in.EncodeFrameManifest(buffer, m)
	must(t, err)
	return buffer[:n]
}

func dataPacket(t testing.TB, manifest []byte, offset int, payload []byte) []byte {
	t.Helper()
	buffer := make([]byte, in.MaxFrameDataBytes)
	n, err := in.EncodeFrameData(buffer, sha256.Sum256(manifest), offset, payload)
	must(t, err)
	return buffer[:n]
}

func TestFrameOutputManifestGoldenAndFullProofAssembly(t *testing.T) {
	q, m, p, expected := frameOutputFixture(t)
	manifest := manifestPacket(t, m)
	decoded, err := in.DecodeFrameManifest(manifest)
	must(t, err)
	if !reflect.DeepEqual(m, decoded) {
		t.Fatal("manifest changed")
	}
	cmd := exec.Command("python3", "-I", "-B", "frame_output_golden.py", hex.EncodeToString(manifest), hex.EncodeToString(p))
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("independent Python proof: %v %s", err, output)
	}
	buffer := make([]byte, len(p))
	assembly, err := in.NewFrameAssembly(manifest, q, expected, buffer, len(p))
	must(t, err)
	// Changes to caller metadata must not redirect the assembly.
	q.Regions[0].ID = "changed"
	clear(manifest)
	manifest = manifestPacket(t, m)
	for offset := 0; offset < len(p); offset += 8 {
		must(t, assembly.Append(dataPacket(t, manifest, offset, p[offset:offset+8])))
	}
	got, pixels, err := assembly.Finish()
	must(t, err)
	if !reflect.DeepEqual(got, m.Artifact) || !bytes.Equal(pixels, p) {
		t.Fatal("output changed")
	}
	_, _, err = assembly.Finish()
	want(t, err, peer.ErrRetired)
}

func TestFrameOutputRejectsGapsReplayCrossArtifactTruncationAndTampering(t *testing.T) {
	q, m, p, expected := frameOutputFixture(t)
	manifest := manifestPacket(t, m)
	for _, name := range []string{"gap", "duplicate", "foreign", "truncated", "overrun", "corrupt_pixels", "missing_tail", "wrong_origin", "wrong_transform", "wrong_admission"} {
		t.Run(name, func(t *testing.T) {
			e := expected
			switch name {
			case "wrong_origin":
				e.Origin.Sequence++
			case "wrong_transform":
				e.Transform.Mirror = true
			case "wrong_admission":
				e.Admission.Evidence[0] ^= 1
			}
			a, err := in.NewFrameAssembly(manifest, q, e, make([]byte, len(p)), len(p))
			must(t, err)
			packet := dataPacket(t, manifest, 0, p)
			switch name {
			case "gap":
				binary.BigEndian.PutUint32(packet[40:44], 1)
			case "duplicate":
				must(t, a.Append(packet))
			case "foreign":
				packet[8] ^= 1
			case "truncated":
				packet = packet[:len(packet)-1]
			case "overrun":
				packet = dataPacket(t, manifest, 0, append(append([]byte(nil), p...), 0))
			case "corrupt_pixels":
				packet[len(packet)-1] ^= 1
			case "missing_tail":
				packet = dataPacket(t, manifest, 0, p[:8])
			}
			err = a.Append(packet)
			if err == nil {
				artifact, pixels, finishErr := a.Finish()
				if finishErr == nil || pixels != nil || artifact != (fd.Artifact{}) {
					t.Fatal("unverified artifact escaped")
				}
			}
			_, pixels, err := a.Finish()
			if err == nil || pixels != nil {
				t.Fatal("failed assembly revived")
			}
		})
	}
}

func TestFrameManifestRejectsPartialAndMismatchedResponses(t *testing.T) {
	q, m, p, expected := frameOutputFixture(t)
	packet := manifestPacket(t, m)
	for i := 0; i < len(packet); i++ {
		part := append([]byte(nil), packet[:i]...)
		if len(part) >= 8 {
			binary.BigEndian.PutUint16(part[6:8], uint16(len(part)))
		}
		got, err := in.DecodeFrameManifest(part)
		if err == nil || got != (in.FrameManifest{}) {
			t.Fatal("partial manifest accepted")
		}
	}
	for _, change := range []func(*in.FrameRequest){func(q *in.FrameRequest) { q.ID[0]++ }, func(q *in.FrameRequest) { q.Frame.Ref.FrameID++ }, func(q *in.FrameRequest) { q.Frame.Fence.Namespace[0]++ }, func(q *in.FrameRequest) { q.Source = "rear" }, func(q *in.FrameRequest) { q.Surface = "source_decoded" }} {
		other := q
		change(&other)
		if in.ValidateFrameManifest(m, other, len(p)) == nil {
			t.Fatal("mismatched request accepted")
		}
	}
	_, err := in.NewFrameAssembly(packet, q, expected, make([]byte, len(p)), len(p)-1)
	want(t, err, fa.CapacityRejected)
	_, err = in.NewFrameAssembly(packet, q, expected, make([]byte, len(p)-1), len(p))
	want(t, err, fa.CapacityRejected)
	n, err := in.EncodeFrameManifest(make([]byte, len(packet)-1), m)
	want(t, err, fa.CapacityRejected)
	if n != 0 {
		t.Fatal("short manifest exposed")
	}
}

func TestFrameOutputFailureIsExplicitAndCannotCarryPixels(t *testing.T) {
	q, m, _, expected := frameOutputFixture(t)
	m.Bytes = 0
	m.Artifact.State = fa.CorrelationUnavailable
	m.Artifact.Proof = fd.Proof{}
	manifest := manifestPacket(t, m)
	a, err := in.NewFrameAssembly(manifest, q, expected, nil, 1024)
	must(t, err)
	artifact, pixels, err := a.Finish()
	must(t, err)
	if artifact.State != fa.CorrelationUnavailable || len(pixels) != 0 {
		t.Fatal("failure converted to frame")
	}
	m.Bytes = 24
	_, err = in.EncodeFrameManifest(make([]byte, in.MaxFrameManifestBytes), m)
	want(t, err, peer.ErrInvalid)
}

func TestFrameDataSupportsBoundedMultiPacketPayload(t *testing.T) {
	// Codec size/offset coverage independent of a camera's supported geometry.
	manifest := [32]byte{1}
	source := make([]byte, 200_000)
	destination := make([]byte, len(source))
	packet := make([]byte, in.MaxFrameDataBytes)
	for i := range source {
		source[i] = byte(i)
	}
	for offset := 0; offset < len(source); {
		end := min(offset+in.MaxFrameDataBytes-44, len(source))
		n, err := in.EncodeFrameData(packet, manifest, offset, source[offset:end])
		must(t, err)
		if n > in.MaxFrameDataBytes || int(binary.BigEndian.Uint32(packet[40:44])) != offset {
			t.Fatal("chunk bound")
		}
		copy(destination[offset:end], packet[44:n])
		offset = end
	}
	if !bytes.Equal(source, destination) {
		t.Fatal("chunked pixels changed")
	}
}

func TestLargeNativeArtifactMultiPacketProofAndAbort(t *testing.T) {
	f := setup(t)
	f.b.Close()
	layout := f.layout
	layout.Geometry = fa.Geometry{Width: 128, Height: 128, Stride: 128}
	layout.DecodedWidth = 128
	layout.DecodedHeight = 128
	b, err := f.r.Open(context.Background(), layout, fa.Limits{MaxFrames: 2, MaxBindings: 8, PoolBytes: 131072, MaxFrameBytes: 24576, MaxArtifactBytes: 32768, MaxOutputPerRequest: 32768, MaxFullFrameBytes: 24576, MaxRegions: 4, MaxResults: 4, MaxAgeNS: 1000, MaxCopyNS: 100}, copyport.CPU{}, admission{}, fa.CopyPolicy{AllowSoftware: true}, portableIntegrity())
	must(t, err)
	defer b.Close()
	ticket, err := b.AllocateDelivery("analysis")
	must(t, err)
	origin := fd.OriginRef{Fence: b.Fence(), Ref: fa.OriginRef{Source: layout.Source, MediaEpoch: b.Scope().MediaEpoch, Sequence: 1, Graph: layout.Graph}}
	transform := fa.Transform{Full: layout.Geometry, Crop: fa.Rect{X2: 128, Y2: 128}, ResizedWidth: 64, ResizedHeight: 64, DeliveryWidth: 64, DeliveryHeight: 64}
	pixels := make([]byte, 24576)
	for i := range pixels {
		pixels[i] = byte(i)
	}
	must(t, b.Publish(context.Background(), origin, []fd.Publication{{Ticket: ticket, Transform: transform}}, pixels, 100, 200))
	request := fd.Request{ID: [16]byte{4}, Frame: ticket.Ref(), Source: layout.Source, Surface: layout.Surface, Regions: []fa.Region{{ID: "full", FullFrame: true}}, DeadlineNS: 200}
	result, err := f.r.Extract(context.Background(), request)
	must(t, err)
	defer result.Close()
	query := in.FrameRequest{ID: request.ID, Frame: request.Frame, Source: request.Source, Surface: request.Surface, Regions: request.Regions, BudgetNS: 100}
	expected := in.ExpectedFrameEvidence{Origin: origin.Ref, Transform: transform, Admission: admitted(b.Scope())}
	var m in.FrameManifest
	must(t, result.Visit(0, func(artifact fd.Artifact, p []byte) error {
		m = in.FrameManifest{ID: request.ID, Fence: b.Fence(), Bytes: len(p), Artifact: artifact}
		if !bytes.Equal(p, pixels) {
			t.Fatal("native source changed")
		}
		return nil
	}))
	manifest := manifestPacket(t, m)
	a, err := in.NewFrameAssembly(manifest, query, expected, make([]byte, len(pixels)), 32768)
	must(t, err)
	for offset := 0; offset < len(pixels); {
		end := min(offset+in.MaxFrameDataBytes-44, len(pixels))
		must(t, a.Append(dataPacket(t, manifest, offset, pixels[offset:end])))
		offset = end
	}
	_, received, err := a.Finish()
	must(t, err)
	if !bytes.Equal(pixels, received) {
		t.Fatal("large verified output changed")
	}
	a, err = in.NewFrameAssembly(manifest, query, expected, make([]byte, len(pixels)), 32768)
	must(t, err)
	must(t, a.Append(dataPacket(t, manifest, 0, pixels[:128])))
	a.Abort()
	_, received, err = a.Finish()
	want(t, err, peer.ErrRetired)
	if received != nil {
		t.Fatal("aborted pixels exposed")
	}
}

func FuzzFrameManifestCodec(f *testing.F) {
	m := in.FrameManifest{ID: [16]byte{1}, Fence: fd.Fence{Namespace: [16]byte{2}, ProcessGeneration: "broker"}, Artifact: fd.Artifact{Region: "face", State: fa.Expired}}
	f.Add(manifestPacket(f, m))
	f.Add([]byte("FAPM"))
	f.Fuzz(func(t *testing.T, p []byte) {
		m, err := in.DecodeFrameManifest(p)
		if err != nil {
			if m != (in.FrameManifest{}) {
				t.Fatal("partial manifest")
			}
			return
		}
		if !bytes.Equal(p, manifestPacket(t, m)) {
			t.Fatal("noncanonical manifest")
		}
	})
}
