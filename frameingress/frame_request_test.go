package ingress_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"math"
	"os/exec"
	"reflect"
	"strings"
	"testing"

	in "github.com/Josepavese/videosluice/frameingress"
	fa "github.com/Josepavese/videosluice/frameaccess"
	fd "github.com/Josepavese/videosluice/framedomain"
	peer "github.com/Josepavese/videosluice/framepeer"
)

func goldenFrameRequest() in.FrameRequest {
	return in.FrameRequest{ID: [16]byte{2}, Frame: fd.DeliveryRef{
		Fence: fd.Fence{Namespace: [16]byte{1}, ProcessGeneration: "broker-1"},
		Ref:   fa.DeliveryRef{Stream: "analysis", Epoch: 9007199254740993, FrameID: ^uint64(0) - 1, Graph: fa.Digest{3}}},
		Source: "front", Surface: "native", BudgetNS: 100,
		Regions: []fa.Region{{ID: "face", BBox: [4]float64{1.5, 2, 5, 7}, PaddingRatio: 0.25}, {ID: "all", FullFrame: true}}}
}

func TestFrameRequestGoldenRoundtripAndOwnership(t *testing.T) {
	q := goldenFrameRequest()
	packet, err := in.EncodeFrameRequest(q)
	must(t, err)
	golden, err := exec.Command("python3", "-I", "-B", "frame_request_golden.py").Output()
	must(t, err)
	if hex.EncodeToString(packet) != strings.TrimSpace(string(golden)) {
		t.Fatal("Python/Go disagree")
	}
	decoded, err := in.DecodeFrameRequest(packet)
	must(t, err)
	if !reflect.DeepEqual(q, decoded) {
		t.Fatal("identity/geometry changed")
	}
	clear(packet)
	if !reflect.DeepEqual(q, decoded) {
		t.Fatal("request aliases receive buffer")
	}
	q.Regions = make([]fa.Region, 16)
	for i := range q.Regions {
		q.Regions[i] = fa.Region{ID: strings.Repeat("x", 63) + string(rune('A'+i)), FullFrame: true}
	}
	q.Source = strings.Repeat("s", 64)
	q.Surface = q.Source
	q.Frame.Fence.ProcessGeneration = q.Source
	q.Frame.Ref.Stream = q.Source
	packet, err = in.EncodeFrameRequest(q)
	must(t, err)
	if len(packet) >= 2200 {
		t.Fatalf("fixed representation bound: %d", len(packet))
	}
	decoded, err = in.DecodeFrameRequest(packet)
	must(t, err)
	if !reflect.DeepEqual(q, decoded) {
		t.Fatal("maximum request changed")
	}
}

func TestFrameRequestRejectsTruncationFlagsAndMalformedGeometry(t *testing.T) {
	packet, err := in.EncodeFrameRequest(goldenFrameRequest())
	must(t, err)
	for i := 0; i < len(packet); i++ {
		p := append([]byte(nil), packet[:i]...)
		if len(p) >= 8 {
			binary.BigEndian.PutUint16(p[6:8], uint16(len(p)))
		}
		q, err := in.DecodeFrameRequest(p)
		want(t, err, peer.ErrInvalid)
		if !reflect.DeepEqual(q, in.FrameRequest{}) {
			t.Fatal("partial request exposed")
		}
	}
	for _, offset := range []int{0, 4, 5} {
		p := append([]byte(nil), packet...)
		p[offset] = 255
		_, err := in.DecodeFrameRequest(p)
		want(t, err, peer.ErrInvalid)
	}
	p := append(append([]byte(nil), packet...), 0)
	binary.BigEndian.PutUint16(p[6:8], uint16(len(p)))
	_, err = in.DecodeFrameRequest(p)
	want(t, err, peer.ErrInvalid)
	for _, mutate := range []func(*in.FrameRequest){
		func(q *in.FrameRequest) { q.ID = [16]byte{} },
		func(q *in.FrameRequest) { q.Frame.Ref.FrameID = 0 },
		func(q *in.FrameRequest) { q.Frame.Ref.Epoch = 0 },
		func(q *in.FrameRequest) { q.Frame.Ref.Graph = fa.Digest{} },
		func(q *in.FrameRequest) { q.Source = "*" },
		func(q *in.FrameRequest) { q.BudgetNS = 0 },
		func(q *in.FrameRequest) { q.BudgetNS = in.MaxFrameRequestBudgetNS + 1 },
		func(q *in.FrameRequest) { q.Regions = nil },
		func(q *in.FrameRequest) { q.Regions = make([]fa.Region, 17) },
		func(q *in.FrameRequest) { q.Regions[1].ID = q.Regions[0].ID },
		func(q *in.FrameRequest) { q.Regions[0].BBox[0] = math.NaN() },
		func(q *in.FrameRequest) { q.Regions[0].BBox[0] = math.Inf(1) },
		func(q *in.FrameRequest) { q.Regions[0].BBox[0] = math.Copysign(0, -1) },
		func(q *in.FrameRequest) { q.Regions[0].BBox[2] = 0 },
		func(q *in.FrameRequest) { q.Regions[0].PaddingRatio = 2 },
		func(q *in.FrameRequest) { q.Regions[0].FullFrame = true },
	} {
		q := goldenFrameRequest()
		mutate(&q)
		p, err := in.EncodeFrameRequest(q)
		want(t, err, peer.ErrInvalid)
		if p != nil {
			t.Fatal("invalid request encoded")
		}
	}
}

func TestFramePacketUsesBrokerClockAndExistingACL(t *testing.T) {
	f, a, _, q := accessFixture(t)
	request := in.FrameRequest{ID: q.ID, Frame: q.Frame, Source: q.Source, Surface: q.Surface, Regions: q.Regions, BudgetNS: 100}
	packet, err := in.EncodeFrameRequest(request)
	must(t, err)
	result, err := a.ExtractPacket(context.Background(), principal(), packet)
	must(t, err)
	must(t, result.Visit(0, func(artifact fd.Artifact, pixels []byte) error {
		if len(pixels) != 24 {
			t.Fatal("wrong native ROI")
		}
		return fd.VerifyDomainProof(artifact.Proof, f.b.Fence())
	}))
	result.Close()
	_, err = a.ExtractPacket(context.Background(), peer.Principal{Generation: "forged"}, packet)
	want(t, err, peer.ErrUnauthorized)
	_, err = a.ExtractPacket(nil, principal(), packet)
	want(t, err, peer.ErrInvalid)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = a.ExtractPacket(ctx, principal(), packet)
	want(t, err, context.Canceled)
	request.BudgetNS = 101 // The source admits at most 100ns in this fixture.
	tooLong, err := in.EncodeFrameRequest(request)
	must(t, err)
	boundedResult, err := a.ExtractPacket(context.Background(), principal(), tooLong)
	must(t, err)
	boundedResult.Close()
	// Only the RPC upper bound is capped. The core's admitted limit remains
	// strict: a direct request exceeding it is still an invalid contract.
	unbounded := q
	unbounded.DeadlineNS = 201
	_, err = a.Extract(context.Background(), principal(), unbounded)
	want(t, err, fa.InvalidContract)
	request.BudgetNS = 100
	request.Frame.Ref.FrameID++ // Must not substitute the retained previous frame.
	missing, err := in.EncodeFrameRequest(request)
	must(t, err)
	missingResult, err := a.ExtractPacket(context.Background(), principal(), missing)
	if err == nil {
		must(t, missingResult.Visit(0, func(artifact fd.Artifact, pixels []byte) error {
			if len(pixels) != 0 || artifact.State == fa.Complete {
				t.Fatal("nearest frame substituted")
			}
			return nil
		}))
		missingResult.Close()
	}
	deadline, err := f.r.RequestDeadline(f.r.Fence(), 100)
	must(t, err)
	if deadline != 200 {
		t.Fatal("deadline not in runtime clock")
	}
	deadline, err = f.b.RequestDeadline(1000)
	must(t, err)
	if deadline != 200 {
		t.Fatal("client budget exceeded source copy admission", deadline)
	}
	deadline, err = f.b.RequestDeadline(50)
	must(t, err)
	if deadline != 150 {
		t.Fatal("short client deadline was extended", deadline)
	}
	_, err = f.b.RequestDeadline(0)
	want(t, err, fd.ErrInvalid)
	foreign := f.r.Fence()
	foreign.Namespace[0] ^= 1
	_, err = f.r.RequestDeadline(foreign, 100)
	want(t, err, fd.ErrDomainMismatch)
	f.c.now.Store(^uint64(0) - 50)
	_, err = a.ExtractPacket(context.Background(), principal(), packet)
	want(t, err, fa.DeadlineExceeded)
	f.c.now.Store(100)
	f.b.Close()
	_, err = a.ExtractPacket(context.Background(), principal(), packet)
	if err == nil {
		t.Fatal("source retirement served old packet")
	}
}

func FuzzFrameRequestCodec(f *testing.F) {
	packet, err := in.EncodeFrameRequest(goldenFrameRequest())
	if err != nil {
		f.Fatal(err)
	}
	f.Add(packet)
	f.Add([]byte("FAPQ"))
	f.Fuzz(func(t *testing.T, p []byte) {
		q, err := in.DecodeFrameRequest(p)
		if err != nil {
			if !reflect.DeepEqual(q, in.FrameRequest{}) {
				t.Fatal("partial request")
			}
			return
		}
		encoded, err := in.EncodeFrameRequest(q)
		must(t, err)
		if !bytes.Equal(p, encoded) {
			t.Fatal("noncanonical packet accepted")
		}
	})
}
