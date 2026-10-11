//go:build linux && cgo && nemesi_integration

package ingress_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	fa "github.com/Josepavese/videosluice/frameaccess"
	"github.com/Josepavese/videosluice/frameaccess/copyport"
	fd "github.com/Josepavese/videosluice/framedomain"
	in "github.com/Josepavese/videosluice/frameingress"
	pl "github.com/Josepavese/videosluice/framepeer/provider/linux"
	wire "github.com/Josepavese/videosluice/framewire"
	snapshotlinux "github.com/Josepavese/videosluice/framewire/provider/linux"
	"golang.org/x/sys/unix"
)

// Test-only capture, explicitly NOT part of runtime memory admission. Saves
// byte-exact C records before Go interpretation for independent Python proof.
type nativeOutputEvidence struct {
	in.SourceSnapshot
	bytes.Buffer
	headers []wire.Header
}
type nativeSurfaceEvidence struct {
	in.SourceSnapshot
	packets [][]byte // independent test oracle only, never a production queue
}

func (s *nativeSurfaceEvidence) StageInto(ctx context.Context, slot int, after uint64, p []byte) (int, uint64, error) {
	n, serial, err := s.SourceSnapshot.StageInto(ctx, slot, after, p)
	if err == nil {
		s.packets = append(s.packets, bytes.Clone(p[:n]))
	}
	return n, serial, err
}

func (s *nativeOutputEvidence) StageInto(ctx context.Context, slot int, after uint64, p []byte) (int, uint64, error) {
	n, serial, err := s.SourceSnapshot.StageInto(ctx, slot, after, p)
	if err == nil {
		var length [2]byte
		binary.BigEndian.PutUint16(length[:], uint16(wire.HeaderSize))
		s.Write(length[:])
		s.Write(p[:wire.HeaderSize])
		if string(p[:4]) == "VBCF" {
			h, e := wire.ParseHeader(p[:wire.HeaderSize])
			if e != nil {
				return 0, 0, e
			}
			s.headers = append(s.headers, h)
		}
		// Test-only cross-language wire oracle assembled from the exact RAM
		// snapshot. Production no longer transfers these per-frame chunks.
		id := sha256.Sum256(p[:wire.HeaderSize])
		payload := p[wire.HeaderSize:n]
		for offset := 0; offset < len(payload); {
			end := min(len(payload), offset+wire.MaxDataPacketBytes-wire.DataHeaderSize)
			storage := make([]byte, wire.MaxDataPacketBytes)
			used, e := wire.EncodeDataChunk(storage, id, offset, payload[offset:end])
			if e != nil {
				return 0, 0, e
			}
			binary.BigEndian.PutUint16(length[:], uint16(used))
			s.Write(length[:])
			s.Write(storage[:used])
			offset = end
		}
	}
	return n, serial, err
}

// Real C decoder adapter + atomic native/source-output mailboxes + pinned peer
// -> admitted receivers -> store -> ROI. SDK output and source/transform
// admission are fixtures. NOT camera/HW qualification, daemon/mux provisioning
// or a total native/kernel memory quote. The Python oracle gets test-only chunks
// reconstructed from exact snapshots, not runtime pixel socket transfers.
func TestNativeDecoderSurfaceToExactROI(t *testing.T) {
	root, err := filepath.Abs("../..")
	must(t, err)
	native := filepath.Join(root, "video-broker", "native")
	wireNative := filepath.Join(root, "video-broker/framewire", "native")
	peerNative := filepath.Join(root, "video-broker/framepeer", "provider", "linux", "native")
	binaryPath := filepath.Join(t.TempDir(), "decoder-envelope")
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	compile := exec.CommandContext(ctx, "cc", "-std=c11", "-O1", "-g", "-Wall", "-Wextra", "-Wpedantic", "-Werror", "-pthread",
		"-fsanitize=address,undefined", "-fno-omit-frame-pointer", "-fno-pie", "-no-pie",
		"-I"+filepath.Join(native, "tests", "stubs"), "-I"+native, "-I"+wireNative,
		"-I"+peerNative, "-I"+filepath.Join(root, "video-broker/framepeer", "native"),
		filepath.Join(native, "rdkx5_decoder_runtime.c"), filepath.Join(native, "tests", "stubs", "decoder_runtime.c"),
		filepath.Join(native, "frame_source_linux.c"),
		filepath.Join(native, "source_publisher.c"),
		filepath.Join(native, "tests", "decoder_envelope_test.c"), filepath.Join(wireNative, "causal_envelope.c"),
		filepath.Join(wireNative, "causal_chunks.c"), filepath.Join(wireNative, "causal_snapshot.c"), filepath.Join(wireNative, "atomic_snapshot.c"), filepath.Join(wireNative, "../provider/linux/native/source_mailbox.c"), filepath.Join(peerNative, "launch.c"), "-lcrypto", "-o", binaryPath)
	if output, err := compile.CombinedOutput(); err != nil {
		t.Fatalf("native integration compilation: %v\n%s", err, output)
	}
	pythonLibrary := filepath.Join(t.TempDir(), "libvideo_broker_frame_peer.so")
	// Each compiler invocation has its own preparation envelope. Neither can
	// consume the following invocation's allowance; runtime frame/peer budgets
	// below are separate and unchanged.
	peerCompileCtx, cancelPeerCompile := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancelPeerCompile()
	compilePeer := exec.CommandContext(peerCompileCtx, "cc", "-std=c11", "-O2", "-Wall", "-Wextra", "-Werror", "-fPIC", "-shared",
		"-I"+peerNative, "-I"+filepath.Join(root, "video-broker/framepeer", "native"),
		filepath.Join(native, "frame_peer_python.c"), filepath.Join(peerNative, "launch.c"), "-o", pythonLibrary, "-lcrypto", "-pthread")
	if output, err := compilePeer.CombinedOutput(); err != nil {
		t.Fatalf("Python native peer compilation: %v\n%s", err, output)
	}
	const base = uint64(1<<53) + 10
	for _, shape := range [][2]int{{16, 16}, {1280, 720}, {1920, 1080}} {
		for _, codec := range []string{"mjpeg", "h265", "h264"} {
			t.Run(fmt.Sprintf("%dx%d/%s", shape[0], shape[1], codec), func(t *testing.T) {
				width, height := shape[0], shape[1]
				size := width * height * 3 / 2
				selfFD, err := unix.PidfdOpen(os.Getpid(), 0)
				must(t, err)
				selfFile := os.NewFile(uintptr(selfFD), "native-decoder-test-controller")
				controller, err := pl.ImportProcessCapability(selfFile, pl.ProcessIdentity{PID: os.Getpid(), UID: uint32(os.Getuid()), GID: uint32(os.Getgid()), Generation: "native.source.broker"})
				if err != nil {
					selfFile.Close()
					t.Fatal(err)
				}
				t.Cleanup(func() { controller.Close() })
				// This process integration admits a real 1s RPC, unlike the 100ns
				// virtual-clock boundary fixtures. Production admission is unchanged.
				f := setupGeometryAuthorityCopyBudget(t, copyport.CPU{}, controller, width, height, uint64(time.Second))
				launch, err := pl.PrepareLaunch(controller)
				if launch != nil {
					t.Cleanup(func() { launch.Close() })
				}
				must(t, err)
				files, metadata, err := launch.Inherited()
				must(t, err)
				descriptor, err := f.i.SourceDescriptor()
				must(t, err)
				input := descriptor[:]
				var stderr bytes.Buffer
				runCtx, stop := context.WithTimeout(context.Background(), 10*time.Second)
				defer stop()
				serial, err := wire.NextMappingGeneration()
				must(t, err)
				mailbox, err := snapshotlinux.NewSourceMailbox(wire.Mapping{Namespace: f.b.Fence().Namespace, Generation: serial, Capacity: uint64(wire.HeaderSize + size)})
				must(t, err)
				defer mailbox.Close()
				mailboxFile, err := mailbox.Inherited()
				must(t, err)
				outputSerial, err := wire.NextMappingGeneration()
				must(t, err)
				outputCapacity := wire.HeaderSize + size/4
				outputMailbox, err := snapshotlinux.NewSourceMailbox(wire.Mapping{Namespace: f.b.Fence().Namespace, Generation: outputSerial, Capacity: uint64(outputCapacity)})
				must(t, err)
				defer outputMailbox.Close()
				outputFile, err := outputMailbox.Inherited()
				must(t, err)
				cmd := exec.CommandContext(runCtx, binaryPath, codec, hex.EncodeToString(metadata), f.b.Scope().Source, strconv.FormatUint(f.b.Scope().MediaEpoch, 10), strconv.FormatUint(serial, 10), strconv.FormatUint(outputSerial, 10), strconv.Itoa(outputCapacity))
				cmd.ExtraFiles, cmd.Stderr, cmd.WaitDelay = append(files[:], mailboxFile, outputFile), &stderr, time.Second
				commandInput, err := cmd.StdinPipe()
				must(t, err)
				defer commandInput.Close()
				must(t, cmd.Start())
				waited := false
				defer func() {
					stop()
					if !waited {
						cmd.Wait()
					}
					if t.Failed() {
						t.Logf("native stderr: %s", stderr.String())
					}
				}()
				must(t, launch.CloseInherited())
				must(t, mailbox.CloseInherited())
				must(t, outputMailbox.CloseInherited())
				generation := "native.decoder." + codec
				endpoint, err := launch.Deliver(runCtx, cmd.Process, generation, uint32(os.Getuid()), uint32(os.Getgid()), time.Second)
				must(t, err)
				baseline := f.r.Stats().ReservedResourceBytes
				receiver, err := in.NewSourceOutputReceiver(runCtx, f.i, endpoint, generation, uint64(time.Second), sha256.Sum256, 0)
				must(t, err)
				defer receiver.Close()
				charged := 2 * outputCapacity
				if f.r.Stats().ReservedResourceBytes != baseline+charged {
					t.Fatal("source workspace escaped admission")
				}
				must(t, endpoint.WriteCommand(runCtx, input, nil))
				var ready [8]byte
				n, principal, err := endpoint.Receive(runCtx, ready[:])
				must(t, err)
				if n != 8 || principal.Generation != generation || !bytes.Equal(ready[:], []byte{'V', 'B', 'N', 'S', 2, 2, 2, 0}) {
					t.Fatal("invalid mailbox handoff")
				}
				must(t, mailbox.ConfirmWriter())
				must(t, outputMailbox.ConfirmWriter())
				surfaces, err := in.NewSourceSurfaceReceiver(runCtx, f.i, endpoint, generation, f.c, 100)
				must(t, err)
				defer surfaces.Close()
				surfaceEvidence := &nativeSurfaceEvidence{SourceSnapshot: mailbox}
				var delivered []wire.Header
				var deliveredPixels [][]byte // test-only evidence, never retained by runtime consumer
				consume := func(_ context.Context, h wire.Header, p []byte) error {
					if h.Kind != wire.Delivery || h.OutputSequence != 0 || h.DeliveryID != uint64(len(delivered)+1) || h.Transform != f.tr.Digest() || len(p) != size/4 {
						return fmt.Errorf("native output was not bound to a broker delivery: %+v", h)
					}
					value := byte(31 + h.OriginSequence - base)
					w, ht := width/2, height/2
					for y := 0; y < ht; y++ {
						for x := 0; x < w; x++ {
							if p[y*w+x] != value+byte(y*14+x*6) {
								return fmt.Errorf("delivered luma has wrong origin/resize")
							}
						}
					}
					for y := 0; y < ht/2; y++ {
						for x := 0; x < w; x++ {
							if p[w*ht+y*w+x] != byte(128+y*10+(x/2)*4+x%2) {
								return fmt.Errorf("delivered chroma has wrong resize")
							}
						}
					}
					delivered = append(delivered, h)
					deliveredPixels = append(deliveredPixels, bytes.Clone(p))
					return nil
				}
				// The source has returned/overwritten its detached pixels after each
				// send; both distinct frames must remain correct in the broker store.
				var done [16]byte // Test control record, not an HD receive arena.
				n, principal, err = endpoint.Receive(runCtx, done[:])
				must(t, err)
				if string(done[:n]) != "SOURCE_DONE" || principal.Generation != generation {
					t.Fatal("unexpected third source frame")
				}
				// The native test barrier confirms both asynchronous publications.
				// Receiving an analysis output alone does not imply source readiness.
				must(t, surfaces.Poll(runCtx, surfaceEvidence))
				if surfaces.Stats().StagedFrames != 2 {
					t.Fatal("native mailbox lost exact fixture surfaces")
				}
				evidence := &nativeOutputEvidence{SourceSnapshot: outputMailbox}
				must(t, receiver.Poll(runCtx, evidence, consume))
				must(t, endpoint.WriteCommand(runCtx, []byte("done"), nil))
				_, err = commandInput.Write([]byte{1})
				must(t, err)
				waited = true
				if err := cmd.Wait(); err != nil {
					t.Fatalf("native decoder failed: %v\n%s", err, stderr.String())
				}
				if !strings.Contains(stderr.String(), "surfaces=2 unknown_origin_rejected=1") {
					t.Fatal("missing negative native proof")
				}
				stats := receiver.Stats()
				if stats.Outputs != 2 || stats.Failures != 0 || stats.Drops != 0 {
					t.Fatalf("unexpected HD source accounting: %+v", stats)
				}
				receiver.Close()
				must(t, receiver.WaitClosed(runCtx))
				surfaces.Close()
				must(t, surfaces.WaitClosed(runCtx))
				if f.r.Stats().ReservedResourceBytes != baseline {
					t.Fatal("joined source arena still charged")
				}
				if len(delivered) != 2 {
					t.Fatal("native source header count")
				}
				for index, sequence := range []uint64{base + 2, base + 1} {
					// Each independent consumer fixture owns the same ten-second
					// envelope. Native ASAN exit and other Python process imports
					// are not charged against this consumer's media operations.
					consumerCtx, stopConsumer := context.WithTimeout(context.Background(), 10*time.Second)
					defer stopConsumer()
					h := delivered[index]
					if h.OriginSequence != sequence || h.SourceEpoch != f.b.Scope().MediaEpoch || h.Namespace != f.b.Fence().Namespace || h.Process != f.b.Fence().ProcessGeneration {
						t.Fatal("native origin replaced")
					}
					ref := fd.DeliveryRef{Fence: f.b.Fence(), Ref: fa.DeliveryRef{Stream: h.Stream, Epoch: h.PublicationEpoch, FrameID: h.DeliveryID, Graph: h.Graph}}
					origin := fd.OriginRef{Fence: f.b.Fence(), Ref: fa.OriginRef{Source: h.Source, MediaEpoch: h.SourceEpoch, Sequence: sequence, Graph: h.Graph}}
					box := [4]float64{1, 1, 3, 3}
					if width >= 1280 {
						box = [4]float64{float64(width) / 8, float64(height) / 8, float64(width) * 3 / 8, float64(height) * 3 / 8}
					}
					region := fa.Region{ID: "head", BBox: box}
					q := fd.Request{ID: [16]byte{byte(index + 1)}, Frame: ref, Source: h.Source, Surface: "processed_full", Regions: []fa.Region{region}, DeadlineNS: 200}
					result, err := f.r.Extract(consumerCtx, q)
					must(t, err)
					x1, y1, x2, y2 := int(box[0])*2, int(box[1])*2, int(box[2])*2, int(box[3])*2
					must(t, result.Visit(0, func(a fd.Artifact, crop []byte) error {
						if a.State != fa.Complete || a.Proof.Core.Geometry != (fa.Geometry{Width: x2 - x1, Height: y2 - y1, Stride: x2 - x1}) {
							return fmt.Errorf("wrong native ROI geometry: %+v", a.Proof.Core.Geometry)
						}
						must(t, fd.VerifyDomainProof(a.Proof, f.b.Fence()))
						must(t, fa.VerifyProof(a.Proof.Core, crop, q.ID, ref.Ref, origin.Ref, f.tr, region, q.Surface, admitted(f.b.Scope()), size))
						var expected []byte
						for y := y1; y < y2; y++ {
							for x := x1; x < x2; x++ {
								expected = append(expected, byte(31+sequence-base)+byte(y*7+x*3))
							}
						}
						for y := y1 / 2; y < y2/2; y++ {
							for x := x1; x < x2; x++ {
								expected = append(expected, byte(128+y*5+x))
							}
						}
						if !bytes.Equal(crop, expected) {
							return fmt.Errorf("ROI pixels differ from exact native origin")
						}
						verifyNemesiConsumers(t, consumerCtx, root, pythonLibrary, controller, f, h, deliveredPixels[index], f.tr, q, a, crop, admitted(f.b.Scope()))
						return nil
					}))
					result.Close()
				}
				// Independently parse ORIGINAL C records: no Go re-encoding, no
				// float conversion, and no assumption that a chunk is a frame.
				parityCtx, stopParity := context.WithTimeout(context.Background(), 10*time.Second)
				defer stopParity()
				python := exec.CommandContext(parityCtx, "python3", "-B", "-c", `
import sys, hashlib, struct
from envelope import parse_header, parse, marshal
# Two full 1080p NV12 surfaces plus two half-dimension deliveries and headers.
# This is bounded test evidence, not production buffering or memory admission.
b = sys.stdin.buffer.read(8*1024*1024+1)
assert len(b) <= 8*1024*1024
layout = parse_header(b[:448]); pos = 448
def expected_planes(w,hgt,value,scale):
    # Independent exact-pixel oracle, not the SDK/broker transform. Build
    # the same modular coordinate pattern with bounded rows rather than
    # millions of interpreted per-pixel iterations on the ARM64 test board.
    # All pixels remain compared; no sampling, relaxed budget or NumPy import.
    ybase=bytes((x*3*scale)%256 for x in range(w))
    uvbase=bytes(((x//2)*2*scale+x%2)%256 for x in range(w))
    def plane(row,height,offset,step):
        rows={}
        for y in range(height):
            delta=(offset+y*step)%256
            if delta not in rows:
                rows[delta]=row.translate(bytes((v+delta)%256 for v in range(256)))
        return b''.join(rows[(offset+y*step)%256] for y in range(height))
    return plane(ybase,hgt,value,7*scale),plane(uvbase,hgt//2,128,5*scale)
for scale in (1,2):
    ycheck,uvcheck=expected_planes(34,40,33,scale)
    assert ycheck==bytes((33+y*7*scale+x*3*scale)%256 for y in range(40) for x in range(34))
    assert uvcheck==bytes((128+y*5*scale+(x//2)*2*scale+x%2)%256 for y in range(20) for x in range(34))
def record():
    global pos
    assert pos+2 <= len(b)
    n = struct.unpack_from('>H',b,pos)[0]; pos += 2
    assert 0 < n <= 16384 and pos+n <= len(b)
    result = b[pos:pos+n]; pos += n
    return result
for sequence, kind, output_seq in ((2**53+12,5,1),(2**53+11,5,2)):
    header=record(); h=parse_header(header)
    assert len(header)==448
    identity=hashlib.sha256(header).digest(); pixels=bytearray()
    while len(pixels)<h.payload_bytes:
        p=record()
        assert p[:6]==b'FAPD\x01\x00' and struct.unpack_from('>H',p,6)[0]==len(p)
        assert p[8:40]==identity and struct.unpack_from('>I',p,40)[0]==len(pixels)
        assert len(p)>44
        pixels.extend(p[44:])
        assert len(pixels)<=h.payload_bytes
    packet=header+pixels; parsed, pixels=parse(packet)
    assert parsed==h and h.origin_sequence==sequence and h.source_epoch==layout.source_epoch
    assert (h.namespace,h.process,h.source,h.graph)==(layout.namespace,layout.process,layout.source,layout.graph)
    assert h.kind==kind and h.output_sequence==output_seq and h.publication_epoch==h.delivery_id==0
    if kind==1: assert h.transform==layout.transform
    else: assert h.stream=='analysis' and h.width*2==layout.width and h.height*2==layout.height
    w,hgt=h.width,h.height; value=31+sequence-(2**53+10)
    scale=1 if kind==1 else 2
    yplane,uvplane=expected_planes(w,hgt,value,scale)
    assert pixels[:w*hgt]==yplane
    assert pixels[w*hgt:]==uvplane
    assert marshal(h,pixels)==packet
for sequence in (2**53+12,2**53+11):
    n=struct.unpack_from('>I',b,pos)[0]; pos+=4
    packet=b[pos:pos+n]; pos+=n
    h,pixels=parse(packet)
    assert h.kind==1 and h.origin_sequence==sequence and h.source_epoch==layout.source_epoch
    assert (h.namespace,h.process,h.source,h.graph,h.transform)==(layout.namespace,layout.process,layout.source,layout.graph,layout.transform)
    w,hgt=h.width,h.height; value=31+sequence-(2**53+10)
    yplane,uvplane=expected_planes(w,hgt,value,1)
    assert pixels[:w*hgt]==yplane
    assert pixels[w*hgt:]==uvplane
    assert marshal(h,pixels)==packet
assert pos==len(b)
print('NATIVE_SURFACE_PYTHON_PASS frames=2 outputs=2')
`)
				python.Dir = filepath.Join(root, "video-broker/framewire")
				var nativeBytes bytes.Buffer
				for _, packet := range surfaceEvidence.packets {
					var size [4]byte
					binary.BigEndian.PutUint32(size[:], uint32(len(packet)))
					nativeBytes.Write(size[:])
					nativeBytes.Write(packet)
				}
				python.Stdin = io.MultiReader(bytes.NewReader(input), bytes.NewReader(evidence.Bytes()), &nativeBytes)
				if output, err := python.CombinedOutput(); err != nil || !bytes.Contains(output, []byte("NATIVE_SURFACE_PYTHON_PASS frames=2")) {
					t.Fatalf("Python C/HD parity: %v\n%s", err, output)
				}
			})
		}
	}
}
