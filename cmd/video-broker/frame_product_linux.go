//go:build linux && cgo

package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"runtime"

	"github.com/Josepavese/videosluice/config"
	fa "github.com/Josepavese/videosluice/frameaccess"
	"github.com/Josepavese/videosluice/frameaccess/copyport"
	fd "github.com/Josepavese/videosluice/framedomain"
	in "github.com/Josepavese/videosluice/frameingress"
	peer "github.com/Josepavese/videosluice/framepeer"
	wire "github.com/Josepavese/videosluice/framewire"
	snapshotlinux "github.com/Josepavese/videosluice/framewire/provider/linux"
	"github.com/Josepavese/videosluice/internal/framehash"
	"github.com/Josepavese/videosluice/internal/source"
)

// One bounded product policy for incremental frame ownership. Decoder/encoder
// resources are still owned by the existing source graph, not by a second FAP
// worker. All pending/retiring source generations share these same limits.
const frameProductMaxSurface = 4 << 20

// Frame-path budgets. The former one-second end-to-end deadline sat below the
// multi-second storage stalls this hardware produces (archive finalize and
// retention bursts on the SD card), so a stall retired a healthy capture epoch
// and cost a tracker rebind and restart. Three seconds absorbs the measured
// one-to-ten second stalls while keeping a bound that still fails closed; the
// provider's five-second bootstrap stays the outer limit.
const (
	frameProductWorkBudgetNS = 3_000_000_000
	frameProductFrameAgeNS   = 3_000_000_000
	frameProductReceiveNS    = 3_000_000_000
)

func openProductFrameBootstrap(ctx context.Context, input frameLaunchInput) (frameBootstrapOwner, error) {
	log.Printf("[video-broker] event=frame_integrity_configured algorithm=sha256 large_payload_backend=%s small_payload_backend=go wire_contract=unchanged", framehash.Backend())
	return nativeFrameOpener(nativeFrameAdmission{
		limits: fd.Limits{MaxSources: 16, MaxStreams: 64, MaxSourceGenerations: 64,
			MaxMemoryBytes: 256 << 20, MaxSourceBytes: 96 << 20,
			MaxBuffers: 128, MaxBufferBytes: 16 << 20, MaxResources: 512, MaxResourceOperations: 16},
		bytes: 1 << 20, controlBytes: 1 << 20, workspaceBytes: 65536,
		transportBytes: 65536, maxGrants: 32,
	})(ctx, input)
}

func (b *nativeFrameBootstrap) Configure(cfg config.Config) error {
	if b == nil || b.resource == nil {
		return peer.ErrInvalid
	}
	b.run.Lock()
	defer b.run.Unlock()
	if b.started || b.configurationSet {
		return peer.ErrRetired
	}
	if err := config.Validate(cfg); err != nil {
		return err
	}
	access := cfg.Runtime.FrameAccess
	if access == nil {
		return errors.New("authenticated frame launch requires explicit frame_access configuration")
	}
	readers := make([]nativeFrameReaderPlan, 0, len(access.Readers)*2)
	requested := make(map[string]bool, len(access.Readers))
	for _, reader := range access.Readers {
		requested[reader.Stream] = true
		for _, delivery := range []bool{true, false} {
			plan := nativeFrameReaderPlan{Grant: reader.ID, Delivery: delivery,
				ListenerBytes: 65536, AcceptBytes: 1 << 20, MaxBytes: 2 * frameProductMaxSurface,
				MaxWorkNS: frameProductWorkBudgetNS, Path: reader.ROISocket}
			if delivery {
				plan.Stream, plan.Path = reader.Stream, reader.DeliverySocket
			}
			if err := plan.validate(); err != nil {
				return err
			}
			readers = append(readers, plan)
		}
	}
	// Configuration selects public streams, not source IDs. The actual capture
	// plan is supplied afresh by the source owner after routing and HW admission.
	b.admission.source = func(ctx context.Context, plan source.BrokerMuxPlan) (nativeFrameSourceAdmission, error) {
		if ctx == nil {
			return nativeFrameSourceAdmission{}, peer.ErrInvalid
		}
		if err := ctx.Err(); err != nil {
			return nativeFrameSourceAdmission{}, err
		}
		if !plan.AnalysisEnabled || !requested[plan.AnalysisStreamID] {
			return nativeFrameSourceAdmission{}, source.ErrFrameCaptureNotRequested
		}
		if runtime.GOARCH != "arm64" {
			return nativeFrameSourceAdmission{}, fmt.Errorf("native frame provider architecture: %w", errFrameProviderUnavailable)
		}
		return composeRDKFrameSource(plan)
	}
	b.admission.readers = readers
	b.configurationSet = true
	log.Printf("[video-broker] frame_product stage=configured readers=%d copy_provider=%s native_admission=source_owner scope=incremental_frame_resources", len(access.Readers), access.CopyProvider)
	return nil
}

// CPU copying is explicitly selected by configuration. This receipt attests
// only that portable NV12 copy implementation and this exact scope; it does
// not attest the capture decoder, accelerator, or a measured DMA ceiling.
type nativeFrameCopyAdmission struct{ graph fa.Digest }

func (a nativeFrameCopyAdmission) Attest(ctx context.Context, scope fa.Scope) (fa.CopyAdmission, error) {
	if ctx == nil || scope.Graph != a.graph || scope.MediaEpoch == 0 {
		return fa.CopyAdmission{}, fa.InvalidContract
	}
	if err := ctx.Err(); err != nil {
		return fa.CopyAdmission{}, err
	}
	encoded, err := json.Marshal(struct {
		Scope    fa.Scope
		Provider string
	}{scope, "cpu_nv12_copy"})
	if err != nil {
		return fa.CopyAdmission{}, err
	}
	return fa.CopyAdmission{Provider: "cpu_nv12_copy", MediaEpoch: scope.MediaEpoch, Graph: scope.Graph,
		Evidence: sha256.Sum256(encoded), Attested: true}, nil
}

func composeRDKFrameSource(plan source.BrokerMuxPlan) (nativeFrameSourceAdmission, error) {
	var a nativeFrameSourceAdmission
	if (plan.Transport != source.TransportV4L2 && plan.Transport != source.TransportV4L2UVC) ||
		plan.WorkerProvider != "native_sp" || plan.DecodeProvider != "native_sp" || !plan.RequireHWCodec || plan.AllowSWFallback ||
		!plan.AnalysisEnabled || !frameReaderToken(plan.SourceID) || !frameReaderToken(plan.AnalysisStreamID) {
		return a, fmt.Errorf("native frame source profile is not admitted: transport=%s worker=%s decode=%s require_hw=%t software_fallback=%t analysis=%t source_id_valid=%t stream_id_valid=%t: %w",
			plan.Transport, plan.WorkerProvider, plan.DecodeProvider, plan.RequireHWCodec, plan.AllowSWFallback,
			plan.AnalysisEnabled, frameReaderToken(plan.SourceID), frameReaderToken(plan.AnalysisStreamID), errFrameProviderUnavailable)
	}
	// Tone processing is downstream of the immutable source publication. The
	// native worker now checks OpenCL drain/release and retires with an explicit
	// unconfirmed status on failure; it must not be certified by process exit
	// alone. Codec HW admission and the configured tone provider remain intact.
	switch plan.CaptureFormat {
	case "h265":
		if !plan.H265DecodeHWAvailable {
			return a, errFrameProviderUnavailable
		}
	case "h264":
		if !plan.H264DecodeHWAvailable {
			return a, errFrameProviderUnavailable
		}
	case "mjpeg":
		if !plan.MJPEGDecodeHWAvailable {
			return a, errFrameProviderUnavailable
		}
	default:
		return a, fmt.Errorf("native frame source codec: %w", errFrameProviderUnavailable)
	}
	geometry := fa.Geometry{Width: plan.CaptureWidth, Height: plan.CaptureHeight, Stride: plan.CaptureWidth}
	n, err := geometry.Bytes()
	if err != nil || n > frameProductMaxSurface {
		return a, fa.CapacityRejected
	}
	transform, err := nativeNV12Transform(geometry, plan.SubWidth, plan.SubHeight, plan.VideoOrientation, plan.CaptureAspectPolicy)
	if err != nil {
		return a, err
	}
	deliveryBytes := plan.SubWidth * plan.SubHeight * 3 / 2
	if deliveryBytes > frameProductMaxSurface {
		return a, fa.CapacityRejected
	}
	identity := plan
	identity.MediaEpoch, identity.MainActive = 0, false
	encoded, err := json.Marshal(identity)
	if err != nil {
		return a, err
	}
	graph := fa.Digest(sha256.Sum256(encoded))
	full := fa.Transform{Full: geometry, Crop: fa.Rect{X2: geometry.Width, Y2: geometry.Height},
		ResizedWidth: geometry.Width, ResizedHeight: geometry.Height, DeliveryWidth: geometry.Width, DeliveryHeight: geometry.Height}
	a = nativeFrameSourceAdmission{
		Layout: fd.Layout{Source: plan.SourceID, Graph: graph, Streams: []string{plan.AnalysisStreamID}, Surface: "source_decoded",
			Geometry: geometry, DecodedWidth: geometry.Width, DecodedHeight: geometry.Height},
		Limits: fa.Limits{MaxFrames: 16, MaxBindings: 256, PoolBytes: 18 * n, MaxFrameBytes: n,
			MaxArtifactBytes: 2 * n, MaxOutputPerRequest: n, MaxFullFrameBytes: n, MaxRegions: 16, MaxResults: 16,
			MaxAgeNS: frameProductFrameAgeNS, MaxCopyNS: 100_000_000},
		Copier: copyport.CPU{}, Admission: nativeFrameCopyAdmission{graph: graph}, Policy: fa.CopyPolicy{AllowSoftware: true},
		// One additional MiB covers the native worker's synchronous kernel hash
		// workspace. Reserve conservatively even if native selection uses OpenSSL.
		// The peer provider bounds process bootstrap at five seconds; the frame
		// receive budget below is the same three-second bound as the reader and
		// artifact budgets, so a stall is reported once and not as three layers
		// of timeouts.
		// Detached capture input/output, two immutable full-source slots and
		// two immutable delivery slots. Admit the second publisher's pixels and
		// bounded thread/stack/eventfd state before creating it on first frame.
		SurfaceDigest: full.Digest(), CaptureBytes: 4*n + 2*deliveryBytes + (5 << 20), StartupNS: 5_000_000_000, ReceiveNS: frameProductReceiveNS, CopyNS: 100_000_000,
		Deliveries: map[string]in.DeliveryLayout{plan.AnalysisStreamID: {Codec: wire.NV12,
			StrideY: uint32(plan.SubWidth), StrideUV: uint32(plan.SubWidth), MaxPayload: uint32(deliveryBytes), Transform: transform}},
	}
	a.DeliveryAllocator, err = snapshotlinux.New(snapshotlinux.Limits{Bytes: 3*uint64(wire.HeaderSize+deliveryBytes) + 8192 + (2 << 20), Allocations: 1, Operations: 2})
	if err != nil {
		return nativeFrameSourceAdmission{}, err
	}
	mailboxBytes, err := snapshotlinux.SourceMailboxBytes(uint64(wire.HeaderSize + n))
	if err != nil {
		return nativeFrameSourceAdmission{}, err
	}
	a.MailboxBytes = int(mailboxBytes) + 4096
	outputMailboxBytes, err := snapshotlinux.SourceMailboxBytes(uint64(wire.HeaderSize + deliveryBytes))
	if err != nil {
		return nativeFrameSourceAdmission{}, err
	}
	a.OutputCapacity = wire.HeaderSize + deliveryBytes
	a.OutputMailboxBytes = int(outputMailboxBytes) + 4096
	a.Mailbox = func(mapping wire.Mapping) (nativeSurfaceMailbox, error) {
		m, err := snapshotlinux.NewSourceMailbox(mapping)
		if m == nil {
			return nil, err
		}
		return m, err
	}
	a.Execution = func(ctx context.Context) (source.FrameExecution, error) {
		if ctx == nil {
			return nil, peer.ErrInvalid
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return source.TrackAdmittedNativeCapture(plan.V4L2BridgePath)
	}
	return a, nil
}

// Mirrors the selected worker's single native NV12 spatial operation. The
// worker publishes its independently computed digest; a mismatch is rejected
// before delivery. No resize is performed or reconstructed in the broker.
func nativeNV12Transform(full fa.Geometry, width, height int, orientation, aspect string) (fa.Transform, error) {
	t := fa.Transform{Full: full, Crop: fa.Rect{X2: full.Width, Y2: full.Height},
		ResizedWidth: width, ResizedHeight: height, DeliveryWidth: width, DeliveryHeight: height}
	if full.Width < 2 || full.Height < 2 || width < 2 || height < 2 || width > 8192 || height > 8192 {
		return t, fa.InvalidContract
	}
	switch orientation {
	case "normal":
	case "rotate_180":
		t.Rotation = 180
	case "mirror_horizontal":
		t.Mirror = true
	case "mirror_vertical":
		t.Rotation, t.Mirror = 180, true
	default:
		return t, fa.TransformMismatch
	}
	even := func(n int) int { return max(2, n & ^1) }
	sourceRatio, targetRatio := int64(full.Width)*int64(height), int64(width)*int64(full.Height)
	switch aspect {
	case "exact":
	case "crop":
		if sourceRatio > targetRatio {
			w := even(full.Height * width / height)
			t.Crop.X1 = ((full.Width - w) / 2) &^ 1
			t.Crop.X2 = t.Crop.X1 + w
		}
		if sourceRatio < targetRatio {
			h := even(full.Width * height / width)
			t.Crop.Y1 = ((full.Height - h) / 2) &^ 1
			t.Crop.Y2 = t.Crop.Y1 + h
		}
	case "letterbox":
		if sourceRatio > targetRatio {
			t.ResizedHeight = even(width * full.Height / full.Width)
			t.PadTop = ((height - t.ResizedHeight) / 2) &^ 1
		}
		if sourceRatio < targetRatio {
			t.ResizedWidth = even(height * full.Width / full.Height)
			t.PadLeft = ((width - t.ResizedWidth) / 2) &^ 1
		}
	default:
		return t, fa.TransformMismatch
	}
	return t, t.Validate()
}
