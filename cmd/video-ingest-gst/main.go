// Command video-ingest-gst is the portable GStreamer worker for video-broker.
package main

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	videoproviders "github.com/Josepavese/videosluice/internal/source"
	"github.com/Josepavese/videosluice/quality"
	"github.com/pion/rtp"
	"github.com/pion/rtp/codecs"
)

const (
	maxMuxPayload               = 16 * 1024 * 1024
	gstChildTerminationGrace    = 6 * time.Second
	gstChildProcessWaitFallback = 7 * time.Second
	gstChildKillWait            = 1 * time.Second
)

type muxWriter struct {
	mu  sync.Mutex
	out io.Writer
}

func (w *muxWriter) write(streamID byte, payload []byte) error {
	if len(payload) == 0 || len(payload) > maxMuxPayload {
		return fmt.Errorf("invalid mux payload size=%d", len(payload))
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	var header [5]byte
	header[0] = streamID
	binary.BigEndian.PutUint32(header[1:], uint32(len(payload)))
	if err := writeFull(w.out, header[:]); err != nil {
		return err
	}
	return writeFull(w.out, payload)
}

func main() {
	cfg := ingestConfig{}
	flag.StringVar(&cfg.sourceID, "source-id", "", "broker-owned canonical source identity")
	flag.Uint64Var(&cfg.mediaEpoch, "media-epoch", 0, "broker-owned media epoch")
	flag.StringVar(&cfg.sourceTransport, "source-transport", "", "capture source transport: v4l2 or libcamera")
	flag.StringVar(&cfg.device, "device", "", "explicit V4L2 capture device or libcamera selector")
	flag.StringVar(&cfg.inputFormat, "input-format", "", "canonical input format")
	flag.StringVar(&cfg.encodeProvider, "encode-provider", "", "live encoder provider")
	flag.StringVar(&cfg.archiveEncodeProvider, "archive-encode-provider", "", "archive encoder provider; defaults to live provider")
	flag.StringVar(&cfg.decodeProvider, "decode-provider", "", "canonical decoder provider")
	flag.StringVar(&cfg.orientation, "video-orientation", "normal", "canonical orientation")
	flag.StringVar(&cfg.aspectPolicy, "aspect-policy", "crop", "aspect normalization: exact, crop or letterbox")
	flag.IntVar(&cfg.captureWidth, "capture-width", 0, "capture width")
	flag.IntVar(&cfg.captureHeight, "capture-height", 0, "capture height")
	flag.IntVar(&cfg.captureFPS, "fps", 0, "capture FPS")
	flag.IntVar(&cfg.mainWidth, "output-width", 0, "live stream width")
	flag.IntVar(&cfg.mainHeight, "output-height", 0, "live stream height")
	flag.IntVar(&cfg.mainFPS, "main-fps", 0, "live stream FPS")
	flag.IntVar(&cfg.subWidth, "sub-width", 0, "analysis stream width")
	flag.IntVar(&cfg.subHeight, "sub-height", 0, "analysis stream height")
	flag.IntVar(&cfg.subFPS, "sub-fps", 0, "analysis stream FPS")
	flag.IntVar(&cfg.archiveWidth, "archive-width", 0, "archive stream width")
	flag.IntVar(&cfg.archiveHeight, "archive-height", 0, "archive stream height")
	flag.IntVar(&cfg.archiveFPS, "archive-fps", 0, "archive stream FPS")
	flag.StringVar(&cfg.liveCodec, "live-codec", "", "live codec: h265 or explicit h264 interoperability")
	flag.StringVar(&cfg.archiveCodec, "archive-codec", "h265", "archive codec")
	flag.StringVar(&cfg.archiveContent, "archive-content", "adaptive", "archive content policy")
	flag.IntVar(&cfg.liveBitrateKbps, "live-encode-bitrate-kbps", 4096, "live bitrate")
	flag.IntVar(&cfg.archiveBitrateKbps, "archive-encode-bitrate-kbps", 8192, "archive bitrate")
	flag.BoolVar(&cfg.liveEnabled, "live-enabled", false, "materialize the live output")
	flag.BoolVar(&cfg.analysisEnabled, "analysis-enabled", false, "materialize the analysis output")
	flag.BoolVar(&cfg.archiveEnabled, "archive-enabled", false, "materialize the archive output")
	flag.BoolVar(&cfg.mainActive, "main-active", true, "initial live publication state")
	flag.IntVar(&cfg.warmupSec, "warmup-sec", 0, "frames warmup interval")
	flag.BoolVar(&cfg.allowSWFallback, "allow-sw-fallback", false, "allow software codecs")
	flag.BoolVar(&cfg.qualityEnabled, "quality-monitor-enabled", false, "publish sampled NV12 quality")
	flag.IntVar(&cfg.qualityEvery, "quality-monitor-report-every", 900, "quality report frame interval")
	flag.IntVar(&cfg.qualityStride, "quality-monitor-sample-stride", 8, "quality sampling stride")
	flag.StringVar(&cfg.qualityState, "quality-monitor-state-path", "", "quality state path")
	flag.StringVar(&cfg.toneMappingMode, "tone-mapping-mode", "off", "off, global, local or auto")
	flag.StringVar(&cfg.toneAccelerator, "tone-mapping-accelerator", "auto", "auto, cpu or opencl")
	flag.IntVar(&cfg.toneTarget, "tone-mapping-target", 128, "target content luma")
	flag.IntVar(&cfg.toneHysteresis, "tone-mapping-hysteresis", 12, "luma hysteresis")
	flag.IntVar(&cfg.toneGridColumns, "tone-mapping-grid-columns", 16, "spatial grid columns")
	flag.IntVar(&cfg.toneGridRows, "tone-mapping-grid-rows", 9, "spatial grid rows")
	flag.IntVar(&cfg.toneStatisticsHz, "tone-mapping-statistics-hz", 8, "maximum statistics updates per second")
	flag.Float64Var(&cfg.toneMaxShadowGain, "tone-mapping-max-shadow-gain", 2.4, "maximum shadow gain")
	flag.Float64Var(&cfg.toneMaxHighlight, "tone-mapping-max-highlight-reduction", 0.2, "maximum highlight reduction")
	flag.Float64Var(&cfg.toneTemporalAlpha, "tone-mapping-temporal-alpha", 0.35, "temporal adaptation coefficient")
	flag.BoolVar(&cfg.toneNoiseGuard, "tone-mapping-noise-guard", true, "avoid amplifying non-informative noise")
	flag.Parse()
	if cfg.liveCodec != "h265" && cfg.liveCodec != "h264" {
		fmt.Fprintln(os.Stderr, "[video-ingest-gst] --live-codec must be exactly h265 or h264")
		os.Exit(2)
	}
	if cfg.archiveCodec != "h265" && cfg.archiveCodec != "h264" {
		fmt.Fprintln(os.Stderr, "[video-ingest-gst] --archive-codec must be exactly h265 or h264")
		os.Exit(2)
	}

	cfg.sourceTransport = strings.ToLower(strings.TrimSpace(cfg.sourceTransport))
	cfg.inputFormat = strings.ToLower(strings.TrimSpace(cfg.inputFormat))
	cfg.encodeProvider = strings.ToLower(strings.TrimSpace(cfg.encodeProvider))
	cfg.archiveEncodeProvider = strings.ToLower(strings.TrimSpace(cfg.archiveEncodeProvider))
	cfg.decodeProvider = strings.ToLower(strings.TrimSpace(cfg.decodeProvider))
	cfg.orientation = strings.ToLower(strings.TrimSpace(cfg.orientation))
	cfg.aspectPolicy = strings.ToLower(strings.TrimSpace(cfg.aspectPolicy))
	cfg.archiveContent = strings.ToLower(strings.TrimSpace(cfg.archiveContent))
	cfg.toneMappingMode = strings.ToLower(strings.TrimSpace(cfg.toneMappingMode))
	cfg.toneAccelerator = strings.ToLower(strings.TrimSpace(cfg.toneAccelerator))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, cfg); err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintf(os.Stderr, "[video-ingest-gst] %v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, cfg ingestConfig) error {
	cfg = normalizeIngestConfig(cfg)
	configureLocalGStreamerPluginPath()
	if _, err := exec.LookPath("gst-launch-1.0"); err != nil {
		return errors.New("gst-launch-1.0 is unavailable")
	}
	elements, err := resolveGStreamerElements(ctx, cfg)
	if err != nil {
		return err
	}
	emitGStreamerStageCapabilities(cfg, elements)
	return runIsolatedGStreamer(ctx, cfg, elements)
}

func configureGStreamerChildLifecycle(cmd *exec.Cmd) {
	if cmd == nil {
		return
	}
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.Setpgid = true
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return nil
		}
		pid := cmd.Process.Pid
		if err := syscall.Kill(-pid, syscall.SIGTERM); err != nil {
			if errors.Is(err, syscall.ESRCH) {
				return nil
			}
			return err
		}
		if err := syscall.Kill(-pid, syscall.SIGCONT); err != nil && !errors.Is(err, syscall.ESRCH) {
			return err
		}
		return nil
	}
	cmd.WaitDelay = gstChildProcessWaitFallback
}

func gstreamerProcessGroupExists(pgid int) bool {
	if pgid <= 0 {
		return false
	}
	err := syscall.Kill(-pgid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}

func emitGStreamerStageCapabilities(cfg ingestConfig, elements gstElements) {
	emit := func(stage, role, format, requested, element string) {
		if strings.TrimSpace(element) == "" {
			return
		}
		selected := gstreamerElementBackend(element)
		fallback := "none"
		if selected != requested {
			fallback = requested + "_unavailable"
		}
		fmt.Fprintf(os.Stderr, "[video-ingest-gst][stage-capability] stage=%s role=%s format=%s requested_backend=%s selected_backend=%s element=%s fallback=%s\n", stage, role, format, requested, selected, element, fallback)
	}
	emit("decode", "capture", cfg.inputFormat, cfg.decodeProvider, elements.decoder)
	emit("encode", "live", cfg.liveCodec, cfg.encodeProvider, elements.liveEncoder)
	if cfg.liveEnabled && !canUseNativeLive(cfg) {
		emit("resize", "live", "nv12", "gstreamer_video", "videoscale")
	}
	if cfg.archiveEnabled {
		emit("encode", "archive", cfg.archiveCodec, cfg.archiveEncodeProvider, elements.archiveEncoder)
		if archiveRequiresEncode(cfg) {
			emit("resize", "archive", "nv12", "gstreamer_video", "videoscale")
		}
	}
	if cfg.analysisEnabled {
		emit("resize", "analysis", "nv12", "gstreamer_video", "videoscale")
	}
}

func emitGStreamerRuntimeCapability(cfg ingestConfig, elements gstElements, fanoutControlMode string) {
	if fanoutControlMode != videoproviders.WorkerFanoutControlBranchTransactionV1 {
		fanoutControlMode = videoproviders.WorkerFanoutControlDisabled
	}
	archiveCodec := "none"
	archiveDelivery := gstreamerArchiveRuntimeMode(cfg, elements)
	archiveFPS := 0
	if cfg.archiveEnabled && archiveUsesNativeSource(cfg) {
		archiveCodec = cfg.inputFormat
		archiveFPS = cfg.captureFPS
	} else if cfg.archiveEnabled {
		archiveCodec = cfg.archiveCodec
		archiveFPS = cfg.archiveFPS
	}
	liveCodec := "none"
	liveControlMode := "disabled"
	if cfg.liveEnabled {
		liveCodec = cfg.liveCodec
		liveControlMode = "backend_static"
	}
	fmt.Fprintf(
		os.Stderr,
		"[video-ingest-gst][capability] live_enabled=%t analysis_enabled=%t archive_enabled=%t capture_codec=%s capture=%dx%d capture_fps=%d live_codec=%s archive_codec=%s archive_delivery=%s archive_fps=%d live_control_mode=%s fanout_control_mode=%s\n",
		cfg.liveEnabled,
		cfg.analysisEnabled,
		cfg.archiveEnabled,
		cfg.inputFormat,
		cfg.captureWidth,
		cfg.captureHeight,
		cfg.captureFPS,
		liveCodec,
		archiveCodec,
		archiveDelivery,
		archiveFPS,
		liveControlMode,
		fanoutControlMode,
	)
}

func gstreamerArchiveRuntimeMode(cfg ingestConfig, elements gstElements) string {
	if !cfg.archiveEnabled {
		return "disabled"
	}
	if archiveUsesNativeSource(cfg) {
		return videoproviders.WorkerFanoutRuntimeNativePassthrough
	}
	if gstreamerElementBackend(elements.archiveEncoder) == "software" {
		return videoproviders.WorkerFanoutRuntimeSoftwareEncode
	}
	if cfg.archiveContent == "source" {
		return videoproviders.WorkerFanoutRuntimeSourceHWEncode
	}
	return videoproviders.WorkerFanoutRuntimeProcessedHWEncode
}

func gstreamerElementBackend(element string) string {
	element = strings.ToLower(strings.TrimSpace(element))
	switch {
	case strings.HasPrefix(element, "x264"), strings.HasPrefix(element, "x265"),
		strings.HasPrefix(element, "avdec_"), element == "jpegdec", element == "openh264enc":
		return "software"
	case strings.HasPrefix(element, "mpp"):
		return "gstreamer_mpp"
	case strings.HasPrefix(element, "v4l2"):
		return "gstreamer_v4l2"
	case element == "videoscale":
		return "gstreamer_video"
	case element == "rgaconvert", element == "rga", element == "mppvideoconvert":
		return "gstreamer_mpp_resize"
	default:
		return "gstreamer_vendor"
	}
}

func configureLocalGStreamerPluginPath() {
	executable, err := os.Executable()
	if err != nil {
		return
	}
	directory := filepath.Dir(executable)
	current := strings.TrimSpace(os.Getenv("GST_PLUGIN_PATH"))
	if current != "" {
		directory += string(os.PathListSeparator) + current
	}
	_ = os.Setenv("GST_PLUGIN_PATH", directory)
}

func forwardRTP(
	r io.Reader,
	codec string,
	warmupUntil time.Time,
	emit func([]byte) error,
) error {
	if emit == nil {
		return errors.New("RTP frame emitter is required")
	}
	reader := bufio.NewReaderSize(r, 128*1024)
	var h264 codecs.H264Packet
	var h265 codecs.H265Depacketizer
	var accessUnit []byte
	var previousSequence uint16
	haveSequence := false
	for {
		var lengthBytes [2]byte
		if _, err := io.ReadFull(reader, lengthBytes[:]); err != nil {
			return err
		}
		length := int(binary.BigEndian.Uint16(lengthBytes[:]))
		if length < 12 {
			return fmt.Errorf("invalid RFC4571 RTP frame size=%d", length)
		}
		body := make([]byte, length)
		if _, err := io.ReadFull(reader, body); err != nil {
			return err
		}
		var packet rtp.Packet
		if err := packet.Unmarshal(body); err != nil {
			return err
		}
		if haveSequence && packet.SequenceNumber != previousSequence+1 {
			return fmt.Errorf(
				"RTP sequence discontinuity codec=%s expected=%d got=%d",
				codec,
				previousSequence+1,
				packet.SequenceNumber,
			)
		}
		previousSequence = packet.SequenceNumber
		haveSequence = true
		var payload []byte
		var err error
		if codec == "h265" {
			payload, err = h265.Unmarshal(packet.Payload)
		} else {
			payload, err = h264.Unmarshal(packet.Payload)
		}
		if err != nil {
			accessUnit = accessUnit[:0]
			return err
		}
		if len(payload) > 0 {
			if len(accessUnit)+len(payload) > maxMuxPayload {
				return errors.New("depacketized access unit exceeds broker limit")
			}
			accessUnit = append(accessUnit, payload...)
		}
		if packet.Marker {
			if len(accessUnit) > 0 && !time.Now().Before(warmupUntil) {
				if err := emit(accessUnit); err != nil {
					return err
				}
			}
			accessUnit = accessUnit[:0]
		}
	}
}

type lumaMetrics struct {
	samples     uint64
	luma        float64
	contentLuma float64
	lapVar      float64
	edge        float64
	blackRatio  float64
	whiteRatio  float64
	informative bool
	regions     [quality.SpatialRegionCount]lumaRegionMetrics
}

type lumaRegionAccumulator struct {
	samples        uint64
	blackSamples   uint64
	whiteSamples   uint64
	contentSamples uint64
	lumaSum        uint64
	contentLumaSum uint64
	lapSum         int64
	lapSquared     uint64
	edgeSum        uint64
}

type lumaRegionMetrics struct {
	samples     uint64
	luma        float64
	contentLuma float64
	lapVar      float64
	edge        float64
	blackRatio  float64
	whiteRatio  float64
	informative bool
}

type qualityRequest struct {
	response chan lumaMetrics
}

func forwardNV12(
	r io.Reader,
	cfg ingestConfig,
	warmupUntil time.Time,
	emit func([]byte) error,
) error {
	if emit == nil {
		return errors.New("NV12 frame emitter is required")
	}
	frameSize := cfg.subWidth * cfg.subHeight * 3 / 2
	if frameSize <= 0 || frameSize > maxMuxPayload {
		return fmt.Errorf("invalid NV12 frame size=%d", frameSize)
	}
	frame := make([]byte, frameSize)
	for {
		if _, err := io.ReadFull(r, frame); err != nil {
			return err
		}
		if !time.Now().Before(warmupUntil) {
			if err := emit(frame); err != nil {
				return err
			}
		}
	}
}

func forwardQualityCapture(
	ctx context.Context,
	sourceReader io.Reader,
	processedReader io.Reader,
	cfg ingestConfig,
	warmupUntil time.Time,
	qualityRequests chan<- qualityRequest,
	currentAnalysis func() *isolatedGStreamerRole,
) error {
	frameSize := cfg.captureWidth * cfg.captureHeight * 3 / 2
	if frameSize <= 0 || frameSize > maxMuxPayload {
		return fmt.Errorf("invalid capture NV12 frame size=%d", frameSize)
	}
	sourceFrame := make([]byte, frameSize)
	processedFrame := make([]byte, frameSize)
	var frameID uint64
	for {
		if _, err := io.ReadFull(sourceReader, sourceFrame); err != nil {
			return err
		}
		if _, err := io.ReadFull(processedReader, processedFrame); err != nil {
			return err
		}
		if time.Now().Before(warmupUntil) {
			continue
		}
		analysis := currentAnalysis()
		if analysis == nil {
			// Drain bounded capture pipes without measuring or waiting for an
			// intentionally detached role. Saved sampling intent stays enabled.
			continue
		}
		frameID += uint64(cfg.qualityEvery)
		source := measureNV12Luma(
			sourceFrame,
			cfg.captureWidth,
			cfg.captureHeight,
			cfg.qualityStride,
		)
		processed := measureNV12Luma(
			processedFrame,
			cfg.captureWidth,
			cfg.captureHeight,
			cfg.qualityStride,
		)
		response := make(chan lumaMetrics, 1)
		select {
		case qualityRequests <- qualityRequest{response: response}:
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
			if currentAnalysis() != analysis {
				continue
			}
			return errors.New("timed out requesting sub-stream quality sample")
		}
		var sub lumaMetrics
		select {
		case sub = <-response:
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
			if currentAnalysis() != analysis {
				continue
			}
			return errors.New("timed out receiving sub-stream quality sample")
		}
		if currentAnalysis() != analysis {
			continue
		}
		measuredConfig := cfg
		measuredConfig.subWidth, measuredConfig.subHeight = analysis.cfg.subWidth, analysis.cfg.subHeight
		if err := publishQualityState(measuredConfig, frameID, source, processed, sub); err != nil {
			return err
		}
	}
}

func publishQualityState(cfg ingestConfig, frameID uint64, source, processed, analysis lumaMetrics) error {
	state := quality.State{
		Schema: quality.Schema, TimestampUnixMS: time.Now().UnixMilli(), Frame: frameID,
		SampleStride: cfg.qualityStride,
		Source:       qualityMetrics(cfg.captureWidth, cfg.captureHeight, source),
		Processed:    qualityMetrics(cfg.captureWidth, cfg.captureHeight, processed),
		Analysis:     qualityMetrics(cfg.subWidth, cfg.subHeight, analysis),
		Capture:      quality.Capture{Format: cfg.inputFormat, Orientation: cfg.orientation},
	}
	return quality.WriteFile(cfg.qualityState, state)
}

func qualityMetrics(width, height int, metrics lumaMetrics) quality.Metrics {
	regions := make([]quality.Region, 0, quality.SpatialRegionCount)
	for index, region := range metrics.regions {
		regions = append(regions, quality.Region{
			X: index % quality.SpatialColumns, Y: index / quality.SpatialColumns,
			Samples: region.samples, Luma: region.luma, ContentLuma: region.contentLuma,
			LaplacianVariance: region.lapVar, Edge: region.edge,
			BlackRatio: region.blackRatio, WhiteRatio: region.whiteRatio,
			SignalInformative: region.informative,
		})
	}
	return quality.Metrics{
		Width: width, Height: height, Samples: metrics.samples, Luma: metrics.luma,
		ContentLuma: metrics.contentLuma, LaplacianVariance: metrics.lapVar, Edge: metrics.edge,
		BlackRatio: metrics.blackRatio, WhiteRatio: metrics.whiteRatio,
		SignalInformative: metrics.informative,
		Spatial:           quality.Spatial{Columns: quality.SpatialColumns, Rows: quality.SpatialRows, Regions: regions},
	}
}

func measureNV12Luma(nv12 []byte, width int, height int, stride int) lumaMetrics {
	if width < 3 || height < 3 || len(nv12) < width*height {
		return lumaMetrics{}
	}
	if stride <= 0 {
		stride = 8
	}
	var count uint64
	var blackSamples, whiteSamples, contentSamples uint64
	var lumaSum, contentLumaSum, lapSquared, edgeSum uint64
	var lapSum int64
	var regionAccumulators [quality.SpatialRegionCount]lumaRegionAccumulator
	for y := 1; y < height-1; y += stride {
		for x := 1; x < width-1; x += stride {
			index := y*width + x
			center := int(nv12[index])
			left := int(nv12[index-1])
			right := int(nv12[index+1])
			up := int(nv12[index-width])
			down := int(nv12[index+width])
			lap := 4*center - left - right - up - down
			regionX := minInt(x*quality.SpatialColumns/width, quality.SpatialColumns-1)
			regionY := minInt(y*quality.SpatialRows/height, quality.SpatialRows-1)
			region := &regionAccumulators[regionY*quality.SpatialColumns+regionX]
			if center <= 4 {
				blackSamples++
				region.blackSamples++
			}
			if center >= 251 {
				whiteSamples++
				region.whiteSamples++
			}
			if center > 4 {
				contentSamples++
				contentLumaSum += uint64(center)
				region.contentSamples++
				region.contentLumaSum += uint64(center)
			}
			lumaSum += uint64(center)
			lapSum += int64(lap)
			lapSquared += uint64(int64(lap) * int64(lap))
			edge := absInt(center-left) + absInt(center-up)
			edgeSum += uint64(edge)
			region.samples++
			region.lumaSum += uint64(center)
			region.lapSum += int64(lap)
			region.lapSquared += uint64(int64(lap) * int64(lap))
			region.edgeSum += uint64(edge)
			count++
		}
	}
	if count == 0 {
		return lumaMetrics{}
	}
	divisor := float64(count)
	meanLap := float64(lapSum) / divisor
	variance := float64(lapSquared)/divisor - meanLap*meanLap
	if variance < 0 {
		variance = 0
	}
	blackRatio := float64(blackSamples) / divisor
	whiteRatio := float64(whiteSamples) / divisor
	edge := float64(edgeSum) / divisor
	contentLuma := 0.0
	if contentSamples > 0 {
		contentLuma = float64(contentLumaSum) / float64(contentSamples)
	}
	metrics := lumaMetrics{
		samples: count, luma: float64(lumaSum) / divisor, contentLuma: contentLuma, lapVar: variance, edge: edge,
		blackRatio: blackRatio, whiteRatio: whiteRatio,
		informative: (blackRatio < 0.98 && whiteRatio < 0.98) || variance >= 1.0 || edge >= 0.25,
	}
	for index, accumulator := range regionAccumulators {
		metrics.regions[index] = finalizeLumaRegion(accumulator)
	}
	return metrics
}

func finalizeLumaRegion(accumulator lumaRegionAccumulator) lumaRegionMetrics {
	if accumulator.samples == 0 {
		return lumaRegionMetrics{}
	}
	divisor := float64(accumulator.samples)
	meanLap := float64(accumulator.lapSum) / divisor
	variance := float64(accumulator.lapSquared)/divisor - meanLap*meanLap
	if variance < 0 {
		variance = 0
	}
	contentLuma := 0.0
	if accumulator.contentSamples > 0 {
		contentLuma = float64(accumulator.contentLumaSum) / float64(accumulator.contentSamples)
	}
	blackRatio := float64(accumulator.blackSamples) / divisor
	whiteRatio := float64(accumulator.whiteSamples) / divisor
	edge := float64(accumulator.edgeSum) / divisor
	return lumaRegionMetrics{
		samples: accumulator.samples, luma: float64(accumulator.lumaSum) / divisor,
		contentLuma: contentLuma, lapVar: variance, edge: edge,
		blackRatio: blackRatio, whiteRatio: whiteRatio,
		informative: (blackRatio < 0.98 && whiteRatio < 0.98) || variance >= 1.0 || edge >= 0.25,
	}
}

func absInt(value int) int {
	if value < 0 {
		return -value
	}
	return value
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func writeFull(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		if err != nil {
			return err
		}
		if n <= 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}
