package main

import (
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
)

type gstreamerBusPaths struct {
	raw       string
	sourceRaw string
	native    string
}

func validateGStreamerWorkerConfig(cfg ingestConfig) error {
	cfg = normalizeIngestConfig(cfg)
	if strings.TrimSpace(cfg.aspectPolicy) == "" {
		cfg.aspectPolicy = "crop"
	}
	if !cfg.liveEnabled && !cfg.analysisEnabled && !cfg.archiveEnabled {
		return errors.New("video ingest requires at least one enabled output")
	}
	if (cfg.sourceTransport == "v4l2" && cfg.device == "") ||
		cfg.captureWidth <= 0 || cfg.captureHeight <= 0 || cfg.captureFPS <= 0 ||
		(cfg.liveEnabled && (cfg.mainWidth <= 0 || cfg.mainHeight <= 0 || cfg.mainFPS <= 0)) ||
		(cfg.analysisEnabled && (cfg.subWidth <= 0 || cfg.subHeight <= 0 || cfg.subFPS <= 0)) ||
		(cfg.archiveEnabled && (cfg.archiveWidth <= 0 || cfg.archiveHeight <= 0 || cfg.archiveFPS <= 0)) {
		return errors.New("invalid video ingest dimensions or frame rate")
	}
	if cfg.captureWidth%2 != 0 || cfg.captureHeight%2 != 0 ||
		(cfg.liveEnabled && (cfg.mainWidth%2 != 0 || cfg.mainHeight%2 != 0)) ||
		(cfg.analysisEnabled && (cfg.subWidth%2 != 0 || cfg.subHeight%2 != 0)) ||
		(cfg.archiveEnabled && (cfg.archiveWidth%2 != 0 || cfg.archiveHeight%2 != 0)) {
		return errors.New("NV12 video dimensions must be even")
	}
	if (cfg.liveEnabled && cfg.mainFPS > cfg.captureFPS) ||
		(cfg.analysisEnabled && cfg.subFPS > cfg.captureFPS) ||
		(cfg.archiveEnabled && cfg.archiveFPS > cfg.captureFPS) {
		return errors.New("output frame rate cannot exceed capture frame rate")
	}
	if cfg.archiveEnabled && cfg.archiveCodec != "h264" && cfg.archiveCodec != "h265" {
		return fmt.Errorf("unsupported archive codec %q", cfg.archiveCodec)
	}
	if cfg.liveEnabled && cfg.liveCodec != "h264" && cfg.liveCodec != "h265" {
		return fmt.Errorf("unsupported live codec %q", cfg.liveCodec)
	}
	if cfg.archiveEnabled && cfg.archiveContent != "source" &&
		cfg.archiveContent != "processed" && cfg.archiveContent != "adaptive" {
		return fmt.Errorf("unsupported archive content %q", cfg.archiveContent)
	}
	switch cfg.orientation {
	case "normal", "rotate_180", "mirror_horizontal", "mirror_vertical":
	default:
		return fmt.Errorf("unsupported video orientation %q", cfg.orientation)
	}
	switch cfg.aspectPolicy {
	case "exact":
		if (cfg.liveEnabled && !sameVideoAspect(cfg.captureWidth, cfg.captureHeight, cfg.mainWidth, cfg.mainHeight)) ||
			(cfg.analysisEnabled && !sameVideoAspect(cfg.captureWidth, cfg.captureHeight, cfg.subWidth, cfg.subHeight)) ||
			(cfg.archiveEnabled && archiveRequiresEncode(cfg) &&
				!sameVideoAspect(cfg.captureWidth, cfg.captureHeight, cfg.archiveWidth, cfg.archiveHeight)) {
			return errors.New("exact aspect policy rejects capture/output aspect mismatch")
		}
	case "crop", "letterbox":
	default:
		return fmt.Errorf("unsupported aspect policy %q", cfg.aspectPolicy)
	}
	if cfg.qualityEnabled && (cfg.qualityEvery <= 0 ||
		cfg.qualityStride <= 0 || strings.TrimSpace(cfg.qualityState) == "") {
		return errors.New("quality monitor requires positive cadence/stride and a state path")
	}
	return nil
}

func buildGStreamerCaptureArgs(
	cfg ingestConfig,
	elements gstElements,
	buses gstreamerBusPaths,
) ([]string, error) {
	cfg = normalizeIngestConfig(cfg)
	if strings.TrimSpace(cfg.aspectPolicy) == "" {
		cfg.aspectPolicy = "crop"
	}
	if err := validateGStreamerWorkerConfig(cfg); err != nil {
		return nil, err
	}
	needsRaw := rawProcessingRequired(cfg)
	needsNative := gstreamerNativeBusRequired(cfg)
	needsSourceRaw := gstreamerSourceRawBusRequired(cfg)
	if needsRaw && (cfg.inputFormat == "h264" || cfg.inputFormat == "h265" || cfg.inputFormat == "mjpeg") &&
		strings.TrimSpace(elements.decoder) == "" {
		return nil, errors.New("capture decode element is required for the raw GStreamer bus")
	}
	if cfg.toneMappingMode != "off" && strings.TrimSpace(elements.toneMapper) == "" {
		return nil, errors.New("tone-mapping element is required for the raw GStreamer bus")
	}
	if needsRaw && !canonicalPrivateBusPath(buses.raw) {
		return nil, errors.New("raw GStreamer bus path is not absolute and canonical")
	}
	if needsSourceRaw && !canonicalPrivateBusPath(buses.sourceRaw) {
		return nil, errors.New("source raw GStreamer bus path is not absolute and canonical")
	}
	if needsNative && !canonicalPrivateBusPath(buses.native) {
		return nil, errors.New("native GStreamer bus path is not absolute and canonical")
	}
	args := []string{"-q"}
	switch cfg.sourceTransport {
	case "v4l2":
		args = append(args, "v4l2src", "device="+cfg.device, "io-mode=2", "do-timestamp=true", "!")
	case "libcamera":
		args = append(args, "libcamerasrc")
		if selector := strings.TrimSpace(cfg.device); selector != "" {
			args = append(args, "camera-name="+selector)
		}
		args = append(args, "do-timestamp=true", "!")
	default:
		return nil, fmt.Errorf("unsupported source transport %q", cfg.sourceTransport)
	}
	captureCaps := fmt.Sprintf("video/x-%s,width=%d,height=%d,framerate=%d/1",
		gstInputMediaType(cfg.inputFormat), cfg.captureWidth, cfg.captureHeight, cfg.captureFPS)
	if cfg.sourceTransport == "libcamera" {
		captureCaps = fmt.Sprintf("video/x-raw,width=%d,height=%d,framerate=%d/1",
			cfg.captureWidth, cfg.captureHeight, cfg.captureFPS)
	} else if cfg.inputFormat == "yuyv" {
		captureCaps = fmt.Sprintf("video/x-raw,format=YUY2,width=%d,height=%d,framerate=%d/1",
			cfg.captureWidth, cfg.captureHeight, cfg.captureFPS)
	} else if cfg.inputFormat == "nv12" {
		captureCaps = fmt.Sprintf("video/x-raw,format=NV12,width=%d,height=%d,framerate=%d/1",
			cfg.captureWidth, cfg.captureHeight, cfg.captureFPS)
	}
	args = append(args, captureCaps, "!")

	switch cfg.inputFormat {
	case "h264", "h265":
		args = append(args,
			cfg.inputFormat+"parse", "config-interval=-1", "!",
			"video/x-"+cfg.inputFormat+",stream-format=byte-stream,alignment=au", "!",
			"tee", "name=native",
		)
		if needsNative {
			args = append(args, gstreamerSharedBusSink("native.", buses.native, 64<<20, false)...)
		}
		if needsRaw {
			args = append(args, "native.", "!", "queue", "max-size-buffers=4", "leaky=downstream", "!", elements.decoder, "!")
			args = append(args, decoderOutputConversion(elements.decoder)...)
			args = append(args, rawNV12Fanout(cfg, elements)...)
		}
	case "mjpeg":
		args = append(args, elements.decoder, "!", "videoconvert", "!")
		args = append(args, rawNV12Fanout(cfg, elements)...)
	case "yuyv", "nv12":
		args = append(args, "videoconvert", "!")
		args = append(args, rawNV12Fanout(cfg, elements)...)
	default:
		return nil, fmt.Errorf("unsupported capture format %q", cfg.inputFormat)
	}
	if needsRaw {
		frameBytes := cfg.captureWidth * cfg.captureHeight * 3 / 2
		args = append(args, gstreamerSharedBusSink("raw.", buses.raw, frameBytes*4, true)...)
		if needsSourceRaw {
			args = append(args, gstreamerSharedBusSink("source.", buses.sourceRaw, frameBytes*4, true)...)
		}
	}
	if cfg.qualityEnabled {
		args = append(args, qualityNV12Branch(cfg, "source.", 3)...)
		args = append(args, qualityNV12Branch(cfg, "raw.", 4)...)
	}
	return args, nil
}

func buildGStreamerRoleArgs(
	cfg ingestConfig,
	elements gstElements,
	buses gstreamerBusPaths,
	role string,
) ([]string, error) {
	cfg = normalizeIngestConfig(cfg)
	if strings.TrimSpace(cfg.aspectPolicy) == "" {
		cfg.aspectPolicy = "crop"
	}
	if err := validateGStreamerWorkerConfig(cfg); err != nil {
		return nil, err
	}
	var busPath string
	var caps string
	var branch []string
	switch role {
	case "live":
		if !cfg.liveEnabled {
			return nil, errors.New("live role is disabled")
		}
		if canUseNativeLive(cfg) {
			busPath = buses.native
			caps = nativeBusCaps(cfg)
			branch = nativeEncodedRoleBranch(cfg.liveCodec, 96)
		} else {
			if strings.TrimSpace(elements.liveEncoder) == "" {
				return nil, errors.New("live encoder element is required")
			}
			busPath = buses.raw
			caps = rawBusCaps(cfg)
			branch = encodedLiveRoleBranch(cfg, elements.liveEncoder)
		}
	case "analysis":
		if !cfg.analysisEnabled {
			return nil, errors.New("analysis role is disabled")
		}
		busPath = buses.raw
		caps = rawBusCaps(cfg)
		branch = analysisRoleBranch(cfg)
	case "archive":
		if !cfg.archiveEnabled {
			return nil, errors.New("archive role is disabled")
		}
		if archiveUsesNativeSource(cfg) {
			busPath = buses.native
			caps = nativeBusCaps(cfg)
			branch = nativeEncodedRoleBranch(cfg.inputFormat, 98)
		} else {
			if strings.TrimSpace(elements.archiveEncoder) == "" {
				return nil, errors.New("archive encoder element is required")
			}
			busPath = buses.raw
			if gstreamerArchiveUsesSourceRawBus(cfg) {
				busPath = buses.sourceRaw
			}
			caps = rawBusCaps(cfg)
			branch = encodedArchiveRoleBranch(cfg, elements.archiveEncoder)
		}
	default:
		return nil, fmt.Errorf("unsupported GStreamer role %q", role)
	}
	if !canonicalPrivateBusPath(busPath) {
		return nil, fmt.Errorf("%s GStreamer bus path is not absolute and canonical", role)
	}
	args := []string{
		"-q", "shmsrc", "socket-path=" + busPath, "is-live=true", "do-timestamp=true", "!", caps, "!",
	}
	return append(args, branch...), nil
}

func gstreamerNativeBusRequired(cfg ingestConfig) bool {
	cfg = normalizeIngestConfig(cfg)
	return cfg.inputFormat == "h264" || cfg.inputFormat == "h265"
}

func gstreamerSourceRawBusRequired(cfg ingestConfig) bool {
	cfg = normalizeIngestConfig(cfg)
	return rawProcessingRequired(cfg) && cfg.toneMappingMode != "off"
}

func gstreamerArchiveUsesSourceRawBus(cfg ingestConfig) bool {
	cfg = normalizeIngestConfig(cfg)
	return cfg.archiveEnabled && !archiveUsesNativeSource(cfg) &&
		cfg.archiveContent == "source" && cfg.toneMappingMode != "off"
}

func canonicalPrivateBusPath(path string) bool {
	clean := filepath.Clean(path)
	return filepath.IsAbs(clean) && clean != "/" && clean == path
}

func rawBusCaps(cfg ingestConfig) string {
	return fmt.Sprintf("video/x-raw,format=NV12,width=%d,height=%d,framerate=%d/1",
		cfg.captureWidth, cfg.captureHeight, cfg.captureFPS)
}

func nativeBusCaps(cfg ingestConfig) string {
	return fmt.Sprintf("video/x-%s,stream-format=byte-stream,alignment=au,width=%d,height=%d,framerate=%d/1",
		cfg.inputFormat, cfg.captureWidth, cfg.captureHeight, cfg.captureFPS)
}

func gstreamerSharedBusSink(source, path string, size int, leaky bool) []string {
	queue := []string{source, "!", "queue", "max-size-buffers=4"}
	if leaky {
		queue = append(queue, "leaky=downstream")
	}
	return append(queue,
		"!", "shmsink", "socket-path="+path,
		"shm-size="+strconv.Itoa(size), "perms=416",
		"wait-for-connection=false", "sync=false", "async=false",
	)
}

func analysisRoleBranch(cfg ingestConfig) []string {
	branch := []string{"queue", "max-size-buffers=2", "leaky=downstream", "!", "videorate", "drop-only=true", "!"}
	branch = append(branch, aspectScaleElements(
		cfg.captureWidth, cfg.captureHeight, cfg.subWidth, cfg.subHeight, cfg.aspectPolicy)...)
	return append(branch,
		fmt.Sprintf("video/x-raw,format=NV12,width=%d,height=%d,framerate=%d/1",
			cfg.subWidth, cfg.subHeight, cfg.subFPS),
		"!", "fdsink", "fd=3", "sync=false", "async=false",
	)
}

func encodedLiveRoleBranch(cfg ingestConfig, encoder string) []string {
	branch := []string{"queue", "max-size-buffers=4", "leaky=downstream", "!", "videorate", "drop-only=true", "!"}
	branch = append(branch, aspectScaleElements(
		cfg.captureWidth, cfg.captureHeight, cfg.mainWidth, cfg.mainHeight, cfg.aspectPolicy)...)
	if cfg.orientation != "normal" {
		branch = append(branch, "videoflip", "method="+gstOrientation(cfg.orientation), "!")
	}
	branch = append(branch,
		fmt.Sprintf("video/x-raw,format=NV12,width=%d,height=%d,framerate=%d/1",
			cfg.mainWidth, cfg.mainHeight, cfg.mainFPS),
		"!",
	)
	branch = append(branch, encoderInputConversion(encoder)...)
	branch = append(branch, encoderArgs(encoder, cfg.liveCodec, cfg.liveBitrateKbps, cfg.mainFPS)...)
	branch = append(branch,
		"!", cfg.liveCodec+"parse", "config-interval=-1", "!",
		"video/x-"+cfg.liveCodec+",stream-format=byte-stream,alignment=au", "!",
	)
	return append(branch, rtpOutput(cfg.liveCodec, 96, false)...)
}

func encodedArchiveRoleBranch(cfg ingestConfig, encoder string) []string {
	branch := []string{"queue", "max-size-buffers=32", "!", "videorate", "drop-only=true", "!"}
	branch = append(branch, aspectScaleElements(
		cfg.captureWidth, cfg.captureHeight, cfg.archiveWidth, cfg.archiveHeight, cfg.aspectPolicy)...)
	if cfg.archiveContent != "source" && cfg.orientation != "normal" {
		branch = append(branch, "videoflip", "method="+gstOrientation(cfg.orientation), "!")
	}
	branch = append(branch,
		fmt.Sprintf("video/x-raw,format=NV12,width=%d,height=%d,framerate=%d/1",
			cfg.archiveWidth, cfg.archiveHeight, cfg.archiveFPS),
		"!",
	)
	branch = append(branch, encoderInputConversion(encoder)...)
	branch = append(branch, encoderArgs(encoder, cfg.archiveCodec, cfg.archiveBitrateKbps, cfg.archiveFPS)...)
	branch = append(branch,
		"!", cfg.archiveCodec+"parse", "config-interval=-1", "!",
		"video/x-"+cfg.archiveCodec+",stream-format=byte-stream,alignment=au", "!",
	)
	return append(branch, rtpOutput(cfg.archiveCodec, 98, true)...)
}

func nativeEncodedRoleBranch(codec string, payloadType int) []string {
	return append([]string{
		"queue", "max-size-buffers=32", "!", codec + "parse", "config-interval=-1", "!",
		"video/x-" + codec + ",stream-format=byte-stream,alignment=au", "!",
	}, rtpOutput(codec, payloadType, true)...)
}

func rtpOutput(codec string, payloadType int, lossless bool) []string {
	payloader := "rtph264pay"
	if codec == "h265" {
		payloader = "rtph265pay"
	}
	queueDepth := "4"
	leaky := []string{"leaky=downstream"}
	if lossless {
		queueDepth = "32"
		leaky = nil
	}
	result := []string{"queue", "max-size-buffers=" + queueDepth}
	result = append(result, leaky...)
	return append(result,
		"!", payloader, "pt="+strconv.Itoa(payloadType), "config-interval=1", "mtu=1200", "!",
		"rtpstreampay", "!", "fdsink", "fd=3", "sync=false", "async=false",
	)
}
