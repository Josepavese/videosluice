// Pipeline construction is capability-driven and independent of any consumer.
package main

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

type ingestConfig struct {
	sourceID              string
	mediaEpoch            uint64
	sourceTransport       string
	device                string
	inputFormat           string
	encodeProvider        string
	archiveEncodeProvider string
	decodeProvider        string
	orientation           string
	aspectPolicy          string
	captureWidth          int
	captureHeight         int
	captureFPS            int
	mainWidth             int
	mainHeight            int
	mainFPS               int
	subWidth              int
	subHeight             int
	subFPS                int
	archiveWidth          int
	archiveHeight         int
	archiveFPS            int
	liveCodec             string
	archiveCodec          string
	archiveContent        string
	liveBitrateKbps       int
	archiveBitrateKbps    int
	liveEnabled           bool
	analysisEnabled       bool
	archiveEnabled        bool
	mainActive            bool
	warmupSec             int
	allowSWFallback       bool
	qualityEnabled        bool
	qualityEvery          int
	qualityStride         int
	qualityState          string
	toneMappingMode       string
	toneAccelerator       string
	toneTarget            int
	toneHysteresis        int
	toneGridColumns       int
	toneGridRows          int
	toneStatisticsHz      int
	toneMaxShadowGain     float64
	toneMaxHighlight      float64
	toneTemporalAlpha     float64
	toneNoiseGuard        bool
}

type gstElements struct {
	liveEncoder    string
	archiveEncoder string
	decoder        string
	toneMapper     string
}

var inspectElement = func(ctx context.Context, element string) bool {
	probeCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	return exec.CommandContext(probeCtx, "gst-inspect-1.0", element).Run() == nil
}

func normalizeIngestConfig(cfg ingestConfig) ingestConfig {
	if cfg.archiveWidth <= 0 {
		cfg.archiveWidth = cfg.captureWidth
	}
	if cfg.archiveHeight <= 0 {
		cfg.archiveHeight = cfg.captureHeight
	}
	if cfg.archiveFPS <= 0 {
		cfg.archiveFPS = cfg.captureFPS
	}
	if strings.TrimSpace(cfg.archiveCodec) == "" {
		cfg.archiveCodec = "h265"
	}
	if strings.TrimSpace(cfg.liveCodec) == "" {
		cfg.liveCodec = "h265"
	}
	if strings.TrimSpace(cfg.archiveContent) == "" {
		cfg.archiveContent = "adaptive"
	}
	if strings.TrimSpace(cfg.archiveEncodeProvider) == "" {
		cfg.archiveEncodeProvider = cfg.encodeProvider
	}
	if strings.TrimSpace(cfg.toneMappingMode) == "" {
		cfg.toneMappingMode = "off"
	}
	if strings.TrimSpace(cfg.toneAccelerator) == "" {
		cfg.toneAccelerator = "auto"
	}
	return cfg
}

func resolveGStreamerElements(ctx context.Context, cfg ingestConfig) (gstElements, error) {
	cfg = normalizeIngestConfig(cfg)
	for _, element := range []string{"shmsink", "shmsrc"} {
		if !inspectElement(ctx, element) {
			return gstElements{}, fmt.Errorf("required isolated-branch GStreamer element %s is unavailable", element)
		}
	}
	directMain := canUseNativeLive(cfg)
	elements := gstElements{}
	if cfg.liveEnabled && !directMain {
		candidates, err := encoderCandidates(cfg.encodeProvider, cfg.liveCodec, cfg.allowSWFallback)
		if err != nil {
			return elements, err
		}
		elements.liveEncoder = firstAvailableElement(ctx, candidates)
		if elements.liveEncoder == "" {
			return elements, fmt.Errorf("no permitted %s encoder is available for provider %q", strings.ToUpper(cfg.liveCodec), cfg.encodeProvider)
		}
	}
	if archiveRequiresEncode(cfg) {
		candidates, err := encoderCandidates(cfg.archiveEncodeProvider, cfg.archiveCodec, cfg.allowSWFallback)
		if err != nil {
			return elements, err
		}
		elements.archiveEncoder = firstAvailableElement(ctx, candidates)
		if elements.archiveEncoder == "" {
			return elements, fmt.Errorf(
				"no permitted %s archive encoder is available for provider %q",
				strings.ToUpper(cfg.archiveCodec),
				cfg.archiveEncodeProvider,
			)
		}
	}
	if rawProcessingRequired(cfg) && (cfg.inputFormat == "h264" || cfg.inputFormat == "h265" || cfg.inputFormat == "mjpeg") {
		candidates, err := decoderCandidates(cfg.decodeProvider, cfg.inputFormat, cfg.allowSWFallback)
		if err != nil {
			return elements, err
		}
		elements.decoder = firstAvailableElement(ctx, candidates)
		if elements.decoder == "" {
			return elements, fmt.Errorf("no permitted %s decoder is available for provider %q", cfg.inputFormat, cfg.decodeProvider)
		}
	}
	if cfg.toneMappingMode != "off" {
		if !inspectElement(ctx, "videobrokertone") {
			return elements, errors.New("spatial tone mapping requested but the product videobrokertone element is unavailable")
		}
		elements.toneMapper = "videobrokertone"
	}
	return elements, nil
}

func encoderCandidates(provider string, codec string, allowSoftware bool) ([]string, error) {
	codec = strings.ToLower(strings.TrimSpace(codec))
	if codec != "h264" && codec != "h265" {
		return nil, fmt.Errorf("unsupported GStreamer encode codec %q", codec)
	}
	hardware := map[string]map[string][]string{
		"gstreamer_mpp": {
			"h264": {"mpph264enc"},
			"h265": {"mpph265enc"},
		},
		"gstreamer_v4l2": {
			"h264": {"v4l2h264enc", "v4l2slh264enc"},
			"h265": {"v4l2h265enc", "v4l2slh265enc"},
		},
		"gstreamer_vendor": {
			"h264": {"x5h264enc", "mpph264enc", "v4l2h264enc", "v4l2slh264enc"},
			"h265": {"x5h265enc", "mpph265enc", "v4l2h265enc", "v4l2slh265enc"},
		},
	}
	software := map[string]string{"h264": "x264enc", "h265": "x265enc"}
	switch strings.TrimSpace(provider) {
	case "gstreamer_mpp", "gstreamer_v4l2", "gstreamer_vendor":
		candidates := append([]string(nil), hardware[strings.TrimSpace(provider)][codec]...)
		if allowSoftware {
			candidates = append(candidates, software[codec])
		}
		return candidates, nil
	case "software":
		if allowSoftware {
			return []string{software[codec]}, nil
		}
		return nil, fmt.Errorf("software %s encoder is forbidden by policy", strings.ToUpper(codec))
	default:
		return nil, fmt.Errorf("unsupported GStreamer encode provider %q", provider)
	}
}

func decoderCandidates(provider string, codec string, allowSoftware bool) ([]string, error) {
	switch strings.TrimSpace(provider) {
	case "gstreamer_mpp":
		candidates := []string{"mppvideodec"}
		if allowSoftware {
			switch codec {
			case "h264":
				candidates = append(candidates, "avdec_h264")
			case "h265":
				candidates = append(candidates, "avdec_h265")
			}
		}
		return candidates, nil
	case "gstreamer_v4l2":
		if codec == "h265" {
			candidates := []string{"v4l2h265dec", "v4l2slh265dec"}
			if allowSoftware {
				candidates = append(candidates, "avdec_h265")
			}
			return candidates, nil
		}
		if codec == "h264" {
			candidates := []string{"v4l2h264dec", "v4l2slh264dec"}
			if allowSoftware {
				candidates = append(candidates, "avdec_h264")
			}
			return candidates, nil
		}
	case "gstreamer_vendor":
		if codec == "h265" {
			candidates := []string{"x5h265dec", "v4l2h265dec", "v4l2slh265dec", "mppvideodec"}
			if allowSoftware {
				candidates = append(candidates, "avdec_h265")
			}
			return candidates, nil
		}
		if codec == "h264" {
			candidates := []string{"x5h264dec", "v4l2h264dec", "v4l2slh264dec", "mppvideodec"}
			if allowSoftware {
				candidates = append(candidates, "avdec_h264")
			}
			return candidates, nil
		}
		if codec == "mjpeg" {
			candidates := []string{"mppvideodec"}
			if allowSoftware {
				candidates = append(candidates, "jpegdec")
			}
			return candidates, nil
		}
	case "software":
		if !allowSoftware {
			return nil, fmt.Errorf("software %s decoder is forbidden by policy", codec)
		}
		switch codec {
		case "h264":
			return []string{"avdec_h264"}, nil
		case "h265":
			return []string{"avdec_h265"}, nil
		case "mjpeg":
			return []string{"jpegdec"}, nil
		}
	}
	if codec == "mjpeg" && allowSoftware {
		return []string{"jpegdec"}, nil
	}
	return nil, fmt.Errorf("unsupported GStreamer decode provider %q for %s", provider, codec)
}

func firstAvailableElement(ctx context.Context, candidates []string) string {
	for _, candidate := range candidates {
		if inspectElement(ctx, candidate) {
			return candidate
		}
	}
	return ""
}

func canUseNativeLive(cfg ingestConfig) bool {
	toneDisabled := strings.TrimSpace(cfg.toneMappingMode) == "" || cfg.toneMappingMode == "off"
	return cfg.liveEnabled && cfg.inputFormat == cfg.liveCodec &&
		toneDisabled &&
		cfg.orientation == "normal" &&
		cfg.mainWidth == cfg.captureWidth &&
		cfg.mainHeight == cfg.captureHeight &&
		cfg.mainFPS == cfg.captureFPS
}

func archiveUsesNativeSource(cfg ingestConfig) bool {
	cfg = normalizeIngestConfig(cfg)
	compressed := cfg.inputFormat == "h264" || cfg.inputFormat == "h265"
	content := strings.ToLower(strings.TrimSpace(cfg.archiveContent))
	toneDisabled := strings.TrimSpace(cfg.toneMappingMode) == "" || cfg.toneMappingMode == "off"
	return cfg.archiveEnabled && compressed && cfg.archiveCodec == cfg.inputFormat &&
		cfg.archiveWidth == cfg.captureWidth && cfg.archiveHeight == cfg.captureHeight &&
		cfg.archiveFPS == cfg.captureFPS &&
		(content == "source" || (content == "adaptive" && toneDisabled))
}

func archiveRequiresEncode(cfg ingestConfig) bool {
	return cfg.archiveEnabled && !archiveUsesNativeSource(cfg)
}

func rawProcessingRequired(cfg ingestConfig) bool {
	return cfg.analysisEnabled || cfg.qualityEnabled ||
		(cfg.liveEnabled && !canUseNativeLive(cfg)) || archiveRequiresEncode(cfg)
}

func effectiveBitrateKbps(value int) int {
	if value <= 0 {
		return 4096
	}
	return value
}

func rawNV12Fanout(cfg ingestConfig, elements gstElements) []string {
	args := []string{
		"video/x-raw,format=NV12", "!", "tee", "name=source",
		"source.", "!", "queue", "max-size-buffers=2", "leaky=downstream", "!",
	}
	if cfg.toneMappingMode != "off" {
		args = append(args,
			elements.toneMapper,
			"mode="+cfg.toneMappingMode,
			"accelerator="+cfg.toneAccelerator,
			"target-luma="+strconv.Itoa(cfg.toneTarget),
			"hysteresis="+strconv.Itoa(cfg.toneHysteresis),
			"grid-columns="+strconv.Itoa(cfg.toneGridColumns),
			"grid-rows="+strconv.Itoa(cfg.toneGridRows),
			"statistics-hz="+strconv.Itoa(cfg.toneStatisticsHz),
			"sample-stride="+strconv.Itoa(cfg.qualityStride),
			"max-shadow-gain="+strconv.FormatFloat(cfg.toneMaxShadowGain, 'f', -1, 64),
			"max-highlight-reduction="+strconv.FormatFloat(cfg.toneMaxHighlight, 'f', -1, 64),
			"temporal-alpha="+strconv.FormatFloat(cfg.toneTemporalAlpha, 'f', -1, 64),
			"noise-guard="+strconv.FormatBool(cfg.toneNoiseGuard),
			"!",
		)
	}
	return append(args, "tee", "name=raw")
}

func decoderOutputConversion(decoder string) []string {
	if strings.HasPrefix(decoder, "avdec_") {
		return []string{"videoconvert", "!"}
	}
	return nil
}

func gstInputMediaType(format string) string {
	switch format {
	case "mjpeg":
		return "jpeg"
	default:
		return format
	}
}

func qualityNV12Branch(cfg ingestConfig, source string, fd int) []string {
	numerator := cfg.captureFPS
	denominator := cfg.qualityEvery
	if denominator <= 0 {
		denominator = cfg.captureFPS
	}
	divisor := greatestCommonDivisor(numerator, denominator)
	numerator /= divisor
	denominator /= divisor
	return []string{
		source, "!", "queue", "max-size-buffers=1", "leaky=downstream", "!",
		"videorate", "drop-only=true", "!",
		"video/x-raw,format=NV12,width=" + strconv.Itoa(cfg.captureWidth) +
			",height=" + strconv.Itoa(cfg.captureHeight) +
			",framerate=" + strconv.Itoa(numerator) + "/" + strconv.Itoa(denominator), "!",
		"fdsink", "fd=" + strconv.Itoa(fd), "sync=false", "async=false",
	}
}

func greatestCommonDivisor(left, right int) int {
	if left < 0 {
		left = -left
	}
	if right < 0 {
		right = -right
	}
	for right != 0 {
		left, right = right, left%right
	}
	if left <= 0 {
		return 1
	}
	return left
}

func encoderInputConversion(element string) []string {
	// x265enc does not accept NV12. Keep the broker's internal raw contract on
	// NV12 and convert only at the software-encoder boundary.
	if element == "x265enc" {
		return []string{"videoconvert", "!", "video/x-raw,format=I420", "!"}
	}
	return nil
}

func sameVideoAspect(leftWidth, leftHeight, rightWidth, rightHeight int) bool {
	return leftWidth > 0 && leftHeight > 0 && rightWidth > 0 && rightHeight > 0 &&
		int64(leftWidth)*int64(rightHeight) == int64(rightWidth)*int64(leftHeight)
}

func aspectScaleElements(srcWidth, srcHeight, dstWidth, dstHeight int, policy string) []string {
	if sameVideoAspect(srcWidth, srcHeight, dstWidth, dstHeight) || policy == "exact" {
		return []string{"videoscale", "!"}
	}
	if policy == "letterbox" {
		return []string{"videoscale", "add-borders=true", "!"}
	}
	left, right, top, bottom := centeredVideoCrop(srcWidth, srcHeight, dstWidth, dstHeight)
	return []string{
		"videocrop",
		"left=" + strconv.Itoa(left), "right=" + strconv.Itoa(right),
		"top=" + strconv.Itoa(top), "bottom=" + strconv.Itoa(bottom), "!",
		"videoscale", "!",
	}
}

func centeredVideoCrop(srcWidth, srcHeight, dstWidth, dstHeight int) (left, right, top, bottom int) {
	if int64(srcWidth)*int64(dstHeight) > int64(dstWidth)*int64(srcHeight) {
		keptWidth := int(int64(srcHeight) * int64(dstWidth) / int64(dstHeight))
		keptWidth &= ^1
		total := srcWidth - keptWidth
		left = (total / 2) & ^1
		right = total - left
		return
	}
	keptHeight := int(int64(srcWidth) * int64(dstHeight) / int64(dstWidth))
	keptHeight &= ^1
	total := srcHeight - keptHeight
	top = (total / 2) & ^1
	bottom = total - top
	return
}

func encoderArgs(element string, codec string, bitrateKbps int, fps int) []string {
	bitrateKbps = effectiveBitrateKbps(bitrateKbps)
	gop := fps * 2
	if gop < 15 {
		gop = 15
	}
	switch element {
	case "mpph264enc", "mpph265enc":
		return []string{element, "bps=" + strconv.Itoa(bitrateKbps*1000), "gop=" + strconv.Itoa(gop), "rc-mode=cbr"}
	case "x264enc":
		return []string{element, "bitrate=" + strconv.Itoa(bitrateKbps), "key-int-max=" + strconv.Itoa(gop), "tune=zerolatency", "speed-preset=ultrafast", "byte-stream=true"}
	case "x265enc":
		return []string{element, "bitrate=" + strconv.Itoa(bitrateKbps), "key-int-max=" + strconv.Itoa(gop), "tune=zerolatency", "speed-preset=ultrafast"}
	default:
		_ = codec
		return []string{element}
	}
}

func gstOrientation(value string) string {
	switch value {
	case "rotate_180":
		return "rotate-180"
	case "mirror_horizontal":
		return "horizontal-flip"
	case "mirror_vertical":
		return "vertical-flip"
	default:
		return "none"
	}
}
