package broker

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	videoproviders "github.com/Josepavese/videosluice/internal/source"
	"github.com/Josepavese/videosluice/internal/unixsocket"
	"github.com/Josepavese/videosluice/quality"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/Josepavese/videosluice/capability"
	productconfig "github.com/Josepavese/videosluice/config"
	in "github.com/Josepavese/videosluice/frameingress"
)

var (
	brokerStatPath  = os.Stat
	brokerGlobPaths = filepath.Glob
	brokerReadDir   = os.ReadDir
	brokerReadFile  = os.ReadFile
)

const (
	rdkBrokerMuxBridgeEnv     = "VIDEO_BROKER_RDKX5_HBN_MUX_BRIDGE"
	rdkBrokerV4L2MuxBridgeEnv = "VIDEO_BROKER_RDKX5_V4L2_MUX_BRIDGE"
	gstVideoIngestBridgeEnv   = "VIDEO_BROKER_GST_VIDEO_INGEST_BRIDGE"
)

const (
	brokerStreamH264                    = 1
	brokerStreamNV12                    = 2
	brokerStreamH265                    = 3
	brokerMuxStreamArchiveH264Native    = 4
	brokerEndpointArchive               = 5
	brokerMuxStreamArchiveH264Generated = 6
	brokerMuxStreamLiveEncoderEpoch     = 7
	brokerMainClientQueueDepth          = 8
	brokerSubClientQueueDepth           = 1
	brokerSubDispatchQueueDepth         = 2
	brokerArchiveQueueSeconds           = 10
	brokerArchiveQueueMinDepth          = 60
	brokerArchiveQueueMaxDepth          = 1200
	brokerArchiveQueueMaxBytes          = 16 * 1024 * 1024
	brokerMainBootstrapMaxBytes         = 16 * 1024 * 1024
	brokerArchiveBootstrapMaxBytes      = 32 * 1024 * 1024
	brokerDefaultMainWriteTimeout       = 2 * time.Second
	brokerMainBootstrapRecoveryEvery    = 2 * time.Second
	brokerArchiveWriteTimeout           = 12 * time.Second
	brokerChildTerminationGrace         = 10 * time.Second
	brokerChildWaitFallback             = 11 * time.Second
	brokerSubWriteTimeout               = 2 * time.Second
	brokerMetricsLogEvery               = 60 * time.Second
	brokerLiveMetricsLogEvery           = 10 * time.Second
	brokerChildRestartDelay             = 3 * time.Second
	brokerChildRestartMaxDelay          = 60 * time.Second
	brokerUVCHandoffRetryInitial        = 5 * time.Second
	brokerUVCHandoffRetryMaximum        = 30 * time.Second
	brokerUVCHandoffRetryBudget         = 90 * time.Second
	brokerComponentRetryInitial         = 30 * time.Second
	brokerComponentRetryMaximum         = 5 * time.Minute
	brokerComponentTrialTimeout         = 15 * time.Second
	brokerComponentStableDuration       = 5 * time.Minute
	brokerComponentFailureThreshold     = 3
	brokerDefaultFrameWatchdog          = 12 * time.Second
	brokerCadenceWatchdogMinRatio       = 0.90
	brokerCadenceRecoveryRatio          = 0.95
	brokerDefaultCadenceFPS             = 15
	brokerDefaultCaptureSelectTimeoutMs = videoproviders.DefaultV4L2CaptureSelectTimeoutMs
	brokerDefaultCaptureMaxTimeouts     = videoproviders.DefaultV4L2CaptureMaxTimeouts
	brokerMuxMaxPayloadBytes            = 16 * 1024 * 1024
	brokerMuxPipeTargetBytes            = 1024 * 1024
	brokerMaxCoalescedLogEntries        = 2048
)

var brokerUSBDevicesDir = "/sys/bus/usb/devices"
var brokerLogRetryAfterRE = regexp.MustCompile(`\bretry_after=\d+s\b`)
var brokerGetProcessGroup = syscall.Getpgid
var brokerProcessGroupAlive = brokerProcessGroupExists
var brokerKillProcessGroup = func(pgid int) error { return syscall.Kill(-pgid, syscall.SIGKILL) }

var brokerMuxPayloadPools [3]sync.Pool

var brokerMuxPayloadClassBytes = [...]int{
	64 * 1024,
	512 * 1024,
	4 * 1024 * 1024,
}

func acquireBrokerMuxPayload(size int) []byte {
	if size <= 0 {
		return nil
	}
	for class, capacity := range brokerMuxPayloadClassBytes {
		if size > capacity {
			continue
		}
		if pooled := brokerMuxPayloadPools[class].Get(); pooled != nil {
			return pooled.([]byte)[:size]
		}
		return make([]byte, size, capacity)
	}
	return make([]byte, size)
}

func releaseBrokerMuxPayload(payload []byte) {
	capacity := cap(payload)
	for class, classCapacity := range brokerMuxPayloadClassBytes {
		if capacity == classCapacity {
			brokerMuxPayloadPools[class].Put(payload[:0])
			return
		}
	}
}

type videoBrokerPlan struct {
	FrameCapture              videoproviders.FrameCaptureFactory `json:"-"`
	SourceID                  string
	SourceIdentity            string
	AdmissionProfiles         []productconfig.SourceRoleProfile
	CapabilityAllocator       *capabilityAllocator
	Enabled                   bool
	LiveEnabled               bool
	AnalysisEnabled           bool
	AnalysisStreamID          string
	ArchiveEnabled            bool
	MainSocketPath            string
	ArchiveSocketPath         string
	SubSocketPath             string
	SubShmPath                string
	SubNotifySocketPath       string
	Transport                 string
	EncodeProvider            string
	ArchiveEncodeProvider     string
	DecodeProvider            string
	MainFormat                string
	SubFormat                 string
	MainWidth                 int
	MainHeight                int
	MainFPS                   int
	SubWidth                  int
	SubHeight                 int
	SubFPS                    int
	CaptureWidth              int
	CaptureHeight             int
	CaptureFPS                int
	ArchiveWidth              int
	ArchiveHeight             int
	ArchiveWidthAlignment     int
	ArchiveHeightAlignment    int
	ArchiveFPS                int
	ArchiveRateControl        string
	CaptureDevice             string
	CaptureFormat             string
	CaptureCodecPreference    string
	CaptureCodecPolicy        string
	CapturePolicy             string
	CaptureAspectPolicy       string
	AllowScaledYUYV           bool
	PreferExactYUYV           bool
	VideoOrientation          string
	RequireHWCodec            bool
	AllowSWFallback           bool
	H264EncodeHWAvailable     bool
	H264DecodeHWAvailable     bool
	H265EncodeHWAvailable     bool
	H265DecodeHWAvailable     bool
	MJPEGDecodeHWAvailable    bool
	CameraHostIndex           int
	WarmupSec                 int
	LiveBitrateKbps           int
	ArchiveBitrateKbps        int
	CaptureSelectTimeoutMs    int
	CaptureMaxTimeouts        int
	FrameWatchdogSec          int
	MainWriteTimeoutMs        int
	ResourceGovernorEnabled   bool
	ResourceGovernorPollSec   int
	ResourceGovernorSoftC     int
	ResourceGovernorHardC     int
	ResourceGovernorBPU       int
	ResourceGovernorStatePath string
	Sensor                    string
	StateDirectory            string
	EngineDirectory           string
	CapabilityInventory       capability.Inventory
	ArchiveContent            string
	ArchiveCodec              string
	MainWidthAlignment        int
	MainHeightAlignment       int

	QualityMonitorEnabled         bool
	QualityMonitorReportEvery     int
	QualityMonitorSampleStride    int
	QualityMonitorStatePath       string
	CameraAdmissionStatePath      string
	ToneMappingEnabled            bool
	ToneMappingTarget             int
	ToneMappingHysteresis         int
	ToneMappingMode               string
	ToneMappingAccelerator        string
	ToneMappingGridColumns        int
	ToneMappingGridRows           int
	ToneMappingStatisticsHz       int
	ToneMappingMaxShadowGain      float64
	ToneMappingMaxHighlightReduce float64
	ToneMappingTemporalAlpha      float64
	ToneMappingNoiseGuard         bool
}

type videoBrokerRuntime struct {
	plan         videoBrokerPlan
	startupReady chan error
	startupOnce  sync.Once

	mu                        sync.Mutex
	nextClientID              int
	nextMainFrame             uint32
	nextArchiveFrame          uint32
	nextSubFrame              uint32
	mainClients               map[int]*brokerClient
	archiveClients            map[int]*brokerClient
	subClients                map[int]*brokerClient
	mainBootstrap             [][]byte
	mainCodecConfig           [][]byte
	mainBootstrapBytes        int
	mainAwaitingEncoderEpoch  bool
	mainAwaitingKeyframe      bool
	mainKeyframeRequestToken  uint64
	mainBootstrapRecoveryAt   time.Time
	archiveBootstrap          [][]byte
	archiveCodecConfig        [][]byte
	archiveBootstrapBytes     int
	archiveCodecStreamID      byte
	observedCaptureCodec      string
	observedCaptureWidth      int
	observedCaptureHeight     int
	observedCaptureFPS        int
	observedArchiveCodec      string
	observedArchiveDelivery   string
	observedArchiveFPS        int
	observedDecodeCodec       string
	observedDecodeRequested   string
	observedDecodeBackend     string
	observedDecodeFallback    string
	observedResizeBackend     string
	observedRoleResizeBackend map[string]string
	observedLiveEpoch         uint64
	capabilityPlan            capability.Plan
	toneMappingSupported      bool
	toneMappingBackend        string
	observedToneMode          string
	observedToneAccelerator   string
	observedToneActive        bool
	observedToneGainMin       float64
	observedToneGainMax       float64
	observedToneUpdates       uint64
	componentRecovery         map[string]hardwareComponentRecovery
	childComponentRetryScopes map[string]string
	componentRetrySequence    uint64
	activeCaptureWidth        int
	activeCaptureHeight       int
	activeCaptureFPS          int
	activeMainWidth           int
	activeMainHeight          int
	activeArchiveWidth        int
	activeArchiveHeight       int
	cameraAdmission           videoproviders.CameraAdmissionDecision
	cameraAdmissionTracker    cameraAdmissionRuntimeTracker
	cameraAdmitted            bool
	modeHealthFingerprint     string
	captureModeHealthEntries  map[string]cameraModeHealthEntry
	captureModeExclusions     []videoproviders.V4L2CaptureModeExclusion
	captureModeFailures       map[string]int
	cameraPersistence         cameraStatePersistence
	subShared                 *brokerSubSharedFrame
	subNotifier               *brokerSubFrameNotifier
	mainPublication           *unixsocket.Publication
	archivePublication        *unixsocket.Publication
	subPublication            *unixsocket.Publication
	mainPublicationCancel     context.CancelFunc
	archivePublicationCancel  context.CancelFunc
	subPublicationCancel      context.CancelFunc
	fanoutRoleGates           map[string]string
	subDispatch               chan brokerSubFrame
	childCmd                  *exec.Cmd
	childOut                  io.ReadCloser
	childStderr               io.ReadCloser
	childStderrDone           chan struct{}
	childExitDone             chan struct{}
	childIn                   io.WriteCloser
	childControl              chan bridgeRuntimeControl
	childFanoutControl        chan workerFanoutWrite
	childFanoutSupported      bool
	childFanoutControlMode    string
	workerFanoutSequence      uint64
	workerFanoutWaiters       map[workerFanoutReceiptKey]chan workerFanoutReceiptResult
	childControlAvailable     bool
	childControlSupported     bool
	childLiveControlMode      string
	mainBitrateKbps           int
	archiveBitrateKbps        int
	childCancel               context.CancelFunc
	childFrames               videoproviders.FrameCapture
	childFramesDone           <-chan struct{}
	causalObserver            *causalDeliveryObserver
	causalSubFrame            uint64
	causalSubSourceEpoch      uint64
	frameCleanupErr           error
	pendingFrameCleanup       videoproviders.FrameCapture
	pendingFrameJoin          <-chan struct{}
	childRunning              bool
	childFinishing            bool
	childRecovery             bool
	childStartedAt            time.Time
	childFailureMu            sync.Mutex
	childFailureEpoch         uint64
	childFailureReason        string
	childFailureAt            time.Time
	childStartMainFrames      uint64
	childStartArchiveFrames   uint64
	childStartSubFrames       uint64
	mainDemandedAt            time.Time
	routingLiveDemand         bool
	lastFrameAt               time.Time
	lastMainFrameAt           time.Time
	lastSubFrameAt            time.Time
	lastArchiveFrameAt        time.Time
	mediaEpoch                uint64
	totalMainFrames           uint64
	totalArchiveFrames        uint64
	totalSubFrames            uint64
	totalMainDrops            uint64
	totalArchiveDisconnects   uint64
	totalSubDrops             uint64
	createdAt                 time.Time
	qualityInputSince         time.Time
	healthState               string
	healthError               string
	healthChangedAt           time.Time
	everProducedFrames        bool
	cadenceDegraded           bool
	cadenceRole               string
	cadenceMeasuredFPS        float64
	cadenceThresholdFPS       float64
	streamPerformance         map[string]StreamPerformance
	metrics                   videoBrokerMetrics
	controlSequence           uint64
	bridgeControlReady        brokerBridgeControlReadyObservation
	bridgeControlReadyChanged chan struct{}
	governor                  resourceGovernorDecision

	nextChildStartAt         time.Time
	childStartBackoff        time.Duration
	uvcHandoffStartedAt      time.Time
	uvcHandoffLastMediaEpoch uint64
	uvcHandoffFailures       int
	uvcHandoffQuietEpoch     uint64
	uvcHandoffQuietDelay     time.Duration
	uvcAdmittedModes         map[string]struct{}
	logLimiter               *coalescedLogLimiter
	wg                       sync.WaitGroup
	lifecycleMu              sync.Mutex
	runtimeContext           context.Context
	stopping                 bool
}

type hardwareComponentRecovery struct {
	ComponentID          string
	Stage                string
	Format               string
	ConsecutiveFailures  int
	LastFailureEpoch     uint64
	LastFailureToken     uint64
	RetryAfter           time.Time
	RetryScope           string
	AwaitNaturalEpoch    bool
	TrialToken           uint64
	TrialMediaEpoch      uint64
	TrialChildPID        int
	TrialStartedAt       time.Time
	TrialCandidateReady  time.Time
	TrialCommitRequested bool
	TrialDeadline        time.Time
	CancelToken          uint64
	CancelMediaEpoch     uint64
	CancelChildPID       int
	StableAfter          time.Time
	HalfOpenEpoch        uint64
	HalfOpenChildPID     int
	HalfOpenFrameBase    uint64
}

type videoBrokerHealthSnapshot struct {
	State               string
	LastError           string
	LastChangeAt        time.Time
	OpticalSignalState  string
	OpticalSignalReason string
}

type brokerClient struct {
	conn            net.Conn
	streamID        byte
	send            chan []byte
	writeTimeout    time.Duration
	dropAllowed     bool
	queueBytesLimit int64
	queuedBytes     int64
	closed          chan struct{}
	closeOnce       sync.Once
	failureMu       sync.Mutex
	failureReason   string
	enqueued        uint64
	dropped         uint64
	written         uint64
	mainControl     videoMainClientControl
	mainControlUser bool
}

type videoBrokerMetrics struct {
	lastLog time.Time

	mainFrames    uint64
	archiveFrames uint64
	subFrames     uint64
	mainBytes     uint64
	archiveBytes  uint64
	subBytes      uint64

	mainNoClientFrames       uint64
	archiveNoClientFrames    uint64
	subNoClientFrames        uint64
	mainClientDrops          uint64
	archiveClientDisconnects uint64
	subClientDrops           uint64
	subIngressDrops          uint64
	mainMaxQueue             int
	archiveMaxQueue          int
	subMaxQueue              int
	mainFrameGapMax          time.Duration
	archiveFrameGapMax       time.Duration
	subFrameGapMax           time.Duration
	lastArchiveFrameAt       time.Time

	subShmWrites      uint64
	subShmWriteErrors uint64
	subNotifyWrites   uint64
	subNotifyErrors   uint64
}

func resolveVideoBrokerPlan(cfg engineConfig) videoBrokerPlan {
	plan := videoBrokerPlan{
		SourceIdentity:            strings.TrimSpace(cfg.SourceIdentity),
		Enabled:                   cfg.Enabled,
		LiveEnabled:               cfg.LiveEnabled,
		AnalysisEnabled:           cfg.AnalysisEnabled,
		AnalysisStreamID:          cfg.AnalysisStreamID,
		ArchiveEnabled:            cfg.ArchiveEnabled,
		MainSocketPath:            strings.TrimSpace(cfg.LiveSocketPath),
		ArchiveSocketPath:         strings.TrimSpace(cfg.ArchiveSocketPath),
		SubSocketPath:             strings.TrimSpace(cfg.AnalysisSocketPath),
		SubShmPath:                strings.TrimSpace(cfg.AnalysisSharedMemoryPath),
		SubNotifySocketPath:       strings.TrimSpace(cfg.AnalysisNotifySocketPath),
		Transport:                 strings.TrimSpace(cfg.VideoTransport),
		EncodeProvider:            strings.TrimSpace(cfg.VideoEncodeProvider),
		DecodeProvider:            strings.TrimSpace(cfg.VideoDecodeProvider),
		MainFormat:                strings.TrimSpace(cfg.LiveFormat),
		SubFormat:                 strings.TrimSpace(cfg.AnalysisFormat),
		MainWidth:                 cfg.LiveWidth,
		MainHeight:                cfg.LiveHeight,
		MainFPS:                   cfg.LiveFPS,
		SubWidth:                  cfg.AnalysisWidth,
		SubHeight:                 cfg.AnalysisHeight,
		SubFPS:                    cfg.AnalysisFPS,
		CaptureWidth:              cfg.RawWidth,
		CaptureHeight:             cfg.RawHeight,
		CaptureFPS:                cfg.VideoCaptureFPS,
		ArchiveWidth:              cfg.ArchiveWidth,
		ArchiveHeight:             cfg.ArchiveHeight,
		ArchiveFPS:                cfg.ArchiveFPS,
		ArchiveRateControl:        strings.TrimSpace(cfg.ArchiveRateControl),
		CaptureDevice:             strings.TrimSpace(cfg.VideoCaptureDevice),
		CaptureFormat:             strings.TrimSpace(cfg.VideoCaptureFormat),
		CaptureCodecPreference:    strings.TrimSpace(cfg.VideoCaptureCodecPreference),
		CaptureCodecPolicy:        strings.TrimSpace(cfg.VideoCaptureCodecPolicy),
		CapturePolicy:             strings.TrimSpace(cfg.VideoCapturePolicyProvider),
		CaptureAspectPolicy:       strings.TrimSpace(cfg.VideoCaptureAspectPolicy),
		AllowScaledYUYV:           cfg.VideoCaptureAllowScaledYUYV,
		PreferExactYUYV:           cfg.VideoCapturePreferExactYUYV,
		VideoOrientation:          videoproviders.NormalizeVideoOrientation(cfg.VideoOrientation),
		RequireHWCodec:            cfg.VideoRequireHWCodec,
		AllowSWFallback:           cfg.VideoAllowSWFallback,
		H264EncodeHWAvailable:     cfg.CapabilityInventory.HardwareAvailable(capability.StageEncode, "h264"),
		H264DecodeHWAvailable:     cfg.CapabilityInventory.HardwareAvailable(capability.StageDecode, "h264"),
		H265EncodeHWAvailable:     cfg.CapabilityInventory.HardwareAvailable(capability.StageEncode, "h265"),
		H265DecodeHWAvailable:     cfg.CapabilityInventory.HardwareAvailable(capability.StageDecode, "h265"),
		MJPEGDecodeHWAvailable:    cfg.CapabilityInventory.HardwareAvailable(capability.StageDecode, "mjpeg"),
		CameraHostIndex:           cfg.CameraHostIndex,
		WarmupSec:                 cfg.VideoCaptureWarmupSec,
		LiveBitrateKbps:           cfg.LiveBitrateKbps,
		ArchiveBitrateKbps:        cfg.ArchiveBitrateKbps,
		CaptureSelectTimeoutMs:    cfg.VideoCaptureSelectTimeoutMs,
		CaptureMaxTimeouts:        cfg.VideoCaptureMaxTimeouts,
		FrameWatchdogSec:          cfg.FrameWatchdogSec,
		MainWriteTimeoutMs:        cfg.LiveWriteTimeoutMs,
		ResourceGovernorEnabled:   cfg.ResourceGovernorEnabled,
		ResourceGovernorPollSec:   cfg.ResourceGovernorPollSec,
		ResourceGovernorSoftC:     cfg.ResourceGovernorThermalSoftC,
		ResourceGovernorHardC:     cfg.ResourceGovernorThermalHardC,
		ResourceGovernorBPU:       cfg.ResourceGovernorBPUThreshold,
		ResourceGovernorStatePath: strings.TrimSpace(cfg.ResourceGovernorStatePath),
		Sensor:                    strings.TrimSpace(cfg.Sensor),
		StateDirectory:            strings.TrimSpace(cfg.StateDirectory),
		EngineDirectory:           strings.TrimSpace(cfg.EngineDirectory),
		CapabilityInventory:       cfg.CapabilityInventory,
		ArchiveContent:            strings.ToLower(strings.TrimSpace(cfg.ArchiveContent)),
		ArchiveCodec:              strings.ToLower(strings.TrimSpace(cfg.ArchiveCodec)),

		QualityMonitorEnabled:         cfg.VideoQualityMonitorEnabled,
		QualityMonitorReportEvery:     cfg.VideoQualityMonitorReportEvery,
		QualityMonitorSampleStride:    cfg.VideoQualityMonitorSampleStride,
		QualityMonitorStatePath:       strings.TrimSpace(cfg.VideoQualityMonitorStatePath),
		CameraAdmissionStatePath:      strings.TrimSpace(cfg.VideoCameraAdmissionStatePath),
		ToneMappingEnabled:            brokerToneMappingEnabled(cfg),
		ToneMappingTarget:             brokerToneMappingTarget(cfg),
		ToneMappingHysteresis:         cfg.ToneMappingHysteresis,
		ToneMappingMode:               cfg.ToneMappingMode,
		ToneMappingAccelerator:        cfg.ToneMappingAccelerator,
		ToneMappingGridColumns:        cfg.ToneMappingGridColumns,
		ToneMappingGridRows:           cfg.ToneMappingGridRows,
		ToneMappingStatisticsHz:       cfg.ToneMappingStatisticsHz,
		ToneMappingMaxShadowGain:      cfg.ToneMappingMaxShadowGain,
		ToneMappingMaxHighlightReduce: cfg.ToneMappingMaxHighlightReduce,
		ToneMappingTemporalAlpha:      cfg.ToneMappingTemporalAlpha,
		ToneMappingNoiseGuard:         cfg.ToneMappingNoiseGuard,
	}
	if plan.CaptureSelectTimeoutMs <= 0 {
		plan.CaptureSelectTimeoutMs = brokerDefaultCaptureSelectTimeoutMs
	}
	if plan.CaptureMaxTimeouts <= 0 {
		plan.CaptureMaxTimeouts = brokerDefaultCaptureMaxTimeouts
	}
	if plan.FrameWatchdogSec <= 0 {
		plan.FrameWatchdogSec = int(brokerDefaultFrameWatchdog / time.Second)
	}
	if plan.MainWriteTimeoutMs <= 0 {
		plan.MainWriteTimeoutMs = int(brokerDefaultMainWriteTimeout / time.Millisecond)
	}
	if plan.ArchiveEnabled {
		if plan.ArchiveWidth <= 0 {
			plan.ArchiveWidth = plan.CaptureWidth
		}
		if plan.ArchiveHeight <= 0 {
			plan.ArchiveHeight = plan.CaptureHeight
		}
		if plan.ArchiveFPS <= 0 {
			plan.ArchiveFPS = plan.CaptureFPS
		}
		if plan.ArchiveCodec == "" {
			plan.ArchiveCodec = "auto"
		}
		if plan.ArchiveContent == "" {
			plan.ArchiveContent = "adaptive"
		}
		if plan.ArchiveRateControl == "" {
			plan.ArchiveRateControl = "cbr"
		}
	} else {
		plan.ArchiveWidth, plan.ArchiveHeight, plan.ArchiveFPS = 0, 0, 0
		plan.ArchiveCodec, plan.ArchiveContent, plan.ArchiveRateControl = "", "", ""
	}
	if plan.LiveEnabled {
		if plan.MainWidth <= 0 {
			plan.MainWidth = plan.CaptureWidth
		}
		if plan.MainHeight <= 0 {
			plan.MainHeight = plan.CaptureHeight
		}
		if plan.MainFPS <= 0 {
			plan.MainFPS = plan.CaptureFPS
		}
		if plan.MainFormat == "" {
			plan.MainFormat = "h265"
		}
	} else {
		plan.MainWidth, plan.MainHeight, plan.MainFPS = 0, 0, 0
		plan.MainFormat = ""
	}
	if plan.AnalysisEnabled {
		if plan.SubWidth <= 0 {
			plan.SubWidth = plan.CaptureWidth
		}
		if plan.SubHeight <= 0 {
			plan.SubHeight = plan.CaptureHeight
		}
		if plan.SubFPS <= 0 {
			plan.SubFPS = plan.CaptureFPS
		}
		if plan.SubFormat == "" {
			plan.SubFormat = "nv12"
		}
	} else {
		plan.SubWidth, plan.SubHeight, plan.SubFPS = 0, 0, 0
		plan.SubFormat = ""
	}
	if !brokerTransportSupported(plan.Transport) {
		plan.Enabled = false
	}
	if !plan.LiveEnabled && !plan.AnalysisEnabled && !plan.ArchiveEnabled {
		plan.Enabled = false
	}
	if plan.LiveEnabled && plan.MainSocketPath == "" {
		plan.Enabled = false
	}
	if plan.ArchiveEnabled && (plan.ArchiveSocketPath == "" || plan.ArchiveSocketPath == plan.MainSocketPath || plan.ArchiveSocketPath == plan.SubSocketPath) {
		plan.Enabled = false
	}
	if plan.LiveEnabled && !strings.EqualFold(plan.MainFormat, "h264") && !strings.EqualFold(plan.MainFormat, "h265") {
		plan.Enabled = false
	}
	if plan.CaptureWidth <= 0 || plan.CaptureHeight <= 0 || plan.CaptureFPS <= 0 {
		plan.Enabled = false
	}
	if plan.AnalysisEnabled && strings.EqualFold(plan.SubFormat, "nv12") {
		if (plan.SubSocketPath == "" && plan.SubShmPath == "") || plan.SubWidth <= 0 || plan.SubHeight <= 0 {
			plan.Enabled = false
		}
	}
	return plan
}

func brokerAdmittedSurfaces(
	profiles []productconfig.SourceRoleProfile,
	mux videoproviders.BrokerMuxPlan,
	inventory capability.Inventory,
) ([]videoproviders.BrokerMuxSurface, error) {
	if !strings.EqualFold(strings.TrimSpace(mux.Transport), videoproviders.TransportVendorHBN) {
		return nil, nil
	}
	seen := make(map[videoproviders.BrokerMuxSurface]bool, len(profiles))
	surfaces := make([]videoproviders.BrokerMuxSurface, 0, len(profiles))
	for _, profile := range profiles {
		width, height := profile.Width, profile.Height
		if width <= 0 || height <= 0 {
			return nil, fmt.Errorf("catalog %s profile has invalid geometry %dx%d", profile.Role, width, height)
		}
		switch profile.Role {
		case "analysis":
			if capability.NormalizeFormat(profile.PixelFormat) != "nv12" {
				return nil, fmt.Errorf("vendor HBN analysis profile requires NV12")
			}
		case "live", "archive":
			codec := capability.NormalizeFormat(profile.Codec)
			if codec == "auto" && profile.Role == "archive" {
				codec = "h265"
			}
			_, operation, ok := inventory.FindOperation(
				"native_sp", capability.StageEncode, codec, true,
			)
			if !ok {
				return nil, fmt.Errorf(
					"vendor HBN catalog %s profile has no native hardware %s encoder",
					profile.Role, strings.ToUpper(codec),
				)
			}
			width, height = videoproviders.FitArchiveGeometryWithAlignment(
				width, height, mux.CaptureWidth, mux.CaptureHeight,
				operation.WidthAlignment, operation.HeightAlignment,
			)
		default:
			return nil, fmt.Errorf("catalog has unsupported role %q", profile.Role)
		}
		surface := videoproviders.BrokerMuxSurface{Width: width, Height: height}
		if seen[surface] {
			continue
		}
		seen[surface] = true
		surfaces = append(surfaces, surface)
	}
	sort.Slice(surfaces, func(i, j int) bool {
		if surfaces[i].Width != surfaces[j].Width {
			return surfaces[i].Width < surfaces[j].Width
		}
		return surfaces[i].Height < surfaces[j].Height
	})
	return surfaces, nil
}

func brokerTransportSupported(transport string) bool {
	switch strings.ToLower(strings.TrimSpace(transport)) {
	case videoproviders.TransportVendorHBN, videoproviders.TransportV4L2, videoproviders.TransportV4L2UVC, videoproviders.TransportLibcamera:
		return true
	default:
		return false
	}
}

func startVideoBroker(ctx context.Context, plan videoBrokerPlan) (*videoBrokerRuntime, <-chan error) {
	if !plan.Enabled {
		return nil, nil
	}
	resetResourceGovernorStateFile(plan)
	log.Printf(
		"[video-broker] enabled live_socket=%s archive_socket=%s analysis_socket=%s transport=%s capture=%s/%s orientation=%s encode=%s decode=%s live=%s analysis=%s main_write_timeout_ms=%d",
		plan.MainSocketPath,
		plan.ArchiveSocketPath,
		plan.SubSocketPath,
		plan.Transport,
		plan.CaptureDevice,
		plan.CaptureFormat,
		plan.VideoOrientation,
		plan.EncodeProvider,
		plan.DecodeProvider,
		plan.MainFormat,
		plan.SubFormat,
		plan.MainWriteTimeoutMs,
	)
	now := time.Now()
	rt := &videoBrokerRuntime{
		plan:                     plan,
		startupReady:             make(chan error, 1),
		mainClients:              make(map[int]*brokerClient),
		archiveClients:           make(map[int]*brokerClient),
		subClients:               make(map[int]*brokerClient),
		subDispatch:              make(chan brokerSubFrame, brokerSubDispatchQueueDepth),
		logLimiter:               newCoalescedLogLimiter(30 * time.Second),
		createdAt:                now,
		healthState:              "initializing",
		healthChangedAt:          now,
		captureModeHealthEntries: make(map[string]cameraModeHealthEntry),
		captureModeFailures:      make(map[string]int),
		componentRecovery:        make(map[string]hardwareComponentRecovery),
		uvcAdmittedModes:         make(map[string]struct{}),
	}
	rt.modeHealthFingerprint = cameraModeRuntimeFingerprint(plan)
	rt.frameCleanupErr = loadFrameCleanupGuard(plan.StateDirectory)
	if rt.frameCleanupErr != nil {
		log.Printf("[video-broker] event=frame_cleanup_guard_active err=%v action=hold_admission", rt.frameCleanupErr)
	}
	rt.captureModeHealthEntries = loadCameraModeHealthEntries(plan.StateDirectory, rt.modeHealthFingerprint, now)
	rt.refreshCameraModeHealthLocked(now)
	rt.governor = nominalResourceGovernorDecision(plan)
	if plan.ResourceGovernorEnabled {
		// A restarted capture must obey temperature caps from its first child
		// launch, not only after the asynchronous governor's first observation.
		rt.governor = decideResourceGovernor(plan, rt.governor, readThermalZonesCelsius(), nil, now.UTC())
	}
	done := make(chan error, 1)
	go func() {
		done <- rt.run(ctx)
		close(done)
	}()
	return rt, done
}

func (rt *videoBrokerRuntime) healthSnapshot(now time.Time) videoBrokerHealthSnapshot {
	if rt == nil {
		return videoBrokerHealthSnapshot{}
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return rt.healthSnapshotLocked(now)
}

func (rt *videoBrokerRuntime) healthSnapshotLocked(now time.Time) videoBrokerHealthSnapshot {
	if now.IsZero() {
		now = time.Now()
	}
	watchdog := time.Duration(rt.plan.FrameWatchdogSec) * time.Second
	if watchdog <= 0 {
		watchdog = brokerDefaultFrameWatchdog
	}
	startupGrace := brokerStartupFrameGrace(watchdog, time.Duration(rt.plan.WarmupSec)*time.Second)
	admissionGrace := brokerCameraAdmissionGrace(watchdog, time.Duration(rt.plan.WarmupSec)*time.Second)

	state := "initializing"
	lastError := ""
	opticalSignalState := "unmonitored"
	opticalSignalReason := ""
	if rt.plan.QualityMonitorEnabled && rt.plan.AnalysisEnabled && strings.TrimSpace(rt.plan.QualityMonitorStatePath) != "" {
		opticalSignalState = "pending"
	}
	if !rt.childRunning {
		if rt.everProducedFrames || now.Sub(rt.createdAt) >= watchdog {
			state = "degraded"
			lastError = "video broker capture process is not running"
		}
	} else {
		mainRequired := len(rt.mainClients) > 0
		mainFresh := !mainRequired || (!rt.lastMainFrameAt.IsZero() && now.Sub(rt.lastMainFrameAt) < watchdog)
		mainStarting := mainRequired && rt.lastMainFrameAt.IsZero() && !rt.mainDemandedAt.IsZero() && now.Sub(rt.mainDemandedAt) < startupGrace
		subRequired := rt.plan.AnalysisEnabled
		subFresh := !subRequired || (!rt.lastSubFrameAt.IsZero() && now.Sub(rt.lastSubFrameAt) < watchdog)
		archiveRequired := rt.plan.ArchiveEnabled
		archiveFresh := !archiveRequired || (!rt.lastArchiveFrameAt.IsZero() && now.Sub(rt.lastArchiveFrameAt) < watchdog)
		archiveStarting := archiveRequired && rt.lastArchiveFrameAt.IsZero() && !rt.childStartedAt.IsZero() && now.Sub(rt.childStartedAt) < startupGrace
		if mainFresh && subFresh && archiveFresh {
			state = "running"
		} else if (mainStarting || mainFresh) && subFresh && (archiveStarting || archiveFresh) {
			state = "initializing"
		} else if !rt.childStartedAt.IsZero() && now.Sub(rt.childStartedAt) >= startupGrace {
			state = "degraded"
			stale := []string{}
			if mainRequired && !mainFresh {
				stale = append(stale, "main")
			}
			if !subFresh {
				stale = append(stale, "sub")
			}
			if !archiveFresh {
				stale = append(stale, "archive")
			}
			lastError = "video broker frame stream is stale: " + strings.Join(stale, ",")
		}
		if rt.cameraAdmission.DevicePath != "" && !rt.cameraAdmitted {
			state = "initializing"
			lastError = ""
			if !rt.childStartedAt.IsZero() && now.Sub(rt.childStartedAt) >= admissionGrace {
				state = "degraded"
				lastError = "camera runtime attestation is incomplete"
			}
		}
	}
	if state == "running" && rt.plan.QualityMonitorEnabled && rt.plan.AnalysisEnabled && strings.TrimSpace(rt.plan.QualityMonitorStatePath) != "" {
		quality, err := readVideoQualitySignalState(rt.plan, now)
		if err != nil {
			opticalSignalState = "unavailable"
			opticalSignalReason = err.Error()
			if !rt.childStartedAt.IsZero() && now.Sub(rt.childStartedAt) < startupGrace {
				state = "initializing"
				lastError = ""
			} else {
				state = "degraded"
				lastError = "video quality evidence is unavailable: " + err.Error()
			}
		} else if !quality.Source.SignalInformative {
			// Darkness, a covered lens, fog or an overexposed scene describe the
			// optical input, not the health of the capture graph.  Keep transport,
			// cadence and codec health independent so an unattended release cannot
			// roll back merely because the scene contains no usable detail.
			opticalSignalState = "non_informative"
			opticalSignalReason = fmt.Sprintf(
				"black_ratio=%.5f white_ratio=%.5f",
				quality.Source.BlackRatio,
				quality.Source.WhiteRatio,
			)
		} else {
			opticalSignalState = "informative"
		}
	}
	// A source with fresh valid frames remains available. Under-production is
	// exposed per stream and must not trigger source recovery or rollback.
	if state != rt.healthState || lastError != rt.healthError {
		rt.healthState = state
		rt.healthError = lastError
		rt.healthChangedAt = now
	}
	return videoBrokerHealthSnapshot{
		State:               rt.healthState,
		LastError:           rt.healthError,
		LastChangeAt:        rt.healthChangedAt,
		OpticalSignalState:  opticalSignalState,
		OpticalSignalReason: opticalSignalReason,
	}
}

func brokerStartupFrameGrace(watchdog, warmup time.Duration) time.Duration {
	if watchdog <= 0 {
		watchdog = brokerDefaultFrameWatchdog
	}
	if warmup < 0 {
		warmup = 0
	}
	// Codec discovery and allocation happen before warm-up starts. Keep the
	// steady-state watchdog strict while granting one warm-up interval to the
	// first frame only.
	return watchdog + warmup
}

func brokerCameraAdmissionGrace(watchdog, warmup time.Duration) time.Duration {
	startupGrace := brokerStartupFrameGrace(watchdog, warmup)
	if watchdog <= 0 {
		watchdog = brokerDefaultFrameWatchdog
	}
	// Camera admission proves steady-state codec and cadence, not the temporary
	// throughput observed while models and hardware contexts are starting. Give
	// it two watchdog periods after normal warm-up, with a useful lower bound
	// for short custom watchdogs and a bounded upper limit for recovery.
	proofWindow := 2 * watchdog
	if proofWindow < 15*time.Second {
		proofWindow = 15 * time.Second
	}
	if proofWindow > 60*time.Second {
		proofWindow = 60 * time.Second
	}
	return startupGrace + proofWindow
}

func readVideoQualitySignalState(plan videoBrokerPlan, now time.Time) (quality.State, error) {
	path := strings.TrimSpace(plan.QualityMonitorStatePath)
	data, err := brokerReadFile(path)
	if err != nil {
		return quality.State{}, err
	}
	state, err := quality.Decode(data)
	if err != nil {
		return quality.State{}, fmt.Errorf("invalid state: %w", err)
	}
	reportEvery := plan.QualityMonitorReportEvery
	if reportEvery <= 0 {
		reportEvery = 1
	}
	fps := plan.CaptureFPS
	if fps <= 0 {
		fps = brokerDefaultCadenceFPS
	}
	maxAge := (2 * time.Duration(reportEvery) * time.Second / time.Duration(fps)) + 5*time.Second
	if maxAge < 10*time.Second {
		maxAge = 10 * time.Second
	}
	if maxAge > 2*time.Minute {
		maxAge = 2 * time.Minute
	}
	measuredAt := time.UnixMilli(state.TimestampUnixMS)
	if measuredAt.After(now.Add(5 * time.Second)) {
		return quality.State{}, errors.New("timestamp is in the future")
	}
	if now.Sub(measuredAt) > maxAge {
		return quality.State{}, fmt.Errorf("stale state age=%.1fs max=%.1fs", now.Sub(measuredAt).Seconds(), maxAge.Seconds())
	}
	return state, nil
}

func resetVideoQualityStateFile(opt engineConfig) {
	if !opt.VideoQualityMonitorEnabled {
		return
	}
	path := strings.TrimSpace(opt.VideoQualityMonitorStatePath)
	if path == "" {
		return
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		log.Printf("[video-broker] warning: quality state reset failed path=%s err=%v", path, err)
	}
}

func (rt *videoBrokerRuntime) run(ctx context.Context) (runErr error) {
	defer func() { rt.signalStartup(runErr) }()
	rt.lifecycleMu.Lock()
	rt.runtimeContext = ctx
	rt.lifecycleMu.Unlock()
	var mainLn *unixsocket.Publication
	var err error
	if rt.plan.LiveEnabled {
		mainLn, err = listenBrokerSocket(rt.plan.MainSocketPath)
		if err != nil {
			return fmt.Errorf("video broker live listen %s: %w", rt.plan.MainSocketPath, err)
		}
		rt.mainPublication = mainLn
		defer mainLn.Cleanup()
	}

	var archiveLn *unixsocket.Publication
	if rt.plan.ArchiveEnabled {
		archiveLn, err = listenBrokerSocket(rt.plan.ArchiveSocketPath)
		if err != nil {
			return fmt.Errorf("video broker archive listen %s: %w", rt.plan.ArchiveSocketPath, err)
		}
		rt.archivePublication = archiveLn
		defer archiveLn.Cleanup()
	}

	var subLn *unixsocket.Publication
	if rt.plan.AnalysisEnabled && strings.TrimSpace(rt.plan.SubSocketPath) != "" {
		subLn, err = listenBrokerSocket(rt.plan.SubSocketPath)
		if err != nil {
			return fmt.Errorf("video broker sub listen %s: %w", rt.plan.SubSocketPath, err)
		}
		rt.subPublication = subLn
		defer subLn.Cleanup()
	}

	if rt.plan.FrameCapture == nil && rt.plan.AnalysisEnabled && rt.plan.SubShmPath != "" && strings.EqualFold(rt.plan.SubFormat, "nv12") {
		payloadCap := (rt.plan.SubWidth * rt.plan.SubHeight * 3) / 2
		subShared, err := newBrokerSubSharedFrame(rt.plan.SubShmPath, payloadCap)
		if err != nil {
			return fmt.Errorf("video broker sub shared frame init %s: %w", rt.plan.SubShmPath, err)
		} else {
			rt.subShared = subShared
			rt.subNotifier = newBrokerSubFrameNotifier(rt.plan.SubNotifySocketPath)
		}
	}
	if rt.subDispatch == nil {
		rt.subDispatch = make(chan brokerSubFrame, brokerSubDispatchQueueDepth)
	}
	// The dispatcher is a source-local, bounded idle goroutine. Keeping it
	// alive for every running source lets a transaction attach analysis without
	// restarting capture; disabled roles are still rejected in dispatchFrame.
	rt.startGoroutine(func() { rt.forwardSubFrames(ctx) })

	rt.mu.Lock()
	if err := rt.ensureChildLocked(ctx); err != nil {
		rt.logf("[video-broker] eager child start failed err=%v", err)
		rt.scheduleChildRecoveryLocked(ctx)
	}
	rt.mu.Unlock()

	if mainLn != nil {
		cancel, err := rt.startPublicationAcceptLoop(ctx, mainLn, brokerStreamH264)
		if err != nil {
			return fmt.Errorf("video broker live accept loop: %w", err)
		}
		rt.mu.Lock()
		rt.mainPublicationCancel = cancel
		rt.mu.Unlock()
	}
	if archiveLn != nil {
		cancel, err := rt.startPublicationAcceptLoop(ctx, archiveLn, brokerEndpointArchive)
		if err != nil {
			return fmt.Errorf("video broker archive accept loop: %w", err)
		}
		rt.mu.Lock()
		rt.archivePublicationCancel = cancel
		rt.mu.Unlock()
	}
	if subLn != nil {
		cancel, err := rt.startPublicationAcceptLoop(ctx, subLn, brokerStreamNV12)
		if err != nil {
			return fmt.Errorf("video broker analysis accept loop: %w", err)
		}
		rt.mu.Lock()
		rt.subPublicationCancel = cancel
		rt.mu.Unlock()
	}
	if rt.plan.ResourceGovernorEnabled {
		rt.startGoroutine(func() { rt.runResourceGovernor(ctx) })
	}
	rt.signalStartup(nil)

	<-ctx.Done()
	rt.lifecycleMu.Lock()
	rt.stopping = true
	rt.lifecycleMu.Unlock()
	if mainLn != nil {
		_ = mainLn.Close()
	}
	if archiveLn != nil {
		_ = archiveLn.Close()
	}
	if subLn != nil {
		_ = subLn.Close()
	}
	rt.mu.Lock()
	rt.stopChildLocked()
	for id := range rt.mainClients {
		rt.detachClientLocked(brokerStreamH264, id)
	}
	for id := range rt.archiveClients {
		rt.detachClientLocked(brokerEndpointArchive, id)
	}
	for id := range rt.subClients {
		rt.detachClientLocked(brokerStreamNV12, id)
	}
	rt.mu.Unlock()
	rt.wg.Wait()
	rt.mu.Lock()
	mainPublication := rt.mainPublication
	archivePublication := rt.archivePublication
	subPublication := rt.subPublication
	rt.mu.Unlock()
	if mainPublication != nil {
		mainPublication.Cleanup()
	}
	if archivePublication != nil {
		archivePublication.Cleanup()
	}
	if subPublication != nil {
		subPublication.Cleanup()
	}
	if rt.subShared != nil {
		_ = rt.subShared.Close()
	}
	if rt.subNotifier != nil {
		_ = rt.subNotifier.Close()
	}
	resetResourceGovernorStateFile(rt.plan)
	return ctx.Err()
}

func (rt *videoBrokerRuntime) signalStartup(err error) {
	if rt == nil {
		return
	}
	rt.startupOnce.Do(func() { rt.startupReady <- err })
}

func (rt *videoBrokerRuntime) waitStartup(ctx context.Context) error {
	if rt == nil || rt.startupReady == nil {
		return errors.New("video broker runtime has no startup boundary")
	}
	select {
	case err := <-rt.startupReady:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func listenBrokerSocket(path string) (*unixsocket.Publication, error) {
	return unixsocket.Listen(path)
}

func (rt *videoBrokerRuntime) acceptLoop(ctx context.Context, publication *unixsocket.Publication, streamID byte) {
	for {
		conn, err := publication.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("[video-broker] accept failed stream=%d err=%v", streamID, err)
			continue
		}
		if err := rt.attachClient(ctx, publication, streamID, conn); err != nil {
			rt.logf("[video-broker] attach failed stream=%d err=%v", streamID, err)
			_ = conn.Close()
		}
	}
}

func (rt *videoBrokerRuntime) startPublicationAcceptLoop(
	parent context.Context,
	publication *unixsocket.Publication,
	streamID byte,
) (context.CancelFunc, error) {
	if parent == nil || publication == nil {
		return nil, errors.New("video broker publication accept loop is incomplete")
	}
	ctx, cancel := context.WithCancel(parent)
	if !rt.startGoroutine(func() { rt.acceptLoop(ctx, publication, streamID) }) {
		cancel()
		_ = publication.Close()
		return nil, errors.New("video broker runtime is stopping")
	}
	go func() {
		<-ctx.Done()
		_ = publication.Close()
	}()
	return cancel, nil
}

func (rt *videoBrokerRuntime) startFanoutPublicationAcceptLoopLocked(
	publication *unixsocket.Publication,
	streamID byte,
) (context.CancelFunc, error) {
	rt.lifecycleMu.Lock()
	parent, stopping := rt.runtimeContext, rt.stopping
	rt.lifecycleMu.Unlock()
	if stopping || parent == nil || parent.Err() != nil {
		return nil, errors.New("video broker runtime is stopping")
	}
	return rt.startPublicationAcceptLoop(parent, publication, streamID)
}

func (rt *videoBrokerRuntime) attachClient(ctx context.Context, publication *unixsocket.Publication, streamID byte, conn net.Conn) error {
	rt.mu.Lock()
	if ctx == nil || ctx.Err() != nil {
		rt.mu.Unlock()
		return errors.New("video broker publication accept context is no longer active")
	}
	var current *unixsocket.Publication
	var path string
	var enabled bool
	switch streamID {
	case brokerStreamH264:
		current, path, enabled = rt.mainPublication, rt.plan.MainSocketPath, rt.plan.LiveEnabled
	case brokerEndpointArchive:
		current, path, enabled = rt.archivePublication, rt.plan.ArchiveSocketPath, rt.plan.ArchiveEnabled
	case brokerStreamNV12:
		current, path, enabled = rt.subPublication, rt.plan.SubSocketPath, rt.plan.AnalysisEnabled
	}
	// Accept may return before cutover and acquire this lock only after the
	// incumbent clients have been detached. Fence that queued connection by
	// publication identity before it can recreate demand on the old source.
	if !enabled || publication == nil || current != publication || !publication.Owns(path) {
		rt.mu.Unlock()
		return errors.New("video broker client publication is no longer current")
	}
	clientID := rt.nextClientID
	rt.nextClientID++
	client := newBrokerClient(
		streamID,
		conn,
		time.Duration(rt.plan.MainWriteTimeoutMs)*time.Millisecond,
		rt.brokerClientFPSLocked(streamID),
	)

	switch streamID {
	case brokerStreamH264:
		firstMainClient := len(rt.mainClients) == 0 && !rt.routingLiveDemand
		if firstMainClient {
			rt.mainDemandedAt = time.Now()
			if rt.childControlSupported {
				rt.mainAwaitingEncoderEpoch = true
				rt.armMainKeyframeGateLocked()
			}
		}
		rt.mainClients[clientID] = client
		client.mainControl = videoMainClientControl{
			TargetFPS:         rt.plan.MainFPS,
			TargetBitrateKbps: rt.plan.LiveBitrateKbps,
		}
	case brokerEndpointArchive:
		rt.archiveClients[clientID] = client
	case brokerStreamNV12:
		rt.subClients[clientID] = client
	default:
		rt.mu.Unlock()
		return errors.New("unknown broker stream")
	}

	if err := rt.ensureChildLocked(ctx); err != nil {
		rt.detachClientLocked(streamID, clientID)
		rt.mu.Unlock()
		return err
	}
	var bootstrap [][]byte
	if streamID == brokerStreamH264 {
		bootstrap = append([][]byte(nil), rt.mainBootstrap...)
		rt.lastMainFrameAt = time.Time{}
		rt.queueBridgeControlLocked("h264_client_attached")
	} else if streamID == brokerEndpointArchive {
		bootstrap = append([][]byte(nil), rt.archiveBootstrap...)
	}
	rt.mu.Unlock()
	// startGoroutine refuses while the runtime is stopping. The client is
	// already registered by this point, so ignoring that refusal left the
	// connection attached and unserved until the process exited: no write loop,
	// no watcher, and acceptLoop only closes the connection when attachClient
	// reports a failure. Detach and report, so the connection is released.
	writeLoopStarted := rt.startGoroutine(func() { client.writeLoopWithBootstrap(bootstrap) })
	watcherStarted := false
	if writeLoopStarted {
		if streamID == brokerStreamH264 {
			watcherStarted = rt.startGoroutine(func() { rt.watchMainClientControl(clientID, conn) })
		} else {
			watcherStarted = rt.startGoroutine(func() { rt.watchClient(streamID, clientID, conn) })
		}
	}
	if !writeLoopStarted || !watcherStarted {
		rt.mu.Lock()
		rt.detachClientLocked(streamID, clientID)
		rt.mu.Unlock()
		return errors.New("video broker is stopping: client detached before it was served")
	}
	return nil
}

func (rt *videoBrokerRuntime) brokerClientFPSLocked(streamID byte) int {
	if streamID != brokerEndpointArchive {
		return rt.plan.CaptureFPS
	}
	fps := rt.plan.ArchiveFPS
	if rt.observedArchiveFPS > fps {
		fps = rt.observedArchiveFPS
	}
	if fps <= 0 {
		fps = rt.plan.CaptureFPS
	}
	return fps
}

func (rt *videoBrokerRuntime) watchClient(streamID byte, clientID int, conn net.Conn) {
	buf := make([]byte, 1)
	for {
		if _, err := conn.Read(buf); err != nil {
			rt.mu.Lock()
			rt.detachClientLocked(streamID, clientID)
			rt.mu.Unlock()
			return
		}
	}
}

func (rt *videoBrokerRuntime) detachClientLocked(streamID byte, clientID int) {
	switch streamID {
	case brokerStreamH264:
		if client, ok := rt.mainClients[clientID]; ok {
			previousControl := rt.bridgeControlTargetsLocked()
			client.close()
			delete(rt.mainClients, clientID)
			if len(rt.mainClients) == 0 && !rt.routingLiveDemand {
				rt.mainDemandedAt = time.Time{}
				if rt.childControlSupported {
					rt.mainAwaitingEncoderEpoch = false
					rt.armMainKeyframeGateLocked()
				}
			} else if rt.childControlSupported {
				currentControl := rt.bridgeControlTargetsLocked()
				if liveEncoderActivationChanged(previousControl, currentControl) {
					rt.mainAwaitingEncoderEpoch = true
					rt.armMainKeyframeGateLocked()
				}
			}
			rt.queueBridgeControlLocked("h264_client_detached")
		}
	case brokerEndpointArchive:
		if client, ok := rt.archiveClients[clientID]; ok {
			reason := client.failure()
			if reason != "" {
				rt.metrics.archiveClientDisconnects++
				rt.totalArchiveDisconnects++
				rt.logf(
					"[video-broker] event=broker_client_disconnected endpoint=archive incident_id=%s media_epoch=%d reason=%s client_id=%d queue_depth=%d queue_bytes=%d queue_limit_bytes=%d total_disconnects=%d",
					captureIncidentID(rt.mediaEpoch),
					rt.mediaEpoch,
					reason,
					clientID,
					len(client.send),
					atomic.LoadInt64(&client.queuedBytes),
					client.queueBytesLimit,
					rt.totalArchiveDisconnects,
				)
			}
			client.close()
			delete(rt.archiveClients, clientID)
		}
	case brokerStreamNV12:
		if client, ok := rt.subClients[clientID]; ok {
			client.close()
			delete(rt.subClients, clientID)
		}
	}
}

func (rt *videoBrokerRuntime) replayBootstrapLocked(conn net.Conn) error {
	timeout := time.Duration(rt.plan.MainWriteTimeoutMs) * time.Millisecond
	if timeout <= 0 {
		timeout = brokerDefaultMainWriteTimeout
	}
	for _, packet := range rt.mainBootstrap {
		_ = conn.SetWriteDeadline(time.Now().Add(timeout))
		if err := writeBrokerPacket(conn, packet); err != nil {
			return err
		}
	}
	_ = conn.SetWriteDeadline(time.Time{})
	return nil
}

func (rt *videoBrokerRuntime) startGoroutine(fn func()) bool {
	rt.lifecycleMu.Lock()
	if rt.stopping {
		rt.lifecycleMu.Unlock()
		return false
	}
	rt.wg.Add(1)
	rt.lifecycleMu.Unlock()
	go func() {
		defer rt.wg.Done()
		fn()
	}()
	return true
}

func (rt *videoBrokerRuntime) ensureChildLocked(ctx context.Context) (result error) {
	if rt.frameCleanupErr != nil {
		return fmt.Errorf("frame source cleanup unconfirmed: %w", rt.frameCleanupErr)
	}
	if rt.childRunning {
		return nil
	}
	rt.drainSubFramesLocked()
	now := time.Now()
	if !rt.nextChildStartAt.IsZero() && now.Before(rt.nextChildStartAt) {
		return fmt.Errorf("video broker child start deferred retry_after=%s", rt.nextChildStartAt.Sub(now).Round(time.Second))
	}
	rt.refreshCameraModeHealthLocked(now)
	engineDirectory := resolveEngineDirectory(rt.plan.EngineDirectory)
	bridgePath := strings.TrimSpace(os.Getenv(rdkBrokerMuxBridgeEnv))
	if bridgePath == "" {
		bridgePath = filepath.Join(engineDirectory, "rdkx5_hbn_mux_bridge")
	}
	v4l2BridgePath := strings.TrimSpace(os.Getenv(rdkBrokerV4L2MuxBridgeEnv))
	if v4l2BridgePath == "" {
		v4l2BridgePath = filepath.Join(engineDirectory, "rdkx5_v4l2_mux_bridge")
	}
	gstBridgePath := strings.TrimSpace(os.Getenv(gstVideoIngestBridgeEnv))
	if gstBridgePath == "" {
		gstBridgePath = filepath.Join(engineDirectory, "video-ingest-gst")
	}
	captureFPS := rt.plan.CaptureFPS
	sensorWidth := rt.plan.CaptureWidth
	sensorHeight := rt.plan.CaptureHeight
	captureDevice := resolveVideoBrokerCaptureDevice(rt.plan)
	captureFormat := strings.TrimSpace(rt.plan.CaptureFormat)
	capturePolicy := strings.TrimSpace(rt.plan.CapturePolicy)
	captureCodecPolicy := strings.TrimSpace(rt.plan.CaptureCodecPolicy)
	captureAspectPolicy := strings.TrimSpace(rt.plan.CaptureAspectPolicy)
	allowScaledYUYV := rt.plan.AllowScaledYUYV
	preferExactYUYV := rt.plan.PreferExactYUYV
	qualityMonitorEnabled := rt.plan.QualityMonitorEnabled
	qualityMonitorReportEvery := rt.plan.QualityMonitorReportEvery
	if rt.plan.ToneMappingEnabled {
		// CameraControl consumes this metric as feedback.  A deployment environment may
		// retain an older diagnostic cadence, but it must not slow the closed
		// loop below one sample per second of the emitted NV12 stream.
		maximumCadence := rt.plan.SubFPS
		if maximumCadence <= 0 {
			maximumCadence = captureFPS
		}
		if maximumCadence <= 0 {
			maximumCadence = 30
		}
		if qualityMonitorReportEvery <= 0 || qualityMonitorReportEvery > maximumCadence {
			qualityMonitorReportEvery = maximumCadence
		}
	}
	qualityMonitorSampleStride := rt.plan.QualityMonitorSampleStride
	qualityMonitorStatePath := strings.TrimSpace(rt.plan.QualityMonitorStatePath)
	cameraAdmissionStatePath := strings.TrimSpace(rt.plan.CameraAdmissionStatePath)
	warmupSec := rt.plan.WarmupSec
	if warmupSec < 0 {
		warmupSec = 0
	}
	captureSelectTimeoutMs := rt.plan.CaptureSelectTimeoutMs
	if captureSelectTimeoutMs <= 0 {
		captureSelectTimeoutMs = brokerDefaultCaptureSelectTimeoutMs
	}
	captureMaxTimeouts := rt.plan.CaptureMaxTimeouts
	if captureMaxTimeouts <= 0 {
		captureMaxTimeouts = brokerDefaultCaptureMaxTimeouts
	}
	frameWatchdogSec := rt.plan.FrameWatchdogSec
	if frameWatchdogSec <= 0 {
		frameWatchdogSec = int(brokerDefaultFrameWatchdog / time.Second)
	}
	mainWriteTimeoutMs := rt.plan.MainWriteTimeoutMs
	if mainWriteTimeoutMs <= 0 {
		mainWriteTimeoutMs = int(brokerDefaultMainWriteTimeout / time.Millisecond)
	}
	rt.plan.MainWriteTimeoutMs = mainWriteTimeoutMs
	liveBitrateKbps := rt.plan.LiveBitrateKbps
	if liveBitrateKbps <= 0 {
		liveBitrateKbps = estimateBrokerBitrateKbps(rt.plan.MainWidth, rt.plan.MainHeight, rt.plan.MainFPS)
	}
	rt.mainBitrateKbps = liveBitrateKbps
	archiveBitrateKbps := rt.plan.ArchiveBitrateKbps
	if archiveBitrateKbps <= 0 {
		archiveBitrateKbps = estimateBrokerBitrateKbps(
			positiveOr(rt.plan.ArchiveWidth, sensorWidth),
			positiveOr(rt.plan.ArchiveHeight, sensorHeight),
			positiveOr(rt.plan.ArchiveFPS, captureFPS),
		)
	}
	rt.archiveBitrateKbps = archiveBitrateKbps
	if qualityMonitorEnabled && qualityMonitorStatePath != "" {
		if err := os.Remove(qualityMonitorStatePath); err != nil && !errors.Is(err, os.ErrNotExist) {
			log.Printf("[video-broker] warning: quality state reset failed path=%s err=%v", qualityMonitorStatePath, err)
		}
	}
	childEpoch := rt.mediaEpoch + 1
	if childEpoch == 0 {
		childEpoch = 1
	}
	muxPlan := videoproviders.BrokerMuxPlan{
		Transport:              rt.plan.Transport,
		SourceID:               rt.plan.SourceID,
		MediaEpoch:             childEpoch,
		HBNBridgePath:          bridgePath,
		V4L2BridgePath:         v4l2BridgePath,
		GStreamerBridgePath:    gstBridgePath,
		CaptureDevicePath:      captureDevice,
		CaptureFormat:          captureFormat,
		CaptureCodecPreference: rt.plan.CaptureCodecPreference,
		CaptureCodecPolicy:     captureCodecPolicy,
		CapturePolicy:          capturePolicy,
		CaptureAspectPolicy:    captureAspectPolicy,
		CaptureModeExclusions:  append([]videoproviders.V4L2CaptureModeExclusion(nil), rt.captureModeExclusions...),
		AllowScaledYUYV:        allowScaledYUYV,
		PreferExactYUYV:        preferExactYUYV,
		VideoOrientation:       rt.plan.VideoOrientation,
		EncodeProvider:         rt.plan.EncodeProvider,
		ArchiveEncodeProvider:  rt.plan.ArchiveEncodeProvider,
		DecodeProvider:         rt.plan.DecodeProvider,
		RequireHWCodec:         rt.plan.RequireHWCodec,
		SensorPrefix:           deriveBrokerSensorPrefix(rt.plan.Sensor),
		CameraHostIndex:        deriveBrokerVideoIndex(rt.plan.CameraHostIndex),
		CaptureWidth:           sensorWidth,
		CaptureHeight:          sensorHeight,
		CaptureFPS:             captureFPS,
		LiveEnabled:            rt.plan.LiveEnabled,
		LiveCodec:              rt.plan.MainFormat,
		AnalysisEnabled:        rt.plan.AnalysisEnabled,
		AnalysisStreamID:       rt.plan.AnalysisStreamID,
		ArchiveEnabled:         rt.plan.ArchiveEnabled,
		ArchiveWidth:           positiveOr(rt.plan.ArchiveWidth, sensorWidth),
		ArchiveHeight:          positiveOr(rt.plan.ArchiveHeight, sensorHeight),
		ArchiveFPS:             positiveOr(rt.plan.ArchiveFPS, captureFPS),
		ArchiveRateControl:     rt.plan.ArchiveRateControl,
		ArchiveContent:         rt.plan.ArchiveContent,
		ArchiveCodec:           rt.plan.ArchiveCodec,
		AllowSWFallback:        rt.plan.AllowSWFallback,
		H264EncodeHWAvailable:  rt.plan.H264EncodeHWAvailable,
		H264DecodeHWAvailable:  rt.plan.H264DecodeHWAvailable,
		H265EncodeHWAvailable:  rt.plan.H265EncodeHWAvailable,
		H265DecodeHWAvailable:  rt.plan.H265DecodeHWAvailable,
		MJPEGDecodeHWAvailable: rt.plan.MJPEGDecodeHWAvailable,
		MJPEGDecodeInPlaceRetrySupported: rt.plan.CapabilityInventory.Supports(
			"native_sp", capability.StageDecode, "mjpeg", true),
		MainWidth:              rt.plan.MainWidth,
		MainHeight:             rt.plan.MainHeight,
		MainFPS:                rt.plan.MainFPS,
		SubWidth:               rt.plan.SubWidth,
		SubHeight:              rt.plan.SubHeight,
		SubFPS:                 rt.plan.SubFPS,
		WarmupSec:              warmupSec,
		LiveBitrateKbps:        liveBitrateKbps,
		ArchiveBitrateKbps:     archiveBitrateKbps,
		MainActive:             rt.plan.LiveEnabled,
		CaptureSelectTimeoutMs: captureSelectTimeoutMs,
		CaptureMaxTimeouts:     captureMaxTimeouts,

		QualityMonitorEnabled:         qualityMonitorEnabled,
		QualityMonitorReportEvery:     qualityMonitorReportEvery,
		QualityMonitorSampleStride:    qualityMonitorSampleStride,
		QualityMonitorStatePath:       qualityMonitorStatePath,
		ToneMappingEnabled:            rt.plan.ToneMappingEnabled,
		ToneMappingTarget:             rt.plan.ToneMappingTarget,
		ToneMappingHysteresis:         rt.plan.ToneMappingHysteresis,
		ToneMappingMode:               rt.plan.ToneMappingMode,
		ToneMappingAccelerator:        rt.plan.ToneMappingAccelerator,
		ToneMappingGridColumns:        rt.plan.ToneMappingGridColumns,
		ToneMappingGridRows:           rt.plan.ToneMappingGridRows,
		ToneMappingStatisticsHz:       rt.plan.ToneMappingStatisticsHz,
		ToneMappingMaxShadowGain:      rt.plan.ToneMappingMaxShadowGain,
		ToneMappingMaxHighlightReduce: rt.plan.ToneMappingMaxHighlightReduce,
		ToneMappingTemporalAlpha:      rt.plan.ToneMappingTemporalAlpha,
		ToneMappingNoiseGuard:         rt.plan.ToneMappingNoiseGuard,
	}
	effectiveInventory := rt.effectiveCapabilityInventoryLocked(now)
	muxPlan.H264EncodeHWAvailable = effectiveInventory.HardwareAvailable(capability.StageEncode, "h264")
	muxPlan.H264DecodeHWAvailable = effectiveInventory.HardwareAvailable(capability.StageDecode, "h264")
	muxPlan.H265EncodeHWAvailable = effectiveInventory.HardwareAvailable(capability.StageEncode, "h265")
	muxPlan.H265DecodeHWAvailable = effectiveInventory.HardwareAvailable(capability.StageDecode, "h265")
	muxPlan.MJPEGDecodeHWAvailable = effectiveInventory.HardwareAvailable(capability.StageDecode, "mjpeg")
	rt.plan.CameraAdmissionStatePath = cameraAdmissionStatePath
	var (
		prepared     videoproviders.BrokerMuxPlan
		admission    videoproviders.CameraAdmissionDecision
		admissionErr error
	)
	switch strings.ToLower(strings.TrimSpace(muxPlan.Transport)) {
	case videoproviders.TransportV4L2UVC, videoproviders.TransportV4L2:
		prepared, admission, admissionErr = videoproviders.PrepareV4L2UVCCameraAdmission(ctx, muxPlan)
	case videoproviders.TransportVendorHBN, videoproviders.TransportLibcamera:
		prepared, admission, admissionErr = videoproviders.PrepareDeclaredCameraAdmission(muxPlan)
	default:
		admissionErr = fmt.Errorf("unsupported camera admission transport %q", muxPlan.Transport)
	}
	rt.cameraAdmission = admission
	rt.cameraAdmitted = false
	rt.cameraAdmissionTracker = cameraAdmissionRuntimeTracker{}
	if admissionErr != nil {
		rt.writeCameraAdmissionStateLocked("rejected", admissionErr.Error())
		rt.noteChildStartFailureLocked()
		return admissionErr
	}
	muxPlan = prepared
	resolvedArchiveCodec, err := resolveBrokerArchiveCodec(muxPlan, effectiveInventory)
	if err != nil {
		rt.writeCameraAdmissionStateLocked("rejected", "archive_codec_plan_invalid: "+err.Error())
		rt.noteChildStartFailureLocked()
		return err
	}
	muxPlan.ArchiveCodec = resolvedArchiveCodec
	liveCodec := ""
	if muxPlan.LiveEnabled {
		liveCodec = capability.NormalizeFormat(muxPlan.LiveCodec)
	}
	policy := capability.ProviderPolicy{
		EncodeProvider:        rt.plan.EncodeProvider,
		DecodeProvider:        rt.plan.DecodeProvider,
		ToneMapAccelerator:    rt.plan.ToneMappingAccelerator,
		RequireHardwareCodec:  rt.plan.RequireHWCodec,
		AllowSoftwareFallback: rt.plan.AllowSWFallback,
	}
	requirements := capability.Requirements{
		Transport:     muxPlan.Transport,
		CaptureFormat: muxPlan.CaptureFormat,
		LiveCodec:     liveCodec,
		LiveEncode:    brokerLiveEncodeRequired(muxPlan),
		ArchiveCodec:  muxPlan.ArchiveCodec,
		ArchiveEncode: brokerArchiveEncodeRequired(muxPlan),
		ToneMap:       muxPlan.ToneMappingEnabled,
		ResizeRoles:   brokerPixelTransformRoles(muxPlan),
	}
	var capabilityPlan capability.Plan
	if rt.plan.CapabilityAllocator != nil {
		capabilityPlan, err = rt.plan.CapabilityAllocator.resolve(rt.plan.SourceID, effectiveInventory, policy, requirements)
	} else {
		capabilityPlan, err = capability.Resolve(effectiveInventory, policy, requirements)
	}
	if err != nil {
		rt.writeCameraAdmissionStateLocked("rejected", "capability_plan_invalid: "+err.Error())
		rt.noteChildStartFailureLocked()
		return err
	}
	muxPlan.EncodeProvider = capabilityPlan.EncodeProvider
	muxPlan.ArchiveEncodeProvider = capabilityPlan.ArchiveEncodeProvider
	muxPlan.DecodeProvider = capabilityPlan.DecodeProvider
	muxPlan.WorkerProvider = capabilityPlan.WorkerProvider
	muxPlan.MainWidthAlignment, muxPlan.MainHeightAlignment = capabilityEncodeAlignmentForRole(
		capabilityPlan,
		"live",
		liveCodec,
	)
	muxPlan.ArchiveWidthAlignment, muxPlan.ArchiveHeightAlignment = capabilityEncodeAlignmentForRole(
		capabilityPlan,
		"archive",
		muxPlan.ArchiveCodec,
	)
	if brokerLiveEncodeRequired(muxPlan) {
		previousWidth, previousHeight := muxPlan.MainWidth, muxPlan.MainHeight
		muxPlan.MainWidth, muxPlan.MainHeight = videoproviders.FitArchiveGeometryWithAlignment(
			previousWidth,
			previousHeight,
			previousWidth,
			previousHeight,
			muxPlan.MainWidthAlignment,
			muxPlan.MainHeightAlignment,
		)
		if muxPlan.MainWidth != previousWidth || muxPlan.MainHeight != previousHeight {
			log.Printf(
				"[video-broker] live geometry normalized requested=%dx%d selected=%dx%d alignment=%dx%d provider=%s",
				previousWidth, previousHeight, muxPlan.MainWidth, muxPlan.MainHeight,
				muxPlan.MainWidthAlignment, muxPlan.MainHeightAlignment, capabilityPlan.EncodeProvider,
			)
		}
	}
	if brokerArchiveEncodeRequired(muxPlan) {
		previousWidth, previousHeight := muxPlan.ArchiveWidth, muxPlan.ArchiveHeight
		muxPlan.ArchiveWidth, muxPlan.ArchiveHeight = videoproviders.FitArchiveGeometryWithAlignment(
			previousWidth,
			previousHeight,
			muxPlan.CaptureWidth,
			muxPlan.CaptureHeight,
			muxPlan.ArchiveWidthAlignment,
			muxPlan.ArchiveHeightAlignment,
		)
		if muxPlan.ArchiveWidth != previousWidth || muxPlan.ArchiveHeight != previousHeight {
			log.Printf(
				"[video-broker] archive geometry normalized requested=%dx%d selected=%dx%d alignment=%dx%d provider=%s codec=%s",
				previousWidth, previousHeight, muxPlan.ArchiveWidth, muxPlan.ArchiveHeight,
				muxPlan.ArchiveWidthAlignment, muxPlan.ArchiveHeightAlignment,
				capabilityPlan.ArchiveEncodeProvider, muxPlan.ArchiveCodec,
			)
		}
	}
	muxPlan.AdmittedSurfaces, err = brokerAdmittedSurfaces(
		rt.plan.AdmissionProfiles, muxPlan, effectiveInventory,
	)
	if err != nil {
		rt.writeCameraAdmissionStateLocked("rejected", "routing_catalog_surfaces_invalid: "+err.Error())
		rt.noteChildStartFailureLocked()
		return err
	}
	rt.plan.MainWidthAlignment = muxPlan.MainWidthAlignment
	rt.plan.MainHeightAlignment = muxPlan.MainHeightAlignment
	rt.plan.ArchiveWidthAlignment = muxPlan.ArchiveWidthAlignment
	rt.plan.ArchiveHeightAlignment = muxPlan.ArchiveHeightAlignment
	rt.activeMainWidth = muxPlan.MainWidth
	rt.activeMainHeight = muxPlan.MainHeight
	rt.activeArchiveWidth = muxPlan.ArchiveWidth
	rt.activeArchiveHeight = muxPlan.ArchiveHeight
	muxPlan.H264EncodeHWAvailable = effectiveInventory.Supports(capabilityEncodeProviderForFormat(capabilityPlan, "h264"), capability.StageEncode, "h264", true)
	muxPlan.H264DecodeHWAvailable = effectiveInventory.Supports(capabilityPlan.DecodeProvider, capability.StageDecode, "h264", true)
	muxPlan.H265EncodeHWAvailable = effectiveInventory.Supports(capabilityEncodeProviderForFormat(capabilityPlan, "h265"), capability.StageEncode, "h265", true)
	muxPlan.H265DecodeHWAvailable = effectiveInventory.Supports(capabilityPlan.DecodeProvider, capability.StageDecode, "h265", true)
	muxPlan.MJPEGDecodeHWAvailable = effectiveInventory.Supports(capabilityPlan.DecodeProvider, capability.StageDecode, "mjpeg", true)
	rt.capabilityPlan = capabilityPlan
	log.Printf(
		"[video-broker] capability plan live_encode=%s archive_encode=%s decode=%s tone=%s resize=%s stages=%d",
		capabilityPlan.EncodeProvider,
		capabilityPlan.ArchiveEncodeProvider,
		capabilityPlan.DecodeProvider,
		capabilityPlan.ToneMapBackend,
		capabilityPlan.ResizeBackend,
		len(capabilityPlan.Stages),
	)
	if cameraTransportHasDiscoverableModes(muxPlan.Transport) {
		captureDevice = admission.DevicePath
	}
	rt.activeCaptureWidth = admission.CaptureWidth
	rt.activeCaptureHeight = admission.CaptureHeight
	rt.activeCaptureFPS = admission.CaptureFPS
	rt.writeCameraAdmissionStateLocked("probing", "runtime_attestation_pending")
	log.Printf(
		"[video-broker] camera selected transport=%s source=%s preferred=%s codec=%s mode=%dx%d@%d codec_policy=%s aspect_policy=%s fingerprint=%s reason=%s probed=%s",
		admission.SourceTransport,
		admission.DevicePath,
		admission.PreferredDevicePath,
		admission.Format,
		admission.CaptureWidth,
		admission.CaptureHeight,
		admission.CaptureFPS,
		admission.CodecPolicy,
		admission.AspectPolicy,
		admission.CapabilityFingerprint,
		admission.Reason,
		strings.Join(admission.ProbedDevices, ","),
	)
	if err := validateBrokerCaptureDeviceReady(rt.plan.Transport, captureDevice); err != nil {
		rt.writeCameraAdmissionStateLocked("rejected", err.Error())
		rt.noteChildStartFailureLocked()
		return err
	}
	childCtx, cancel := context.WithCancel(ctx)
	rt.childComponentRetryScopes = make(map[string]string)
	capabilityReport, capabilityErr := videoproviders.DescribeBrokerMuxPlan(muxPlan)
	if capabilityErr == nil {
		rt.childControlAvailable = capabilityReport.AcceptsRuntimeControlProtocol
		rt.childControlSupported = capabilityReport.SupportsRuntimeLiveControl
		rt.childLiveControlMode = capabilityReport.LiveControlMode
		if rt.childControlSupported && len(rt.mainClients) > 0 {
			rt.mainAwaitingEncoderEpoch = true
			rt.clearMainBootstrapLocked()
		}
		muxPlan.MainActive = rt.plan.LiveEnabled && (len(rt.mainClients) > 0 || !rt.childControlSupported)
		rt.toneMappingSupported = capabilityReport.SupportsToneMapping
		rt.toneMappingBackend = capabilityReport.ToneMappingBackend
		for _, retry := range capabilityReport.ComponentRetries {
			componentID := strings.TrimSpace(retry.ComponentID)
			if componentID != "" {
				rt.childComponentRetryScopes[componentID] = capabilityReport.ComponentRetryScope(componentID)
			}
		}
		log.Printf(
			"[video-broker] capability adapter=%s input_format=%s cost=%s hw_encode=%t sw_decode=%t sw_resize=%t control_protocol=%t live_runtime_control=%t live_control_mode=%s tone_mapping=%t tone_backend=%s notes=%s",
			capabilityReport.AdapterID,
			capabilityReport.InputFormat,
			capabilityReport.EstimatedCost,
			capabilityReport.UsesHardwareEncode,
			capabilityReport.RequiresSoftwareDecode,
			capabilityReport.RequiresSoftwareResize,
			capabilityReport.AcceptsRuntimeControlProtocol,
			capabilityReport.SupportsRuntimeLiveControl,
			capabilityReport.LiveControlMode,
			capabilityReport.SupportsToneMapping,
			capabilityReport.ToneMappingBackend,
			strings.Join(capabilityReport.Notes, "; "),
		)
	}
	rt.reconcileHardwareComponentRetryScopesLocked()
	frames, err := prepareCaptureFrames(childCtx, rt.plan.FrameCapture, &muxPlan)
	if err != nil {
		cancel()
		rt.noteChildStartFailureLocked()
		return fmt.Errorf("frame source admission: %w", err)
	}
	transferred := false
	defer func() {
		if frames != nil && !transferred {
			cleanupCtx, stop := context.WithTimeout(context.Background(), time.Second)
			defer stop()
			if err := frames.Retire(cleanupCtx); err != nil {
				rt.frameCleanupErr = err
				if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
					rt.pendingFrameCleanup = frames
				}
				persistFrameCleanupGuard(rt.plan.StateDirectory, captureIncidentID(childEpoch), err)
				result = errors.Join(result, fmt.Errorf("frame source startup cleanup: %w", err))
			}
		}
	}()
	childEpoch = muxPlan.MediaEpoch
	cmd, adapterID, err := videoproviders.BuildBrokerMuxCommand(childCtx, muxPlan)
	if err != nil {
		cancel()
		rt.writeCameraAdmissionStateLocked("rejected", "bridge_command_invalid: "+err.Error())
		rt.noteChildStartFailureLocked()
		return err
	}
	configureBrokerChildLifecycle(cmd)
	if frames != nil {
		if err := frames.Attach(cmd); err != nil {
			cancel()
			return fmt.Errorf("frame source attach: %w", err)
		}
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		cancel()
		rt.noteChildStartFailureLocked()
		return err
	}
	if actual, pipeErr := tuneBrokerPipeCapacity(stdout, brokerMuxPipeTargetBytes); pipeErr != nil {
		log.Printf(
			"[video-broker] mux pipe capacity unchanged actual_bytes=%d target_bytes=%d err=%v",
			actual, brokerMuxPipeTargetBytes, pipeErr)
	} else if actual > 0 {
		log.Printf(
			"[video-broker] mux pipe capacity actual_bytes=%d target_bytes=%d",
			actual, brokerMuxPipeTargetBytes)
	}
	stderr, stderrWriter, err := brokerChildStderrPipe(cmd)
	if err != nil {
		cancel()
		rt.noteChildStartFailureLocked()
		return err
	}
	defer stderrWriter.Close() // Start failures must release both broker-owned ends.
	defer func() {
		if !transferred {
			_ = stderr.Close()
		}
	}()
	var stdin io.WriteCloser
	if rt.childControlAvailable {
		stdin, err = cmd.StdinPipe()
		if err != nil {
			cancel()
			rt.noteChildStartFailureLocked()
			return err
		}
	}
	if err := cmd.Start(); err != nil {
		cancel()
		rt.writeCameraAdmissionStateLocked("rejected", "bridge_start_failed: "+err.Error())
		rt.noteChildStartFailureLocked()
		return err
	}
	_ = stderrWriter.Close() // Only the child retains the write end; exit yields EOF.
	childPID := cmd.Process.Pid
	childPGID, childPGIDErr := brokerGetProcessGroup(childPID)
	if childPGIDErr != nil || childPGID != childPID {
		cancel()
		abortBrokerChildProcessGroup(cmd)
		rt.writeCameraAdmissionStateLocked("rejected", "bridge_process_group_invalid")
		rt.noteChildStartFailureLocked()
		return fmt.Errorf("capture child process-group isolation failed pid=%d pgid=%d err=%v", childPID, childPGID, childPGIDErr)
	}
	if frames != nil {
		bootstrapStarted := time.Now()
		if err := frames.Started(childCtx, cmd.Process); err != nil {
			bootstrapElapsed := time.Since(bootstrapStarted)
			observation := brokerBootstrapObservation(cmd)
			retirementStarted := time.Now()
			retirementResult := awaitBrokerBootstrapStopReady(childCtx, cmd, observation, brokerPreHandlerRetirementWindow)
			if retirementResult != "not_applicable" {
				log.Printf("[video-broker] event=capture_bootstrap_retirement_window source=%s source_epoch=%d child_pid=%d result=%s waited_ms=%d startup_failed=true action=cooperative_stop_then_verify_release", muxPlan.SourceID, childEpoch, childPID, retirementResult, time.Since(retirementStarted).Milliseconds())
			}
			waitErr, diagnostic := stopFailedBrokerBootstrap(childCtx, cancel, cmd, stderr, brokerChildTerminationGrace)
			line, sanitation := normalizeBrokerChildStderrLine(diagnostic)
			log.Printf("[video-broker] event=capture_bootstrap_failed source=%s source_epoch=%d child_pid=%d bootstrap_ms=%d %s err=%v wait_err=%v process_state=%v native_stderr=%q%s action=verify_release_before_retry", muxPlan.SourceID, childEpoch, childPID, bootstrapElapsed.Milliseconds(), observation, err, waitErr, cmd.ProcessState, line, sanitation.logSuffix())
			rt.noteChildStartFailureLocked()
			return fmt.Errorf("frame source bootstrap: %w", err)
		}
	}
	rt.childCmd = cmd
	rt.childFrames = frames
	transferred = true
	rt.childOut = stdout
	rt.childStderr = stderr
	stderrDone := make(chan struct{})
	rt.childStderrDone = stderrDone
	exitDone := make(chan struct{})
	rt.childExitDone = exitDone
	rt.childIn = stdin
	if rt.childControlAvailable {
		rt.childControl = make(chan bridgeRuntimeControl, 1)
		rt.childFanoutControl = make(chan workerFanoutWrite)
	} else {
		rt.childControl = nil
		rt.childFanoutControl = nil
	}
	rt.childCancel = cancel
	rt.childRunning = true
	rt.childStartedAt = time.Now()
	rt.childStartMainFrames = rt.totalMainFrames
	rt.childStartArchiveFrames = rt.totalArchiveFrames
	rt.childStartSubFrames = rt.totalSubFrames
	rt.mediaEpoch = childEpoch
	rt.causalSubFrame, rt.causalSubSourceEpoch = 0, 0
	rt.resetBridgeControlReadyLocked()
	rt.resetChildFailure(childEpoch)
	if rt.subShared != nil {
		rt.subShared.RotateEpoch()
	}
	if len(rt.mainClients) > 0 {
		rt.mainDemandedAt = rt.childStartedAt
	}
	rt.lastFrameAt = time.Time{}
	rt.lastMainFrameAt = time.Time{}
	rt.lastSubFrameAt = time.Time{}
	rt.lastArchiveFrameAt = time.Time{}
	rt.resetCadenceHealthLocked()
	log.Printf(
		"[video-broker] event=capture_epoch_started incident_id=%s media_epoch=%d child_pid=%d child_pgid=%d lifecycle_owner=video_broker termination=term_cont_group_then_kill_group adapter=%s transport=%s device=%s codec=%s capture=%dx%d@%d archive=%dx%d fingerprint=%s executable=%s",
		captureIncidentID(childEpoch),
		childEpoch,
		childPID,
		childPGID,
		adapterID,
		rt.plan.Transport,
		admission.DevicePath,
		admission.Format,
		admission.CaptureWidth,
		admission.CaptureHeight,
		admission.CaptureFPS,
		muxPlan.ArchiveWidth,
		muxPlan.ArchiveHeight,
		admission.CapabilityFingerprint,
		cmd.Path,
	)

	if !rt.startGoroutine(func() {
		defer close(stderrDone)
		rt.scanStderr(childCtx, stderr, childEpoch, childPID)
	}) {
		close(stderrDone)
	}
	if frames != nil {
		frameDone := make(chan struct{})
		rt.childFramesDone = frameDone
		observerCtx, stopObserver := context.WithCancel(childCtx)
		observer := rt.newCausalDeliveryObserverLocked(observerCtx)
		if !rt.startGoroutine(func() {
			defer close(frameDone)
			observerDone := make(chan struct{})
			go func() {
				defer close(observerDone)
				observer.run(rt)
			}()
			defer func() { stopObserver(); <-observerDone }()
			err := frames.Run(childCtx, observer.observe)
			err, retire := frameReceiveExit(err, childCtx.Err())
			if err != nil {
				action := "join_capture"
				if retire {
					action = "retire_capture"
				}
				// cause names why the receive ended, so an epoch retire can be
				// attributed from this line alone: a peer that failed and a frame
				// that missed its deadline while waiting both retire the receiver
				// but call for different responses.
				log.Printf("[video-broker] frame_source stage=receive_failed source=%s media_epoch=%d child_pid=%d child_context_done=%t cause=%s err=%v action=%s", muxPlan.SourceID, childEpoch, childPID, !retire, in.ClassifyReceiveFailure(err), err, action)
			}
			if retire {
				cancel()
			}
		}) {
			stopObserver()
			close(frameDone)
			cancel()
		}
	}
	rt.startGoroutine(func() {
		enforceBrokerChildTermination(childCtx, cmd, exitDone, brokerChildTerminationGrace)
	})
	if rt.childControlAvailable {
		rt.startGoroutine(func() {
			rt.writeBridgeControls(childCtx, stdin, rt.childControl, rt.childFanoutControl)
		})
		if rt.childControlSupported {
			rt.queueBridgeControlLocked("child_started")
		}
	}
	rt.startGoroutine(func() { rt.forwardMuxStream(ctx, childCtx, cancel, stdout, childEpoch, childPID) })
	rt.startGoroutine(func() {
		rt.watchChildFrames(childCtx, cmd, time.Duration(frameWatchdogSec)*time.Second, time.Duration(warmupSec)*time.Second)
	})
	for _, recovery := range rt.componentRecovery {
		if !rt.hardwareComponentRecoveryActiveLocked(recovery) {
			continue
		}
		if !recovery.RetryAfter.IsZero() {
			rt.scheduleHardwareComponentRetryLocked(childCtx, recovery)
		}
	}
	return nil
}

func capabilityEncodeProviderForFormat(plan capability.Plan, format string) string {
	format = capability.NormalizeFormat(format)
	for _, selection := range plan.Stages {
		if selection.Stage == capability.StageEncode && capability.NormalizeFormat(selection.Format) == format {
			return selection.Backend
		}
	}
	return plan.EncodeProvider
}

func capabilityEncodeAlignmentForRole(plan capability.Plan, role, format string) (int, int) {
	format = capability.NormalizeFormat(format)
	for _, selection := range plan.Stages {
		if selection.Stage == capability.StageEncode && selection.Role == role && capability.NormalizeFormat(selection.Format) == format {
			return selection.WidthAlignment, selection.HeightAlignment
		}
	}
	return 0, 0
}

func brokerComponentRetryDelay(consecutiveFailures int) time.Duration {
	if consecutiveFailures < 1 {
		consecutiveFailures = 1
	}
	delay := brokerComponentRetryInitial
	for attempt := 1; attempt < consecutiveFailures && delay < brokerComponentRetryMaximum; attempt++ {
		delay *= 2
		if delay > brokerComponentRetryMaximum {
			delay = brokerComponentRetryMaximum
		}
	}
	return delay
}

func (rt *videoBrokerRuntime) noteHardwareComponentFailureLocked(
	componentID, stage, format string,
	mediaEpoch, failureToken uint64,
	now time.Time,
) (hardwareComponentRecovery, bool) {
	componentID = strings.TrimSpace(componentID)
	if componentID == "" {
		return hardwareComponentRecovery{}, false
	}
	if now.IsZero() {
		now = time.Now()
	}
	if rt.componentRecovery == nil {
		rt.componentRecovery = make(map[string]hardwareComponentRecovery)
	}
	state := rt.componentRecovery[componentID]
	if failureToken != 0 && state.LastFailureToken == failureToken {
		return state, false
	}
	if failureToken == 0 && state.LastFailureToken == 0 && state.LastFailureEpoch == mediaEpoch && mediaEpoch != 0 {
		return state, false
	}
	state.ComponentID = componentID
	state.Stage = strings.TrimSpace(stage)
	state.Format = capability.NormalizeFormat(format)
	state.ConsecutiveFailures++
	state.LastFailureEpoch = mediaEpoch
	state.LastFailureToken = failureToken
	state.RetryAfter = now.Add(brokerComponentRetryDelay(state.ConsecutiveFailures))
	state.RetryScope = rt.hardwareComponentRetryScopeLocked(componentID, state.RetryScope)
	state.AwaitNaturalEpoch = false
	if failureToken != 0 && state.TrialToken == failureToken && state.TrialMediaEpoch == mediaEpoch &&
		state.TrialChildPID > 0 {
		state.CancelToken = failureToken
		state.CancelMediaEpoch = state.TrialMediaEpoch
		state.CancelChildPID = state.TrialChildPID
	}
	state.TrialToken = 0
	state.TrialMediaEpoch = 0
	state.TrialChildPID = 0
	state.TrialStartedAt = time.Time{}
	state.TrialCandidateReady = time.Time{}
	state.TrialCommitRequested = false
	state.TrialDeadline = time.Time{}
	state.StableAfter = time.Time{}
	state.HalfOpenEpoch = 0
	state.HalfOpenChildPID = 0
	state.HalfOpenFrameBase = 0
	rt.componentRecovery[componentID] = state
	return state, true
}

func (rt *videoBrokerRuntime) noteHardwareComponentCandidateReadyLocked(
	componentID string,
	mediaEpoch uint64,
	childPID int,
	retryToken uint64,
	completedAt time.Time,
	now time.Time,
) (hardwareComponentRecovery, bool) {
	state, ok := rt.componentRecovery[strings.TrimSpace(componentID)]
	if !ok || retryToken == 0 || childPID <= 0 || completedAt.IsZero() {
		return state, false
	}
	if now.IsZero() {
		now = time.Now()
	}
	if rt.mediaEpoch != mediaEpoch || !rt.childRunning || brokerChildPID(rt.childCmd) != childPID ||
		state.TrialToken != retryToken || state.TrialMediaEpoch != mediaEpoch ||
		state.TrialChildPID != childPID || state.TrialStartedAt.IsZero() ||
		state.TrialDeadline.IsZero() || !state.StableAfter.IsZero() ||
		state.TrialCommitRequested || !state.TrialCandidateReady.IsZero() ||
		completedAt.Before(state.TrialStartedAt.Truncate(time.Millisecond)) || !completedAt.Before(state.TrialDeadline) ||
		completedAt.After(now) || !now.Before(state.TrialDeadline) {
		return state, false
	}
	state.TrialCandidateReady = completedAt
	state.TrialCommitRequested = true
	// Commit is a separate causal phase. Re-anchor its deadline so the original
	// trial timer cannot race a valid ready receipt, while still bounding an
	// unacknowledged native switch.
	state.TrialDeadline = now.Add(brokerComponentTrialTimeout)
	rt.componentRecovery[state.ComponentID] = state
	return state, true
}

func (rt *videoBrokerRuntime) acknowledgeHardwareComponentCandidateCanceledLocked(
	componentID string,
	mediaEpoch uint64,
	childPID int,
	retryToken uint64,
	now time.Time,
) (hardwareComponentRecovery, bool, bool) {
	state, ok := rt.componentRecovery[strings.TrimSpace(componentID)]
	if !ok || retryToken == 0 || childPID <= 0 {
		return state, false, false
	}
	if now.IsZero() {
		now = time.Now()
	}
	if state.CancelToken == retryToken && state.CancelMediaEpoch == mediaEpoch &&
		state.CancelChildPID == childPID {
		scheduleRetry := false
		// If the original cooldown elapsed while the exact native cancellation
		// was still pending, its timer deliberately deferred instead of
		// overlapping candidates. A late acknowledgement must replace that
		// temporary deferral with a new bounded in-place deadline; otherwise the
		// same healthy capture child can remain on CPU forever.
		if state.AwaitNaturalEpoch && state.RetryAfter.IsZero() &&
			rt.mediaEpoch == mediaEpoch && rt.childRunning && !rt.childFinishing &&
			brokerChildPID(rt.childCmd) == childPID &&
			rt.hardwareComponentRetryScopeLocked(state.ComponentID, state.RetryScope) == videoproviders.ComponentRetryInPlace &&
			rt.childControlAvailable && rt.childControl != nil &&
			brokerBridgeRetryComponentPattern.MatchString(state.ComponentID) {
			state.AwaitNaturalEpoch = false
			state.RetryScope = videoproviders.ComponentRetryInPlace
			state.RetryAfter = now.Add(brokerComponentRetryDelay(state.ConsecutiveFailures))
			scheduleRetry = true
		}
		state.CancelToken = 0
		state.CancelMediaEpoch = 0
		state.CancelChildPID = 0
		rt.componentRecovery[state.ComponentID] = state
		return state, true, scheduleRetry
	}
	if state.TrialToken != retryToken || state.TrialMediaEpoch != mediaEpoch ||
		state.TrialChildPID != childPID {
		return state, false, false
	}
	// A native rollback can arrive after the committed-frame receipt entered
	// half-open but before the broker's `none` finalization reached the worker.
	// It is authoritative for the exact token/child/epoch and must atomically
	// restore CPU attestation before the old stability timer can clear recovery.
	failureToken := retryToken
	if !state.StableAfter.IsZero() {
		failureToken = 0
	}
	failed, changed := rt.noteHardwareComponentFailureLocked(
		state.ComponentID, state.Stage, state.Format, mediaEpoch, failureToken, now)
	if !changed {
		return state, false, false
	}
	failed.CancelToken = 0
	failed.CancelMediaEpoch = 0
	failed.CancelChildPID = 0
	rt.componentRecovery[failed.ComponentID] = failed
	rollback := brokerCaptureDecoderTelemetry{
		Codec: state.Format, Requested: "auto", Selected: "cpu",
		Fallback: "jpu_retry_deadline_failed", Output: "nv12",
	}
	rt.observedDecodeCodec = rollback.Codec
	rt.observedDecodeRequested = rollback.Requested
	rt.observedDecodeBackend = rollback.Selected
	rt.observedDecodeFallback = rollback.Fallback
	rt.attestCaptureDecoderLocked(rollback)
	return failed, true, true
}

func (rt *videoBrokerRuntime) noteHardwareComponentHalfOpenLocked(
	componentID string,
	mediaEpoch uint64,
	childPID int,
	retryToken uint64,
	now time.Time,
) (hardwareComponentRecovery, bool) {
	state, ok := rt.componentRecovery[strings.TrimSpace(componentID)]
	if !ok {
		return hardwareComponentRecovery{}, false
	}
	if now.IsZero() {
		now = time.Now()
	}
	if rt.mediaEpoch != mediaEpoch || !rt.childRunning || childPID <= 0 || brokerChildPID(rt.childCmd) != childPID {
		return state, false
	}
	if retryToken != 0 {
		if state.TrialToken != retryToken || state.TrialMediaEpoch != mediaEpoch ||
			state.TrialChildPID != childPID || state.TrialDeadline.IsZero() ||
			!state.TrialCommitRequested || state.TrialCandidateReady.IsZero() ||
			!now.Before(state.TrialDeadline) {
			return state, false
		}
	} else {
		naturalEpochTransition := mediaEpoch != 0 && mediaEpoch != state.LastFailureEpoch
		// Token zero has no trial identity. It is admissible only from a genuinely
		// new capture epoch; AwaitNaturalEpoch describes the plan, but must never
		// turn delayed telemetry from the failed child into an authorization.
		if !naturalEpochTransition {
			return state, false
		}
	}
	if state.HalfOpenEpoch == mediaEpoch && state.HalfOpenChildPID == childPID &&
		state.TrialToken == retryToken && !state.StableAfter.IsZero() {
		return state, false
	}
	state.RetryAfter = time.Time{}
	state.AwaitNaturalEpoch = false
	if retryToken == 0 {
		// Scope describes the causal operation that opened this trial, not what
		// the newly started adapter could do on a later retry. Token zero is proof
		// from an independently-created source epoch.
		state.RetryScope = videoproviders.ComponentRetryNextNaturalEpoch
	} else {
		state.RetryScope = videoproviders.ComponentRetryInPlace
	}
	state.TrialToken = retryToken
	state.TrialMediaEpoch = mediaEpoch
	state.TrialChildPID = childPID
	state.TrialStartedAt = time.Time{}
	state.TrialCandidateReady = time.Time{}
	state.TrialCommitRequested = false
	state.TrialDeadline = time.Time{}
	state.CancelToken = 0
	state.CancelMediaEpoch = 0
	state.CancelChildPID = 0
	state.HalfOpenEpoch = mediaEpoch
	state.HalfOpenChildPID = childPID
	state.HalfOpenFrameBase = rt.componentHealthFrameCounterLocked()
	state.StableAfter = now.Add(brokerComponentStableDuration)
	if retryToken != 0 {
		// The retry token has now been consumed by an acknowledged hardware
		// frame. Subsequent runtime fallback is a new epoch-bound failure, not a
		// duplicate of the failure that opened this trial.
		state.LastFailureToken = retryToken
	}
	rt.componentRecovery[state.ComponentID] = state
	return state, true
}

func (rt *videoBrokerRuntime) confirmHardwareComponentStableLocked(
	componentID string,
	mediaEpoch uint64,
	childPID int,
	expectedStableAfter time.Time,
	now time.Time,
) bool {
	state, ok := rt.componentRecovery[strings.TrimSpace(componentID)]
	if !ok || state.HalfOpenEpoch != mediaEpoch || state.HalfOpenChildPID != childPID ||
		!state.StableAfter.Equal(expectedStableAfter) {
		return false
	}
	if now.IsZero() {
		now = time.Now()
	}
	if now.Before(state.StableAfter) {
		return false
	}
	if rt.mediaEpoch != mediaEpoch || !rt.childRunning || brokerChildPID(rt.childCmd) != childPID ||
		!rt.capabilityPlanUsesHardwareComponentLocked(state.ComponentID) ||
		rt.componentHealthFrameCounterLocked() <= state.HalfOpenFrameBase {
		rt.rearmHardwareComponentOnNaturalEpochLocked(state.ComponentID)
		return false
	}
	delete(rt.componentRecovery, state.ComponentID)
	return true
}

func (rt *videoBrokerRuntime) componentHealthFrameCounterLocked() uint64 {
	result := rt.totalMainFrames
	if rt.totalArchiveFrames > result {
		result = rt.totalArchiveFrames
	}
	if rt.totalSubFrames > result {
		result = rt.totalSubFrames
	}
	return result
}

func (rt *videoBrokerRuntime) hardwareComponentRetryScopeLocked(componentID, _ string) string {
	if rt.childComponentRetryScopes[componentID] == videoproviders.ComponentRetryInPlace {
		return videoproviders.ComponentRetryInPlace
	}
	return videoproviders.ComponentRetryNextNaturalEpoch
}

func hardwareComponentRecoveryMatchesOperation(recovery hardwareComponentRecovery, stage, format string) bool {
	return strings.EqualFold(strings.TrimSpace(recovery.Stage), strings.TrimSpace(stage)) &&
		capability.NormalizeFormat(recovery.Format) == capability.NormalizeFormat(format)
}

// hardwareComponentRecoveryActiveLocked binds runtime recovery to an operation
// in the current graph. The selected component is intentionally not compared:
// while hardware is cooling down, the exact same Stage+Format is represented by
// its software continuity component.
func (rt *videoBrokerRuntime) hardwareComponentRecoveryActiveLocked(recovery hardwareComponentRecovery) bool {
	if strings.TrimSpace(recovery.ComponentID) == "" || strings.TrimSpace(recovery.Stage) == "" {
		return false
	}
	for _, selection := range rt.capabilityPlan.Stages {
		if hardwareComponentRecoveryMatchesOperation(recovery, selection.Stage, selection.Format) {
			return true
		}
	}
	return false
}

func (rt *videoBrokerRuntime) pruneInactiveHardwareComponentRecoveriesLocked() {
	for componentID, recovery := range rt.componentRecovery {
		if !rt.hardwareComponentRecoveryActiveLocked(recovery) {
			delete(rt.componentRecovery, componentID)
		}
	}
}

func (rt *videoBrokerRuntime) reconcileHardwareComponentRetryScopesLocked() {
	rt.pruneInactiveHardwareComponentRecoveriesLocked()
	for componentID, recovery := range rt.componentRecovery {
		if recovery.AwaitNaturalEpoch {
			recovery.RetryScope = videoproviders.ComponentRetryNextNaturalEpoch
		} else {
			recovery.RetryScope = rt.hardwareComponentRetryScopeLocked(componentID, recovery.RetryScope)
		}
		rt.componentRecovery[componentID] = recovery
	}
}

func (rt *videoBrokerRuntime) rearmHardwareComponentOnNaturalEpochLocked(componentID string) {
	state, ok := rt.componentRecovery[strings.TrimSpace(componentID)]
	if !ok {
		return
	}
	state.RetryAfter = time.Time{}
	state.RetryScope = videoproviders.ComponentRetryNextNaturalEpoch
	state.AwaitNaturalEpoch = true
	state.TrialToken = 0
	state.TrialMediaEpoch = 0
	state.TrialChildPID = 0
	state.TrialStartedAt = time.Time{}
	state.TrialCandidateReady = time.Time{}
	state.TrialCommitRequested = false
	state.TrialDeadline = time.Time{}
	state.CancelToken = 0
	state.CancelMediaEpoch = 0
	state.CancelChildPID = 0
	state.StableAfter = time.Time{}
	state.HalfOpenEpoch = 0
	state.HalfOpenChildPID = 0
	state.HalfOpenFrameBase = 0
	rt.componentRecovery[state.ComponentID] = state
}

func (rt *videoBrokerRuntime) abortHardwareComponentTrialsForChildLocked(mediaEpoch uint64, childPID int) {
	for componentID, state := range rt.componentRecovery {
		if (state.TrialMediaEpoch == mediaEpoch && state.TrialChildPID == childPID) ||
			(state.HalfOpenEpoch == mediaEpoch && state.HalfOpenChildPID == childPID) {
			rt.rearmHardwareComponentOnNaturalEpochLocked(componentID)
		}
	}
}

func (rt *videoBrokerRuntime) capabilityPlanUsesHardwareComponentLocked(componentID string) bool {
	for _, selection := range rt.capabilityPlan.Stages {
		if selection.Component == componentID && selection.Hardware {
			return true
		}
	}
	return false
}

func (rt *videoBrokerRuntime) scheduleHardwareComponentRetryLocked(
	ctx context.Context,
	recovery hardwareComponentRecovery,
) {
	if recovery.ComponentID == "" || recovery.RetryAfter.IsZero() {
		return
	}
	componentID := recovery.ComponentID
	retryAfter := recovery.RetryAfter
	if !rt.startGoroutine(func() {
		delay := time.Until(retryAfter)
		if delay < 0 {
			delay = 0
		}
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		rt.mu.Lock()
		requested, changed := rt.requestHardwareComponentRetryLocked(componentID, retryAfter, time.Now())
		if changed && requested.TrialToken != 0 {
			rt.scheduleHardwareComponentTrialTimeoutLocked(ctx, requested)
		}
		rt.mu.Unlock()
		if changed {
			action := "next_natural_epoch"
			if requested.TrialToken != 0 {
				action = "retry_in_place"
			}
			log.Printf(
				"[video-broker] event=hardware_component_retry_requested component=%s retry_after=%s retry_scope=%s retry_token=%d media_epoch=%d child_pid=%d action=%s",
				componentID,
				retryAfter.UTC().Format(time.RFC3339Nano),
				requested.RetryScope,
				requested.TrialToken,
				requested.TrialMediaEpoch,
				requested.TrialChildPID,
				action,
			)
		}
	}) {
		return
	}
}

func (rt *videoBrokerRuntime) requestHardwareComponentRetryLocked(
	componentID string,
	expectedRetryAfter time.Time,
	now time.Time,
) (hardwareComponentRecovery, bool) {
	recovery, ok := rt.componentRecovery[componentID]
	if !ok || !rt.hardwareComponentRecoveryActiveLocked(recovery) ||
		recovery.RetryAfter.IsZero() || !recovery.RetryAfter.Equal(expectedRetryAfter) {
		return recovery, false
	}
	if now.IsZero() {
		now = time.Now()
	}
	if now.Before(recovery.RetryAfter) || rt.capabilityPlanUsesHardwareComponentLocked(componentID) {
		return recovery, false
	}
	if recovery.CancelToken != 0 {
		// Never overlap a new candidate with a cancellation that the current
		// child has not acknowledged. Natural-epoch retry preserves liveness
		// without forcing a capture reopen.
		recovery.RetryAfter = time.Time{}
		recovery.RetryScope = videoproviders.ComponentRetryNextNaturalEpoch
		recovery.AwaitNaturalEpoch = true
		rt.componentRecovery[componentID] = recovery
		rt.queueBridgeControlForceLocked("hardware_component_cancel_pending")
		return recovery, true
	}
	recovery.RetryScope = rt.hardwareComponentRetryScopeLocked(componentID, recovery.RetryScope)
	if rt.childCmd == nil || rt.childCmd.Process == nil || !rt.childRunning || rt.childFinishing {
		recovery.RetryAfter = time.Time{}
		recovery.RetryScope = videoproviders.ComponentRetryNextNaturalEpoch
		recovery.AwaitNaturalEpoch = true
		rt.componentRecovery[componentID] = recovery
		return recovery, true
	}
	if recovery.RetryScope != videoproviders.ComponentRetryInPlace || !rt.childControlAvailable || rt.childControl == nil ||
		!brokerBridgeRetryComponentPattern.MatchString(componentID) {
		recovery.RetryAfter = time.Time{}
		recovery.RetryScope = videoproviders.ComponentRetryNextNaturalEpoch
		recovery.AwaitNaturalEpoch = true
		rt.componentRecovery[componentID] = recovery
		return recovery, true
	}
	rt.componentRetrySequence++
	if rt.componentRetrySequence == 0 {
		rt.componentRetrySequence++
	}
	recovery.RetryAfter = time.Time{}
	recovery.RetryScope = videoproviders.ComponentRetryInPlace
	recovery.AwaitNaturalEpoch = false
	recovery.TrialToken = rt.componentRetrySequence
	recovery.TrialMediaEpoch = rt.mediaEpoch
	recovery.TrialChildPID = rt.childCmd.Process.Pid
	recovery.TrialStartedAt = now
	recovery.TrialCandidateReady = time.Time{}
	recovery.TrialCommitRequested = false
	recovery.TrialDeadline = now.Add(brokerComponentTrialTimeout)
	recovery.CancelToken = 0
	recovery.CancelMediaEpoch = 0
	recovery.CancelChildPID = 0
	recovery.StableAfter = time.Time{}
	recovery.HalfOpenEpoch = 0
	recovery.HalfOpenChildPID = 0
	recovery.HalfOpenFrameBase = 0
	rt.componentRecovery[componentID] = recovery
	rt.queueBridgeControlForceLocked("hardware_component_retry")
	return recovery, true
}

func (rt *videoBrokerRuntime) scheduleHardwareComponentTrialTimeoutLocked(
	ctx context.Context,
	recovery hardwareComponentRecovery,
) {
	if recovery.ComponentID == "" || recovery.TrialToken == 0 || recovery.TrialDeadline.IsZero() {
		return
	}
	componentID := recovery.ComponentID
	retryToken := recovery.TrialToken
	deadline := recovery.TrialDeadline
	rt.startGoroutine(func() {
		delay := time.Until(deadline)
		if delay < 0 {
			delay = 0
		}
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		rt.mu.Lock()
		failed, changed := rt.expireHardwareComponentTrialLocked(
			componentID, retryToken, deadline, time.Now())
		if changed {
			rt.queueBridgeControlForceLocked("hardware_component_retry_timed_out")
			rt.scheduleHardwareComponentRetryLocked(ctx, failed)
		}
		rt.mu.Unlock()
		if changed {
			log.Printf(
				"[video-broker] event=hardware_component_retry_timed_out component=%s retry_token=%d action=keep_cpu_continuity",
				componentID, retryToken)
		}
	})
}

func (rt *videoBrokerRuntime) expireHardwareComponentTrialLocked(
	componentID string,
	retryToken uint64,
	expectedDeadline time.Time,
	now time.Time,
) (hardwareComponentRecovery, bool) {
	state, ok := rt.componentRecovery[strings.TrimSpace(componentID)]
	if !ok || state.TrialToken != retryToken || !state.StableAfter.IsZero() ||
		state.TrialDeadline.IsZero() || !state.TrialDeadline.Equal(expectedDeadline) ||
		now.Before(state.TrialDeadline) {
		return state, false
	}
	return rt.noteHardwareComponentFailureLocked(
		state.ComponentID, state.Stage, state.Format, state.TrialMediaEpoch, retryToken, now)
}

func (rt *videoBrokerRuntime) scheduleHardwareComponentStableConfirmationLocked(
	ctx context.Context,
	recovery hardwareComponentRecovery,
) {
	if recovery.ComponentID == "" || recovery.StableAfter.IsZero() || recovery.HalfOpenEpoch == 0 {
		return
	}
	componentID := recovery.ComponentID
	mediaEpoch := recovery.HalfOpenEpoch
	childPID := recovery.HalfOpenChildPID
	stableAfter := recovery.StableAfter
	if !rt.startGoroutine(func() {
		delay := time.Until(stableAfter)
		if delay < 0 {
			delay = 0
		}
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		rt.mu.Lock()
		recovered := rt.confirmHardwareComponentStableLocked(componentID, mediaEpoch, childPID, stableAfter, time.Now())
		rt.mu.Unlock()
		if recovered {
			log.Printf(
				"[video-broker] event=hardware_component_recovered component=%s media_epoch=%d stable_after=%s action=clear_failure_history",
				componentID,
				mediaEpoch,
				stableAfter.UTC().Format(time.RFC3339Nano),
			)
		}
	}) {
		return
	}
}

func (rt *videoBrokerRuntime) effectiveCapabilityInventoryLocked(now time.Time) capability.Inventory {
	if now.IsZero() {
		now = time.Now()
	}
	inventory := rt.plan.CapabilityInventory
	inventory.Components = append([]capability.Component(nil), inventory.Components...)
	for componentID, recovery := range rt.componentRecovery {
		if !rt.hardwareComponentRecoveryActiveLocked(recovery) ||
			recovery.RetryAfter.IsZero() || !now.Before(recovery.RetryAfter) {
			continue
		}
		for index := range inventory.Components {
			component := &inventory.Components[index]
			if component.ID != componentID {
				continue
			}
			operations := make([]capability.Operation, 0, len(component.Operations))
			removed := false
			for _, operation := range component.Operations {
				if hardwareComponentRecoveryMatchesOperation(recovery, operation.Stage, operation.Format) {
					removed = true
					continue
				}
				operations = append(operations, operation)
			}
			if !removed {
				continue
			}
			component.Operations = operations
			if len(component.Operations) == 0 {
				component.Available = false
				component.State = capability.StateCandidate
				if recovery.ConsecutiveFailures >= brokerComponentFailureThreshold {
					component.State = capability.StateRejected
				}
			}
			component.Reason = fmt.Sprintf(
				"runtime recovery cooldown stage=%s format=%s failures=%d threshold=%d retry_after=%s",
				recovery.Stage,
				recovery.Format,
				recovery.ConsecutiveFailures,
				brokerComponentFailureThreshold,
				recovery.RetryAfter.UTC().Format(time.RFC3339),
			)
		}
	}
	return inventory
}

func hardwareComponentRecoveryState(recovery hardwareComponentRecovery, now time.Time) string {
	if !recovery.StableAfter.IsZero() {
		return "half_open"
	}
	if recovery.TrialToken != 0 {
		return "trial_requested"
	}
	if recovery.AwaitNaturalEpoch {
		return "next_natural_epoch"
	}
	if !recovery.RetryAfter.IsZero() && !now.Before(recovery.RetryAfter) {
		return "retry_due"
	}
	return "cooldown"
}

func (rt *videoBrokerRuntime) hardwareRecoveryStatusesLocked(now time.Time) []HardwareRecoveryStatus {
	if now.IsZero() {
		now = time.Now()
	}
	statuses := make([]HardwareRecoveryStatus, 0, len(rt.componentRecovery))
	for _, recovery := range rt.componentRecovery {
		if !rt.hardwareComponentRecoveryActiveLocked(recovery) {
			continue
		}
		state := hardwareComponentRecoveryState(recovery, now)
		status := HardwareRecoveryStatus{
			ComponentID:         recovery.ComponentID,
			Stage:               recovery.Stage,
			Format:              recovery.Format,
			State:               state,
			ConsecutiveFailures: recovery.ConsecutiveFailures,
			FailureThreshold:    brokerComponentFailureThreshold,
			Rejected:            state == "cooldown" && recovery.ConsecutiveFailures >= brokerComponentFailureThreshold,
			RetryScope:          recovery.RetryScope,
			RetryToken:          recovery.TrialToken,
			TrialMediaEpoch:     recovery.TrialMediaEpoch,
			TrialChildPID:       recovery.TrialChildPID,
		}
		if !recovery.RetryAfter.IsZero() {
			retryAt := recovery.RetryAfter.UTC()
			status.RetryAt = &retryAt
		}
		if !recovery.StableAfter.IsZero() {
			stableAfter := recovery.StableAfter.UTC()
			status.StableAfter = &stableAfter
		}
		if !recovery.TrialDeadline.IsZero() {
			trialDeadline := recovery.TrialDeadline.UTC()
			status.TrialDeadline = &trialDeadline
		}
		statuses = append(statuses, status)
	}
	sort.Slice(statuses, func(left, right int) bool {
		return statuses[left].ComponentID < statuses[right].ComponentID
	})
	return statuses
}

func brokerLiveEncodeRequired(plan videoproviders.BrokerMuxPlan) bool {
	if !plan.LiveEnabled {
		return false
	}
	format := capability.NormalizeFormat(plan.CaptureFormat)
	return plan.ToneMappingEnabled || format != capability.NormalizeFormat(plan.LiveCodec) ||
		plan.MainWidth != plan.CaptureWidth || plan.MainHeight != plan.CaptureHeight ||
		plan.MainFPS != plan.CaptureFPS ||
		videoproviders.NormalizeVideoOrientation(plan.VideoOrientation) != videoproviders.VideoOrientationNormal
}

func brokerArchiveEncodeRequired(plan videoproviders.BrokerMuxPlan) bool {
	if !plan.ArchiveEnabled {
		return false
	}
	captureFormat := capability.NormalizeFormat(plan.CaptureFormat)
	if captureFormat != "h264" && captureFormat != "h265" {
		return true
	}
	archiveCodec := capability.NormalizeFormat(plan.ArchiveCodec)
	if archiveCodec != "" && archiveCodec != "auto" && archiveCodec != captureFormat {
		return true
	}
	if plan.ArchiveWidth != plan.CaptureWidth || plan.ArchiveHeight != plan.CaptureHeight ||
		plan.ArchiveFPS != plan.CaptureFPS ||
		videoproviders.NormalizeVideoOrientation(plan.VideoOrientation) != videoproviders.VideoOrientationNormal ||
		strings.EqualFold(strings.TrimSpace(plan.ArchiveRateControl), "avbr") {
		return true
	}
	content := strings.ToLower(strings.TrimSpace(plan.ArchiveContent))
	return content == "processed" || (content != "source" && plan.ToneMappingEnabled)
}

func brokerPixelTransformRoles(plan videoproviders.BrokerMuxPlan) []string {
	oriented := videoproviders.NormalizeVideoOrientation(plan.VideoOrientation) !=
		videoproviders.VideoOrientationNormal
	roles := make([]string, 0, 3)
	if plan.AnalysisEnabled && (oriented || plan.SubWidth != plan.CaptureWidth || plan.SubHeight != plan.CaptureHeight) {
		roles = append(roles, "analysis")
	}
	if plan.ArchiveEnabled && (oriented || plan.ArchiveWidth != plan.CaptureWidth || plan.ArchiveHeight != plan.CaptureHeight) {
		roles = append(roles, "archive")
	}
	if plan.LiveEnabled && (oriented || plan.MainWidth != plan.CaptureWidth || plan.MainHeight != plan.CaptureHeight) {
		roles = append(roles, "live")
	}
	return roles
}

func resolveBrokerArchiveCodec(plan videoproviders.BrokerMuxPlan, inventory capability.Inventory) (string, error) {
	if !plan.ArchiveEnabled {
		return "", nil
	}
	configured := capability.NormalizeFormat(plan.ArchiveCodec)
	if configured == "h264" || configured == "h265" {
		return configured, nil
	}
	if configured != "" && configured != "auto" {
		return "", fmt.Errorf("unsupported archive codec %q", plan.ArchiveCodec)
	}
	capture := capability.NormalizeFormat(plan.CaptureFormat)
	if !brokerArchiveEncodeRequired(plan) && (capture == "h264" || capture == "h265") {
		return capture, nil
	}
	requested := capability.NormalizeProvider(plan.EncodeProvider)
	if requested == "" {
		return "", fmt.Errorf("unsupported encode provider %q", plan.EncodeProvider)
	}
	for _, codec := range []string{"h265", "h264"} {
		if requested == "auto" {
			if inventory.HardwareAvailable(capability.StageEncode, codec) {
				return codec, nil
			}
		} else if requested != "software" && inventory.Supports(requested, capability.StageEncode, codec, true) {
			return codec, nil
		}
	}
	if plan.RequireHWCodec {
		return "", errors.New("no permitted hardware archive encoder is available")
	}
	if plan.AllowSWFallback || requested == "software" {
		for _, codec := range []string{"h264", "h265"} {
			if inventory.Supports("software", capability.StageEncode, codec, false) {
				return codec, nil
			}
		}
	}
	return "", errors.New("no permitted archive encoder is available")
}

func (rt *videoBrokerRuntime) drainSubFramesLocked() {
	if rt.subDispatch == nil {
		return
	}
	for {
		select {
		case frame := <-rt.subDispatch:
			releaseBrokerMuxPayload(frame.payload)
			rt.metrics.subIngressDrops++
			rt.totalSubDrops++
		default:
			return
		}
	}
}

func estimateBrokerBitrateKbps(width, height, framerate int) int {
	if width <= 0 || height <= 0 || framerate <= 0 {
		return 4000
	}
	pixelsPerSecond := width * height * framerate
	bitsPerSecond := (pixelsPerSecond * 18) / 100
	if bitsPerSecond < 2_000_000 {
		bitsPerSecond = 2_000_000
	}
	if bitsPerSecond > 12_000_000 {
		bitsPerSecond = 12_000_000
	}
	return bitsPerSecond / 1000
}

func (rt *videoBrokerRuntime) stopChildLocked() {
	if rt.cameraAdmission.DevicePath != "" {
		rt.writeCameraAdmissionStateLocked("stopped", "broker_child_stopped")
	}
	if rt.childCancel != nil {
		rt.childCancel()
	}
	if rt.childIn != nil {
		_ = rt.childIn.Close()
	}
	// CommandContext sends SIGTERM through cmd.Cancel and escalates after
	// WaitDelay. Immediate SIGKILL would skip V4L2 STREAMOFF and vendor codec
	// teardown, poisoning the next capture epoch on some UVC modules.
	rt.childRunning = false
	rt.childIn = nil
	rt.childControl = nil
	rt.childFanoutControl = nil
	rt.childFanoutSupported = false
	rt.childFanoutControlMode = ""
	rt.failWorkerFanoutWaitersLocked(errors.New("capture worker stopped before fan-out receipt"))
	rt.workerFanoutWaiters = nil
	rt.childControlAvailable = false
	rt.childControlSupported = false
	rt.childLiveControlMode = ""
	rt.childRecovery = false
	rt.resetBridgeControlReadyLocked()
	rt.mainBootstrap = nil
	rt.mainCodecConfig = nil
	rt.mainBootstrapBytes = 0
	rt.mainAwaitingEncoderEpoch = false
	rt.mainAwaitingKeyframe = false
	rt.mainBootstrapRecoveryAt = time.Time{}
	rt.clearArchiveBootstrapLocked()
	// Terminal telemetry belongs to the child epoch until finishChild has
	// waited for the process and drained stderr.  Clearing it here used to turn
	// a healthy controlled shutdown into duration=0, admitted=false and an
	// unknown mode in capture_epoch_ended.
}

func (rt *videoBrokerRuntime) terminateChildForRecoveryLocked(reason string) {
	if rt.childCmd == nil || rt.childCmd.Process == nil || !rt.childRunning {
		return
	}
	rt.noteCameraModeRuntimeFailureLocked(reason)
	rt.recordChildFailure(rt.mediaEpoch, rt.childCmd.Process.Pid, reason)
	log.Printf(
		"[video-broker] event=capture_recovery_requested incident_id=%s media_epoch=%d child_pid=%d reason=%s action=terminate_child",
		captureIncidentID(rt.mediaEpoch),
		rt.mediaEpoch,
		rt.childCmd.Process.Pid,
		reason,
	)
	if rt.childCancel != nil {
		rt.childCancel()
	}
}

func (rt *videoBrokerRuntime) scanStderr(ctx context.Context, stderr io.ReadCloser, mediaEpoch uint64, childPID int) {
	sc := bufio.NewScanner(stderr)
	for sc.Scan() {
		line, sanitation := normalizeBrokerChildStderrLine(sc.Bytes())
		if strings.TrimSpace(line) == "" {
			continue
		}
		if rt.acceptWorkerFanoutReceipt(line, mediaEpoch, childPID) {
			continue
		}
		rt.logf(
			"[video-broker] bridge_incident_id=%s bridge_media_epoch=%d bridge_child_pid=%d %s%s",
			captureIncidentID(mediaEpoch), mediaEpoch, childPID, line, sanitation.logSuffix(),
		)
		if reason := brokerChildTerminalFailureReason(line); reason != "" {
			rt.recordChildFailure(mediaEpoch, childPID, reason)
		}
		if ready, ok := parseBrokerBridgeControlReadyTelemetry(line); ok {
			rt.mu.Lock()
			accepted := rt.noteBridgeControlReadyLocked(mediaEpoch, childPID, ready, time.Now())
			rt.mu.Unlock()
			if accepted {
				log.Printf(
					"[video-broker] event=bridge_control_ready sequence=%d media_epoch=%d child_pid=%d encoder_epoch=%d live_active=%t live_fps=%d live_kbps=%d analysis_fps=%d action=first_encoded_access_unit_attested",
					ready.Sequence, mediaEpoch, childPID, ready.EncoderEpoch, ready.MainActive,
					ready.MainFPS, ready.MainBitrateKbps, ready.SubFPS)
			}
		}
		if capability, ok := parseBrokerRuntimeCapability(line); ok {
			rt.mu.Lock()
			if rt.mediaEpoch != mediaEpoch {
				rt.mu.Unlock()
				continue
			}
			rt.applyRuntimeCapabilityLocked(capability)
			log.Printf(
				"[video-broker] runtime media capture_codec=%s capture=%dx%d capture_fps=%d archive_codec=%s archive_delivery=%s archive_fps=%d live_control_mode=%s fanout_control_mode=%s",
				capability.CaptureCodec,
				capability.CaptureWidth,
				capability.CaptureHeight,
				capability.CaptureFPS,
				capability.ArchiveCodec,
				capability.ArchiveDelivery,
				capability.ArchiveFPS,
				capability.LiveControlMode,
				capability.FanoutControlMode,
			)
			rt.mu.Unlock()
		}
		if tone, ok := parseBrokerToneMappingTelemetry(line); ok {
			rt.mu.Lock()
			if rt.mediaEpoch != mediaEpoch {
				rt.mu.Unlock()
				continue
			}
			rt.applyToneMappingTelemetryLocked(tone)
			rt.mu.Unlock()
		}
		if trial, ok := parseBrokerCaptureDecoderTrialTelemetry(line); ok {
			var state hardwareComponentRecovery
			var changed bool
			var scheduleRetry bool
			rt.mu.Lock()
			if rt.mediaEpoch == mediaEpoch && brokerChildPID(rt.childCmd) == childPID {
				switch trial.Event {
				case "candidate_ready":
					now := time.Now()
					state, changed = rt.noteHardwareComponentCandidateReadyLocked(
						trial.ComponentID,
						mediaEpoch,
						childPID,
						trial.RetryToken,
						time.UnixMilli(trial.CompletedAtUnixMS),
						now,
					)
					if changed {
						rt.queueBridgeControlForceLocked("hardware_component_candidate_ready")
						rt.scheduleHardwareComponentTrialTimeoutLocked(ctx, state)
					}
				case "candidate_canceled":
					state, changed, scheduleRetry = rt.acknowledgeHardwareComponentCandidateCanceledLocked(
						trial.ComponentID, mediaEpoch, childPID, trial.RetryToken, time.Now())
					if changed {
						rt.queueBridgeControlForceLocked("hardware_component_candidate_canceled")
						if scheduleRetry {
							rt.scheduleHardwareComponentRetryLocked(ctx, state)
						}
					}
				}
			}
			rt.mu.Unlock()
			if changed {
				log.Printf(
					"[video-broker] event=hardware_component_%s component=%s retry_token=%d media_epoch=%d child_pid=%d action=%s",
					trial.Event,
					state.ComponentID,
					trial.RetryToken,
					mediaEpoch,
					childPID,
					map[bool]string{true: "commit_candidate", false: "clear_or_rollback_candidate"}[trial.Event == "candidate_ready"],
				)
			}
		}
		if decoder, ok := parseBrokerCaptureDecoderTelemetry(line); ok {
			var failedComponent string
			var recovery hardwareComponentRecovery
			var recoveryChanged bool
			var halfOpen hardwareComponentRecovery
			var halfOpenChanged bool
			rt.mu.Lock()
			if rt.mediaEpoch != mediaEpoch ||
				(rt.childCmd != nil && brokerChildPID(rt.childCmd) != childPID) {
				rt.mu.Unlock()
				continue
			}
			var retryState hardwareComponentRecovery
			if decoder.RetryToken != 0 {
				var retryOK bool
				retryState, retryOK = rt.hardwareRecoveryForTelemetryLocked(
					decoder.RetryToken, mediaEpoch, childPID)
				if !retryOK {
					rt.mu.Unlock()
					continue
				}
				if strings.EqualFold(decoder.Selected, "jpu") &&
					(!retryState.TrialCommitRequested || retryState.TrialCandidateReady.IsZero() ||
						retryState.TrialDeadline.IsZero() || !time.Now().Before(retryState.TrialDeadline)) {
					rt.mu.Unlock()
					continue
				}
			}
			if decoder.RetryToken != 0 && captureDecoderTelemetryReportsHardwareFailure(decoder) {
				failedComponent = retryState.ComponentID
			} else {
				failedComponent = rt.captureDecoderHardwareFailureComponentLocked(decoder)
			}
			if failedComponent == "" && strings.EqualFold(decoder.Selected, "jpu") {
				componentID := retryState.ComponentID
				if componentID == "" {
					componentID = rt.captureDecoderHardwareComponentLocked(decoder)
				}
				if _, recovering := rt.componentRecovery[componentID]; recovering {
					halfOpen, halfOpenChanged = rt.noteHardwareComponentHalfOpenLocked(
						componentID, mediaEpoch, childPID, decoder.RetryToken, time.Now())
					if !halfOpenChanged {
						// Recovery telemetry is causal state, not a hint. A stale same-epoch
						// token-zero line must not move the plan to hardware and suppress
						// the still-pending retry timer.
						rt.mu.Unlock()
						continue
					}
				}
			}
			rt.observedDecodeCodec = decoder.Codec
			rt.observedDecodeRequested = decoder.Requested
			rt.observedDecodeBackend = decoder.Selected
			rt.observedDecodeFallback = decoder.Fallback
			rt.attestCaptureDecoderLocked(decoder)
			if failedComponent != "" {
				recovery, recoveryChanged = rt.noteHardwareComponentFailureLocked(
					failedComponent,
					capability.StageDecode,
					decoder.Codec,
					mediaEpoch,
					decoder.RetryToken,
					time.Now(),
				)
				if recoveryChanged {
					rt.queueBridgeControlForceLocked("hardware_component_retry_failed")
					rt.scheduleHardwareComponentRetryLocked(ctx, recovery)
				}
			} else if halfOpenChanged {
				rt.queueBridgeControlForceLocked("hardware_component_retry_acknowledged")
				rt.scheduleHardwareComponentStableConfirmationLocked(ctx, halfOpen)
			}
			rt.mu.Unlock()
			if failedComponent != "" && recoveryChanged {
				log.Printf(
					"[video-broker] event=hardware_component_degraded component=%s stage=decode format=%s failures=%d threshold=%d state=%s retry_after=%s action=temporary_cpu_continuity",
					failedComponent,
					recovery.Format,
					recovery.ConsecutiveFailures,
					brokerComponentFailureThreshold,
					hardwareComponentRecoveryState(recovery, time.Now()),
					recovery.RetryAfter.UTC().Format(time.RFC3339Nano),
				)
			} else if halfOpenChanged {
				log.Printf(
					"[video-broker] event=hardware_component_half_open component=%s stage=%s format=%s media_epoch=%d failures=%d stable_after=%s action=prove_hardware_stability",
					halfOpen.ComponentID,
					halfOpen.Stage,
					halfOpen.Format,
					halfOpen.HalfOpenEpoch,
					halfOpen.ConsecutiveFailures,
					halfOpen.StableAfter.UTC().Format(time.RFC3339Nano),
				)
			}
		}
		if stage, ok := parseBrokerStageCapabilityTelemetry(line); ok {
			rt.mu.Lock()
			if rt.mediaEpoch != mediaEpoch {
				rt.mu.Unlock()
				continue
			}
			rt.reconcileRuntimeStageCapabilityLocked(stage)
			rt.mu.Unlock()
		}
		rt.handleUVCBridgeFailureForEpoch(line, mediaEpoch)
	}
}

type brokerChildStderrSanitation struct {
	invalidUTF8   int
	controlBytes  int
	ansiSequences int
}

func (sanitation brokerChildStderrSanitation) logSuffix() string {
	result := ""
	if sanitation.invalidUTF8 > 0 {
		result += fmt.Sprintf(" sanitized_invalid_utf8=%d", sanitation.invalidUTF8)
	}
	if sanitation.controlBytes > 0 {
		result += fmt.Sprintf(" sanitized_control_bytes=%d", sanitation.controlBytes)
	}
	if sanitation.ansiSequences > 0 {
		result += fmt.Sprintf(" sanitized_ansi_sequences=%d", sanitation.ansiSequences)
	}
	return result
}

// normalizeBrokerChildStderrLine is the single ingestion boundary for native
// child diagnostics. Every log sink, structured parser and terminal matcher
// consumes its result; none may interpret terminal-decorated raw bytes.
func normalizeBrokerChildStderrLine(raw []byte) (string, brokerChildStderrSanitation) {
	var normalized strings.Builder
	var sanitation brokerChildStderrSanitation
	normalized.Grow(len(raw))
	for offset := 0; offset < len(raw); {
		if end, ok := brokerTerminalEscapeEnd(raw, offset); ok {
			sanitation.ansiSequences++
			offset = end
			continue
		}
		r, size := utf8.DecodeRune(raw[offset:])
		if r == utf8.RuneError && size == 1 {
			normalized.WriteByte('?')
			sanitation.invalidUTF8++
			offset++
			continue
		}
		switch r {
		case 0x009b:
			sanitation.ansiSequences++
			offset = brokerCSIEnd(raw, offset+size)
			continue
		case 0x0090, 0x0098, 0x009e, 0x009f:
			sanitation.ansiSequences++
			offset = brokerControlStringEnd(raw, offset+size, false)
			continue
		case 0x009d:
			sanitation.ansiSequences++
			offset = brokerControlStringEnd(raw, offset+size, true)
			continue
		}
		if r == '\t' {
			normalized.WriteByte(' ')
			sanitation.controlBytes++
			offset += size
			continue
		}
		if unicode.IsControl(r) || unicode.Is(unicode.Cf, r) {
			sanitation.controlBytes++
			offset += size
			continue
		}
		normalized.WriteRune(r)
		offset += size
	}
	return normalized.String(), sanitation
}

func brokerTerminalEscapeEnd(raw []byte, offset int) (int, bool) {
	if offset >= len(raw) {
		return offset, false
	}
	switch raw[offset] {
	case 0x1b:
		if offset+1 >= len(raw) {
			return len(raw), true
		}
		switch raw[offset+1] {
		case '[':
			return brokerCSIEnd(raw, offset+2), true
		case ']':
			return brokerControlStringEnd(raw, offset+2, true), true
		case 'P', 'X', '^', '_':
			return brokerControlStringEnd(raw, offset+2, false), true
		default:
			end := offset + 1
			for end < len(raw) && raw[end] >= 0x20 && raw[end] <= 0x2f {
				end++
			}
			if end < len(raw) && raw[end] >= 0x30 && raw[end] <= 0x7e {
				end++
			}
			return end, true
		}
	case 0x9b:
		return brokerCSIEnd(raw, offset+1), true
	case 0x90, 0x98, 0x9e, 0x9f:
		return brokerControlStringEnd(raw, offset+1, false), true
	case 0x9d:
		return brokerControlStringEnd(raw, offset+1, true), true
	default:
		return offset, false
	}
}

func brokerCSIEnd(raw []byte, offset int) int {
	for offset < len(raw) {
		value := raw[offset]
		if value >= 0x40 && value <= 0x7e {
			return offset + 1
		}
		if value < 0x20 || value > 0x3f {
			return offset
		}
		offset++
	}
	return len(raw)
}

func brokerControlStringEnd(raw []byte, offset int, bellTerminated bool) int {
	for offset < len(raw) {
		if bellTerminated && raw[offset] == 0x07 {
			return offset + 1
		}
		if raw[offset] == 0x1b && offset+1 < len(raw) && raw[offset+1] == '\\' {
			return offset + 2
		}
		if raw[offset] == 0x9c {
			return offset + 1
		}
		if raw[offset] == 0xc2 && offset+1 < len(raw) && raw[offset+1] == 0x9c {
			return offset + 2
		}
		offset++
	}
	return len(raw)
}

func (rt *videoBrokerRuntime) resetChildFailure(mediaEpoch uint64) {
	rt.childFailureMu.Lock()
	rt.childFailureEpoch = mediaEpoch
	rt.childFailureReason = ""
	rt.childFailureAt = time.Time{}
	rt.childFailureMu.Unlock()
}

func (rt *videoBrokerRuntime) recordChildFailure(mediaEpoch uint64, childPID int, reason string) bool {
	reason = strings.TrimSpace(reason)
	if mediaEpoch == 0 || reason == "" {
		return false
	}
	now := time.Now()
	rt.childFailureMu.Lock()
	if rt.childFailureEpoch != mediaEpoch {
		rt.childFailureMu.Unlock()
		return false
	}
	if rt.childFailureReason != "" {
		rt.childFailureMu.Unlock()
		return false
	}
	rt.childFailureReason = reason
	rt.childFailureAt = now
	rt.childFailureMu.Unlock()
	log.Printf(
		"[video-broker] event=capture_fault incident_id=%s media_epoch=%d child_pid=%d reason=%s",
		captureIncidentID(mediaEpoch),
		mediaEpoch,
		childPID,
		reason,
	)
	return true
}

func (rt *videoBrokerRuntime) childFailure(mediaEpoch uint64) (string, time.Time) {
	rt.childFailureMu.Lock()
	defer rt.childFailureMu.Unlock()
	if rt.childFailureEpoch != mediaEpoch {
		return "", time.Time{}
	}
	return rt.childFailureReason, rt.childFailureAt
}

func (rt *videoBrokerRuntime) handleUVCBridgeFailure(line string) {
	rt.handleUVCBridgeFailureForEpoch(line, 0)
}

func (rt *videoBrokerRuntime) handleUVCBridgeFailureForEpoch(line string, mediaEpoch uint64) {
	reason := brokerUVCBridgeFailureReason(line)
	if reason == "" || !strings.EqualFold(rt.plan.Transport, videoproviders.TransportV4L2UVC) {
		return
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if mediaEpoch != 0 && rt.mediaEpoch != mediaEpoch {
		return
	}
	if reason == "uvc_streamon_failed" && rt.deferUVCProducerHandoffFailureLocked(time.Now()) {
		return
	}
	rt.noteCameraModeRuntimeFailureLocked(reason)
	log.Printf(
		"[video-broker] event=capture_mode_failure incident_id=%s media_epoch=%d child_pid=%d device=%s reason=%s action=quarantine_mode_and_backoff no_device_reset=true",
		captureIncidentID(rt.mediaEpoch),
		rt.mediaEpoch,
		brokerChildPID(rt.childCmd),
		resolveVideoBrokerCaptureDevice(rt.plan),
		reason,
	)
}

// deferUVCProducerHandoffFailureLocked treats a bounded sequence of STREAMON
// failures as a device handoff problem when the broker is starting for the
// first time or reopening a native mode already admitted by this process.
// Some UVC firmware needs a quiet interval after the previous producer closes;
// cycling through advertised modes during that interval both delays recovery
// and poisons valid modes in the persistent circuit breaker. A mode never
// admitted by this process still falls through to the ordinary mode-specific
// quarantine policy. The retry budget is process-local and bounded.
func (rt *videoBrokerRuntime) deferUVCProducerHandoffFailureLocked(now time.Time) bool {
	if rt == nil || rt.cameraAdmission.DevicePath == "" ||
		!strings.EqualFold(rt.plan.Transport, videoproviders.TransportV4L2UVC) {
		return false
	}
	modeKey := cameraAdmissionModeKey(rt.cameraAdmission)
	_, previouslyAdmitted := rt.uvcAdmittedModes[modeKey]
	if rt.everProducedFrames && !previouslyAdmitted {
		return false
	}
	if rt.uvcHandoffStartedAt.IsZero() {
		rt.uvcHandoffStartedAt = now
	}
	// A bridge may print the same terminal STREAMON failure more than once.
	// Count at most one recovery outcome for each child epoch, including after
	// the handoff budget expires and the mode-specific circuit owns recovery.
	if rt.mediaEpoch != 0 && rt.mediaEpoch == rt.uvcHandoffLastMediaEpoch {
		return true
	}
	rt.uvcHandoffLastMediaEpoch = rt.mediaEpoch
	if now.Sub(rt.uvcHandoffStartedAt) >= brokerUVCHandoffRetryBudget {
		return false
	}
	rt.uvcHandoffFailures++
	delay := brokerUVCHandoffRetryInitial
	for step := 1; step < rt.uvcHandoffFailures && delay < brokerUVCHandoffRetryMaximum; step++ {
		delay *= 2
		if delay > brokerUVCHandoffRetryMaximum {
			delay = brokerUVCHandoffRetryMaximum
		}
	}
	rt.scheduleUVCQuietLocked(now, delay)
	rt.writeCameraAdmissionStateLocked("probing", "uvc_producer_handoff_quiet_retry")
	log.Printf(
		"[video-broker] event=capture_recovery_scheduled incident_id=%s media_epoch=%d child_pid=%d device=%s reason=uvc_producer_handoff attempt=%d retry_in_ms=%d budget_remaining_ms=%d action=retry_same_native_mode no_device_reset=true",
		captureIncidentID(rt.mediaEpoch), rt.mediaEpoch, brokerChildPID(rt.childCmd), resolveVideoBrokerCaptureDevice(rt.plan), rt.uvcHandoffFailures, delay.Milliseconds(),
		(brokerUVCHandoffRetryBudget - now.Sub(rt.uvcHandoffStartedAt)).Milliseconds(),
	)
	return true
}

func (rt *videoBrokerRuntime) scheduleUVCQuietLocked(now time.Time, delay time.Duration) {
	if rt == nil || delay <= 0 {
		return
	}
	rt.uvcHandoffQuietEpoch = rt.mediaEpoch
	rt.uvcHandoffQuietDelay = delay
	retryAt := now.Add(delay)
	if rt.nextChildStartAt.Before(retryAt) {
		rt.nextChildStartAt = retryAt
	}
}

// reanchorUVCQuietAfterChildLocked starts the promised quiet interval only
// after Wait has completed and the bridge has executed STREAMOFF/close.  A
// retry timestamp derived from the earlier stderr line is not proof that the
// UVC device has actually been idle for the requested duration.
func (rt *videoBrokerRuntime) reanchorUVCQuietAfterChildLocked(now time.Time, mediaEpoch uint64, failureReason string) {
	if rt == nil || failureReason != "uvc_streamon_failed" ||
		rt.uvcHandoffQuietEpoch != mediaEpoch || rt.uvcHandoffQuietDelay <= 0 {
		return
	}
	delay := rt.uvcHandoffQuietDelay
	retryAt := now.Add(delay)
	if rt.nextChildStartAt.Before(retryAt) {
		rt.nextChildStartAt = retryAt
	}
	log.Printf(
		"[video-broker] event=capture_recovery_quiet_started incident_id=%s media_epoch=%d child_pid=%d reason=uvc_streamon_failed quiet_ms=%d retry_at=%s action=hold_device_closed no_device_reset=true",
		captureIncidentID(mediaEpoch), mediaEpoch, brokerChildPID(rt.childCmd), delay.Milliseconds(), retryAt.UTC().Format(time.RFC3339Nano),
	)
	rt.uvcHandoffQuietEpoch = 0
	rt.uvcHandoffQuietDelay = 0
}

func configureBrokerChildLifecycle(cmd *exec.Cmd) {
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
	// os/exec's fallback targets only the group leader. Keep it behind the
	// broker-owned whole-group escalation so portable bridge grandchildren
	// cannot survive an uncooperative teardown.
	cmd.WaitDelay = brokerChildWaitFallback
}

func abortBrokerChildProcessGroup(cmd *exec.Cmd) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	pid := cmd.Process.Pid
	// Setpgid is requested before Start. Target the intended group first even
	// if the post-Start attestation failed, then kill the leader as a fallback.
	_ = syscall.Kill(-pid, syscall.SIGKILL)
	_ = cmd.Process.Kill()
	_ = cmd.Wait()
}

func enforceBrokerChildTermination(ctx context.Context, cmd *exec.Cmd, done <-chan struct{}, grace time.Duration) {
	if cmd == nil || cmd.Process == nil || grace <= 0 {
		return
	}
	pid := cmd.Process.Pid
	select {
	case <-done:
		// Once the leader has been reaped, never retain a numeric PGID for a
		// later timer: Linux may reuse it. A surviving member is necessarily
		// from the just-reaped owned group at this immediate boundary, so
		// escalate once now; an empty group ends ownership permanently.
		if brokerProcessGroupAlive(pid) {
			escalateBrokerChildProcessGroup(pid, 0)
		}
		return
	case <-ctx.Done():
	}
	timer := time.NewTimer(grace)
	defer timer.Stop()
	select {
	case <-done:
		if brokerProcessGroupAlive(pid) {
			escalateBrokerChildProcessGroup(pid, 0)
		}
		return
	case <-timer.C:
		escalateBrokerChildProcessGroup(pid, grace)
	}
}

func escalateBrokerChildProcessGroup(pid int, grace time.Duration) {
	err := brokerKillProcessGroup(pid)
	if err != nil && !errors.Is(err, syscall.ESRCH) {
		log.Printf(
			"[video-broker] event=capture_child_escalation_failed child_pid=%d child_pgid=%d signal=KILL err=%v",
			pid, pid, err,
		)
		return
	}
	log.Printf(
		"[video-broker] event=capture_child_escalated child_pid=%d child_pgid=%d signal=KILL grace_ms=%d",
		pid, pid, grace.Milliseconds(),
	)
}

func brokerProcessGroupExists(pgid int) bool {
	if pgid <= 0 {
		return false
	}
	err := syscall.Kill(-pgid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}

func brokerChildPID(cmd *exec.Cmd) int {
	if cmd == nil || cmd.Process == nil {
		return 0
	}
	return cmd.Process.Pid
}

func captureIncidentID(mediaEpoch uint64) string {
	return fmt.Sprintf("%d-%d", os.Getpid(), mediaEpoch)
}

func (rt *videoBrokerRuntime) resetCadenceHealthLocked() {
	if rt == nil {
		return
	}
	rt.cadenceDegraded = false
	rt.cadenceRole = ""
	rt.cadenceMeasuredFPS = 0
	rt.cadenceThresholdFPS = 0
	rt.streamPerformance = nil
}

func (rt *videoBrokerRuntime) resetUVCHandoffFailuresLocked() {
	if rt == nil {
		return
	}
	rt.uvcHandoffStartedAt = time.Time{}
	rt.uvcHandoffLastMediaEpoch = 0
	rt.uvcHandoffFailures = 0
	rt.uvcHandoffQuietEpoch = 0
	rt.uvcHandoffQuietDelay = 0
}

func brokerUVCBridgeFailureReason(line string) string {
	if !isBrokerUVCStreamOnFailure(line) {
		return ""
	}
	lower := strings.ToLower(line)
	if strings.Contains(lower, "[camera-admission]") &&
		strings.Contains(lower, "reason=runtime_attestation_timeout") {
		return "runtime_attestation_timeout_after_bridge_window"
	}
	if strings.Contains(lower, "vidioc_streamon failed") {
		return "uvc_streamon_failed"
	}
	return "uvc_capture_stalled"
}

func brokerChildTerminalFailureReason(line string) string {
	if reason := brokerWorkerOperationFailureReason(strings.TrimSpace(line)); reason != "" {
		return reason
	}
	if reason := brokerCaptureIntegrityFailureReason(strings.TrimSpace(line)); reason != "" {
		return reason
	}
	if reason := brokerFrameSourceFailureReason(strings.TrimSpace(line)); reason != "" {
		return reason
	}
	lower := strings.ToLower(strings.TrimSpace(line))
	switch {
	case strings.Contains(lower, "vidioc_streamon failed"):
		return "uvc_streamon_failed"
	case strings.Contains(lower, "vidioc_dqbuf failed") && strings.Contains(lower, "no such device"):
		return "uvc_dqbuf_enodev"
	case strings.Contains(lower, "vidioc_dqbuf failed"):
		return "capture_dqbuf_failed"
	case strings.Contains(lower, "capture stalled after"):
		return "uvc_capture_stalled"
	case strings.Contains(lower, "capture select failed"):
		return "capture_select_failed"
	case strings.Contains(lower, "corrupt capture limit reached"):
		return "corrupt_capture_limit"
	case strings.Contains(lower, "reason=decoder_no_output_timeout"):
		return "decoder_no_output_timeout"
	case strings.Contains(lower, "reason=decoder_runtime_failed"):
		return "decoder_runtime_failed"
	case strings.Contains(lower, "reason=runtime_attestation_timeout"):
		return "runtime_attestation_timeout_after_bridge_window"
	case strings.Contains(lower, "invalid buffer index="):
		return "capture_buffer_index_invalid"
	default:
		return ""
	}
}

func (rt *videoBrokerRuntime) applyRuntimeCapabilityLocked(capability brokerRuntimeCapability) {
	previousControlSupport := rt.childControlSupported
	previousFanoutSupport := rt.childFanoutSupported
	if capability.LiveEnabled != rt.plan.LiveEnabled ||
		capability.AnalysisEnabled != rt.plan.AnalysisEnabled ||
		capability.ArchiveEnabled != rt.plan.ArchiveEnabled {
		reason := fmt.Sprintf(
			"runtime_output_contract_mismatch configured=%t/%t/%t observed=%t/%t/%t",
			rt.plan.LiveEnabled, rt.plan.AnalysisEnabled, rt.plan.ArchiveEnabled,
			capability.LiveEnabled, capability.AnalysisEnabled, capability.ArchiveEnabled,
		)
		rt.writeCameraAdmissionStateLocked("rejected", reason)
		rt.terminateChildForRecoveryLocked(reason)
		return
	}
	configuredLiveCodec := "none"
	if rt.plan.LiveEnabled {
		configuredLiveCodec = brokerLiveCodec(rt.plan.MainFormat)
	}
	if capability.LiveCodec != configuredLiveCodec {
		reason := fmt.Sprintf(
			"runtime_live_codec_mismatch configured=%s observed=%s",
			configuredLiveCodec,
			capability.LiveCodec,
		)
		rt.writeCameraAdmissionStateLocked("rejected", reason)
		rt.terminateChildForRecoveryLocked(reason)
		return
	}
	rt.observedCaptureCodec = capability.CaptureCodec
	rt.observedCaptureWidth = capability.CaptureWidth
	rt.observedCaptureHeight = capability.CaptureHeight
	rt.observedCaptureFPS = capability.CaptureFPS
	rt.observedArchiveCodec = capability.ArchiveCodec
	rt.observedArchiveDelivery = capability.ArchiveDelivery
	rt.observedArchiveFPS = capability.ArchiveFPS
	rt.childLiveControlMode = capability.LiveControlMode
	if selected := rt.cameraAdmission; selected.DevicePath != "" &&
		(!strings.EqualFold(capability.CaptureCodec, selected.Format) ||
			capability.CaptureWidth != selected.CaptureWidth ||
			capability.CaptureHeight != selected.CaptureHeight ||
			capability.CaptureFPS+1 < selected.CaptureFPS) {
		reason := fmt.Sprintf(
			"runtime_contract_mismatch selected=%s/%dx%d@%d observed=%s/%dx%d@%d",
			selected.Format, selected.CaptureWidth, selected.CaptureHeight, selected.CaptureFPS,
			capability.CaptureCodec, capability.CaptureWidth, capability.CaptureHeight, capability.CaptureFPS,
		)
		rt.writeCameraAdmissionStateLocked("rejected", reason)
		rt.terminateChildForRecoveryLocked(reason)
		return
	}
	if capability.FanoutControlMode == videoproviders.WorkerFanoutControlBranchTransactionV1 &&
		!rt.childControlAvailable {
		reason := "runtime_fanout_control_without_stdin"
		rt.writeCameraAdmissionStateLocked("rejected", reason)
		rt.terminateChildForRecoveryLocked(reason)
		return
	}
	rt.childFanoutSupported = capability.FanoutControlMode ==
		videoproviders.WorkerFanoutControlBranchTransactionV1 && rt.childControlAvailable
	rt.childFanoutControlMode = capability.FanoutControlMode
	if previousFanoutSupport && !rt.childFanoutSupported {
		rt.failWorkerFanoutWaitersLocked(errors.New("capture worker withdrew dynamic fan-out control"))
	}
	rt.maybeCompleteCameraAdmissionLocked()
	runtimeControlSupported := capability.LiveControlMode == "persistent_encoder_epoch" &&
		rt.childControlAvailable
	rt.childControlSupported = runtimeControlSupported
	if runtimeControlSupported == previousControlSupport {
		return
	}
	if runtimeControlSupported {
		rt.mainAwaitingEncoderEpoch = len(rt.mainClients) > 0
		// The bridge can publish VPS/SPS/PPS before its runtime capability
		// announcement. Keep those attested parameter sets when dynamic control
		// becomes available: the following encoder epoch is allowed to emit an
		// IDR-only refresh, and every consumer still needs the retained config.
		rt.armMainKeyframeGateLocked()
		rt.queueBridgeControlLocked("runtime_capability_dynamic")
		return
	}
	// A static H264 source has no encoder-epoch marker. Drop any stale
	// dynamic bootstrap without arming a gate that only such a marker can
	// release, then explicitly keep its non-switchable encoder active.
	rt.clearMainBootstrapWithoutKeyframeGateLocked()
	rt.queueBridgeControlForceLocked("runtime_capability_static")
}

func (rt *videoBrokerRuntime) watchChildFrames(ctx context.Context, cmd *exec.Cmd, watchdog time.Duration, warmup time.Duration) {
	if watchdog <= 0 {
		return
	}
	startupGrace := brokerStartupFrameGrace(watchdog, warmup)
	admissionGrace := brokerCameraAdmissionGrace(watchdog, warmup)
	interval := watchdog / 3
	if interval < time.Second {
		interval = time.Second
	}
	if interval > 5*time.Second {
		interval = 5 * time.Second
	}
	cadenceWatches := map[string]*brokerStreamCadenceWatch{
		"analysis": {role: "analysis"},
		"archive":  {role: "archive"},
		"live":     {role: "live"},
	}
	admissionObservationStarted := false
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		rt.mu.Lock()
		if rt.childCmd != cmd || !rt.childRunning {
			rt.mu.Unlock()
			return
		}
		now := time.Now()
		if rt.refreshCameraModeHealthLocked(now) && !cameraModeHealthReadOnly() {
			rt.persistCameraModeHealthLocked(now)
		}
		cameraAdmissionPending := rt.cameraAdmission.DevicePath != "" && !rt.cameraAdmitted
		childAge := time.Duration(0)
		if !rt.childStartedAt.IsZero() {
			childAge = now.Sub(rt.childStartedAt)
		}
		if cameraAdmissionPending && childAge >= startupGrace && !admissionObservationStarted {
			// Discard startup-biased cadence counters while retaining no runtime
			// decision. Subsequent frames form the steady-state proof window.
			rt.cameraAdmissionTracker = cameraAdmissionRuntimeTracker{}
			admissionObservationStarted = true
			rt.writeCameraAdmissionStateLocked("probing", "steady_state_attestation_pending")
		}
		if cameraAdmissionPending && childAge >= admissionGrace {
			rt.maybeCompleteCameraAdmissionLocked()
			cameraAdmissionPending = !rt.cameraAdmitted
			if cameraAdmissionPending {
				reason := fmt.Sprintf("runtime_attestation_timeout_after_%.1fs", now.Sub(rt.childStartedAt).Seconds())
				rt.writeCameraAdmissionStateLocked("rejected", reason)
				rt.terminateChildForRecoveryLocked(reason)
				rt.mu.Unlock()
				return
			}
		}
		baseline := rt.lastFrameAt
		if baseline.IsZero() {
			baseline = rt.childStartedAt
		}
		if baseline.IsZero() {
			baseline = now
		}
		staleFor := now.Sub(baseline)
		frameDeadline := watchdog
		if rt.lastFrameAt.IsZero() {
			frameDeadline = startupGrace
		}
		// A controlled live-only source can be parked, and a fan-out cutover
		// can remove the last role before Retire closes its capture. Neither
		// state requests media. Keep admission and every required delivery
		// subject to the existing deadline instead of restarting an idle owner.
		mediaRequired := cameraAdmissionPending
		for _, target := range rt.streamCadenceTargetsLocked() {
			mediaRequired = mediaRequired || target.required
		}
		if mediaRequired && staleFor >= frameDeadline {
			rt.terminateChildForRecoveryLocked(fmt.Sprintf("no_frames_for_%.1fs_watchdog_%.1fs", staleFor.Seconds(), frameDeadline.Seconds()))
			rt.mu.Unlock()
			return
		}
		mainRequired := len(rt.mainClients) > 0
		mainBaseline := rt.lastMainFrameAt
		if mainBaseline.IsZero() {
			mainBaseline = rt.mainDemandedAt
		}
		if mainBaseline.IsZero() {
			mainBaseline = rt.childStartedAt
		}
		if mainBaseline.IsZero() {
			mainBaseline = now
		}
		mainStaleFor := now.Sub(mainBaseline)
		mainDeadline := watchdog
		if rt.lastMainFrameAt.IsZero() {
			mainDeadline = startupGrace
		}
		if mainRequired && mainStaleFor >= mainDeadline {
			rt.terminateChildForRecoveryLocked(fmt.Sprintf("main_no_frames_for_%.1fs_watchdog_%.1fs", mainStaleFor.Seconds(), mainDeadline.Seconds()))
			rt.mu.Unlock()
			return
		}
		analysisRequired := rt.plan.AnalysisEnabled && rt.fanoutRoleGates["analysis"] == ""
		analysisBaseline := rt.lastSubFrameAt
		if analysisBaseline.IsZero() {
			analysisBaseline = rt.childStartedAt
		}
		analysisDeadline := watchdog
		// A low-cadence analysis producer is performance-degraded, not failed.
		// Give the isolated output channel a sustained no-frame window before
		// retiring the otherwise healthy camera/archive epoch.
		if analysisDeadline < 30*time.Second {
			analysisDeadline = 30 * time.Second
		}
		if rt.lastSubFrameAt.IsZero() {
			analysisDeadline = max(analysisDeadline, startupGrace)
		}
		if analysisRequired && !analysisBaseline.IsZero() && now.Sub(analysisBaseline) >= analysisDeadline {
			// The encoded archive may still advance after an output channel is
			// retired. Recover that session through the existing checked worker
			// teardown; no stale delivery can be reinterpreted as a new frame.
			rt.terminateChildForRecoveryLocked(fmt.Sprintf("analysis_no_frames_for_%.1fs_watchdog_%.1fs", now.Sub(analysisBaseline).Seconds(), analysisDeadline.Seconds()))
			rt.mu.Unlock()
			return
		}
		if cameraAdmissionPending {
			// Start performance measurement only after structural admission so
			// codec warmup does not bias the delivery window.
			for _, target := range rt.streamCadenceTargetsLocked() {
				cadenceWatches[target.role].reset(now, target.frame)
			}
			rt.resetCadenceHealthLocked()
			rt.mu.Unlock()
			continue
		}
		for _, target := range rt.streamCadenceTargetsLocked() {
			watch := cadenceWatches[target.role]
			if !target.required || target.expectedFPS <= 0 {
				watch.reset(now, target.frame)
				continue
			}
			watch.observe(
				now, target.frame, target.expectedFPS, interval, watchdog,
				rt.mediaEpoch, brokerChildPID(rt.childCmd),
			)
		}
		rt.applyStreamCadenceHealthLocked(cadenceWatches, watchdog)
		rt.mu.Unlock()
	}
}

type brokerStreamCadenceTarget struct {
	role        string
	required    bool
	frame       uint32
	expectedFPS int
}

type brokerStreamCadenceWatch struct {
	role           string
	lastFrame      uint32
	lastCheck      time.Time
	degradedFor    time.Duration
	measuredFPS    float64
	thresholdFPS   float64
	degradedLogged bool
	recoveredFor   time.Duration
	expectedFPS    int
	window         time.Duration
}

func (watch *brokerStreamCadenceWatch) reset(now time.Time, frame uint32) {
	watch.lastFrame = frame
	watch.lastCheck = now
	watch.degradedFor = 0
	watch.measuredFPS = 0
	watch.thresholdFPS = 0
	watch.degradedLogged = false
	watch.recoveredFor = 0
	watch.expectedFPS = 0
	watch.window = 0
}

func (watch *brokerStreamCadenceWatch) observe(
	now time.Time,
	frame uint32,
	expectedFPS int,
	interval time.Duration,
	watchdog time.Duration,
	mediaEpoch uint64,
	childPID int,
) {
	if watch.lastCheck.IsZero() || (watch.expectedFPS != 0 && watch.expectedFPS != expectedFPS) {
		watch.reset(now, frame)
		watch.expectedFPS = expectedFPS
		return
	}
	watch.expectedFPS = expectedFPS
	elapsed := now.Sub(watch.lastCheck)
	if elapsed < 2*time.Second {
		return
	}
	deltaFrames := frame - watch.lastFrame
	actualFPS := float64(deltaFrames) / elapsed.Seconds()
	thresholdFPS := float64(expectedFPS) * brokerCadenceWatchdogMinRatio
	watch.lastCheck = now
	watch.lastFrame = frame
	watch.measuredFPS = actualFPS
	watch.thresholdFPS = thresholdFPS
	watch.window = elapsed
	if actualFPS < thresholdFPS {
		watch.recoveredFor = 0
		watch.degradedFor += elapsed
		if !watch.degradedLogged && watch.degradedFor >= interval {
			log.Printf(
				"[video-broker] event=stream_cadence_degraded incident_id=%s media_epoch=%d child_pid=%d role=%s observed_fps=%.2f threshold_fps=%.2f degraded_for_ms=%d watchdog_ms=%d",
				captureIncidentID(mediaEpoch), mediaEpoch, childPID, watch.role, actualFPS,
				thresholdFPS, watch.degradedFor.Milliseconds(), watchdog.Milliseconds(),
			)
			watch.degradedLogged = true
		}
		return
	}
	if watch.degradedFor > 0 {
		// A separate recovery band and two measurement windows avoid flicker
		// around the warning threshold. No camera operation follows this state.
		if actualFPS < float64(expectedFPS)*brokerCadenceRecoveryRatio {
			watch.recoveredFor = 0
			return
		}
		watch.recoveredFor += elapsed
		if watch.recoveredFor < 4*time.Second {
			return
		}
	}
	if watch.degradedLogged {
		log.Printf(
			"[video-broker] event=stream_cadence_recovered incident_id=%s media_epoch=%d child_pid=%d role=%s observed_fps=%.2f threshold_fps=%.2f degraded_for_ms=%d",
			captureIncidentID(mediaEpoch), mediaEpoch, childPID, watch.role, actualFPS,
			thresholdFPS, watch.degradedFor.Milliseconds(),
		)
	}
	watch.degradedFor = 0
	watch.degradedLogged = false
	watch.recoveredFor = 0
}

func (rt *videoBrokerRuntime) streamCadenceTargetsLocked() []brokerStreamCadenceTarget {
	liveFPS := rt.plan.MainFPS
	analysisFPS := rt.plan.SubFPS
	liveRequired := rt.plan.LiveEnabled && !rt.childControlSupported
	if rt.childControlSupported {
		// The bridge control is the current producer contract. It combines the
		// safest viewer request with hardware-governor caps and can therefore be
		// lower than the catalog profile without representing lost cadence.
		// Comparing an intentionally adapted producer with the static plan makes
		// healthy congestion control falsely degrade the whole source.
		control := rt.bridgeControlTargetsLocked()
		liveFPS = control.MainFPS
		analysisFPS = control.SubFPS
		liveRequired = control.MainActive
	}
	archiveFPS := rt.plan.ArchiveFPS
	if archiveFPS <= 0 {
		archiveFPS = rt.plan.CaptureFPS
	}
	return []brokerStreamCadenceTarget{
		{
			role: "analysis", required: rt.plan.AnalysisEnabled,
			// Count actual deliveries, including causal FAP frames whose wire
			// identity is never allocated by buildBrokerHeaderLocked.
			frame: uint32(rt.totalSubFrames), expectedFPS: positiveOr(analysisFPS, brokerDefaultCadenceFPS),
		},
		{
			role: "archive", required: rt.plan.ArchiveEnabled,
			frame: rt.nextArchiveFrame, expectedFPS: positiveOr(archiveFPS, brokerDefaultCadenceFPS),
		},
		{
			role:     "live",
			required: liveRequired,
			frame:    rt.nextMainFrame, expectedFPS: positiveOr(liveFPS, brokerDefaultCadenceFPS),
		},
	}
}

func (rt *videoBrokerRuntime) applyStreamCadenceHealthLocked(
	watches map[string]*brokerStreamCadenceWatch,
	watchdog time.Duration,
) {
	performance := make(map[string]StreamPerformance, len(watches))
	for _, target := range rt.streamCadenceTargetsLocked() {
		item := StreamPerformance{State: "inactive", TargetFPS: target.expectedFPS}
		switch target.role {
		case "live":
			item.RequestedFPS = rt.plan.MainFPS
		case "analysis":
			item.RequestedFPS = rt.plan.SubFPS
		case "archive":
			item.RequestedFPS = rt.plan.ArchiveFPS
		}
		if target.required {
			item.State = "measuring"
			if watch := watches[target.role]; watch != nil && watch.window > 0 {
				item.State = "on_target"
				item.ObservedFPS, item.WindowMS = watch.measuredFPS, watch.window.Milliseconds()
				item.MeasuredAt = watch.lastCheck.UTC()
				if watch.degradedFor >= watchdog {
					item.State = "reduced"
				} else if watch.measuredFPS < watch.thresholdFPS {
					item.State = "measuring"
				}
			}
		}
		performance[target.role] = item
	}
	defer func() { rt.streamPerformance = performance }()
	var selected *brokerStreamCadenceWatch
	if rt.cadenceDegraded {
		current := watches[rt.cadenceRole]
		if current != nil && current.degradedFor >= watchdog && current.thresholdFPS > 0 {
			selected = current
		}
	}
	for _, role := range []string{"analysis", "archive", "live"} {
		watch := watches[role]
		if watch == nil || watch.degradedFor < watchdog || watch.thresholdFPS <= 0 {
			continue
		}
		if selected == nil {
			selected = watch
		}
	}
	if selected == nil {
		rt.resetCadenceHealthLocked()
		return
	}
	becameDegraded := !rt.cadenceDegraded || rt.cadenceRole != selected.role
	rt.cadenceDegraded = true
	rt.cadenceRole = selected.role
	rt.cadenceMeasuredFPS = selected.measuredFPS
	rt.cadenceThresholdFPS = selected.thresholdFPS
	if becameDegraded {
		log.Printf(
			"[video-broker] event=stream_cadence_preserved incident_id=%s media_epoch=%d child_pid=%d role=%s observed_fps=%.2f threshold_fps=%.2f degraded_for_ms=%d action=continue_without_capture_restart",
			captureIncidentID(rt.mediaEpoch), rt.mediaEpoch, brokerChildPID(rt.childCmd),
			selected.role, selected.measuredFPS, selected.thresholdFPS,
			selected.degradedFor.Milliseconds(),
		)
	}
}

func (rt *videoBrokerRuntime) forwardMuxStream(
	ctx, childCtx context.Context, childCancel context.CancelFunc, source io.Reader,
	mediaEpoch uint64, childPID int,
) {
	q := newBrokerMuxDispatchQueue(brokerMuxDispatchQueueDepth, brokerMuxDispatchQueueMaxBytes)
	readDone := make(chan error, 1)
	// The pipe reader remains independent of dispatch, status and client locks.
	// It is joined by draining q.frames before this epoch can finish/restart.
	go func() {
		err := readBrokerMuxFrames(childCtx, source, q)
		if err != nil && !errors.Is(err, io.EOF) && childCtx.Err() == nil {
			reason := "mux_read_error"
			if errors.Is(err, errBrokerMuxDispatchOverflow) {
				reason = "mux_dispatch_backpressure"
			}
			rt.recordChildFailure(mediaEpoch, childPID, reason)
			log.Printf("[video-broker] event=mux_ingress_failed incident_id=%s media_epoch=%d child_pid=%d reason=%s action=retire_source", captureIncidentID(mediaEpoch), mediaEpoch, childPID, reason)
			childCancel()
		}
		readDone <- err
	}()
	for frame := range q.frames {
		transferred := false
		if childCtx.Err() == nil {
			switch frame.streamID {
			case brokerMuxStreamLiveEncoderEpoch:
				if err := rt.applyLiveEncoderEpoch(frame.payload); err != nil {
					rt.recordChildFailure(mediaEpoch, childPID, "invalid_live_encoder_epoch")
					childCancel()
				}
			case brokerStreamNV12:
				transferred = rt.enqueueSubFrame(childCtx, mediaEpoch, frame.payload)
				if !transferred {
					childCancel()
				}
			default:
				rt.dispatchFrameForEpoch(mediaEpoch, frame.streamID, frame.payload)
			}
		}
		q.complete(frame, transferred)
	}
	<-readDone
	rt.finishChild(ctx)
}

type brokerSubFrame struct {
	mediaEpoch uint64
	payload    []byte
}

func (rt *videoBrokerRuntime) enqueueSubFrame(ctx context.Context, mediaEpoch uint64, payload []byte) bool {
	if rt.subDispatch == nil || ctx.Err() != nil {
		return false
	}
	frame := brokerSubFrame{mediaEpoch: mediaEpoch, payload: payload}
	for {
		select {
		case <-ctx.Done():
			return false
		case rt.subDispatch <- frame:
			return true
		default:
		}
		select {
		case <-ctx.Done():
			return false
		case stale := <-rt.subDispatch:
			releaseBrokerMuxPayload(stale.payload)
			rt.mu.Lock()
			rt.metrics.subIngressDrops++
			rt.totalSubDrops++
			rt.mu.Unlock()
		default:
		}
	}
}

func (rt *videoBrokerRuntime) forwardSubFrames(ctx context.Context) {
	drain := func() {
		for {
			select {
			case frame := <-rt.subDispatch:
				releaseBrokerMuxPayload(frame.payload)
			default:
				return
			}
		}
	}
	for {
		if ctx.Err() != nil {
			drain()
			return
		}
		select {
		case <-ctx.Done():
			drain()
			return
		case frame := <-rt.subDispatch:
			rt.dispatchFrameForEpoch(frame.mediaEpoch, brokerStreamNV12, frame.payload)
			releaseBrokerMuxPayload(frame.payload)
		}
	}
}

func (rt *videoBrokerRuntime) applyLiveEncoderEpoch(payload []byte) error {
	if len(payload) != 8 {
		return errors.New("invalid live encoder epoch payload")
	}
	epoch := binary.BigEndian.Uint64(payload)
	if epoch == 0 {
		return errors.New("invalid live encoder epoch value")
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if epoch <= rt.observedLiveEpoch {
		return nil
	}
	rt.observedLiveEpoch = epoch
	rt.mainAwaitingEncoderEpoch = false
	rt.armMainKeyframeGateLocked()
	log.Printf("[video-broker] live encoder epoch=%d codec=%s awaiting_keyframe=true", epoch, brokerLiveCodec(rt.plan.MainFormat))
	return nil
}

func (rt *videoBrokerRuntime) dispatchFrame(streamID byte, payload []byte) {
	rt.dispatchFrameForEpoch(0, streamID, payload)
}

func (rt *videoBrokerRuntime) dispatchFrameForEpoch(mediaEpoch uint64, streamID byte, payload []byte) {
	var subNotifyHeader brokerPacketHeader
	var subNotifier *brokerSubFrameNotifier
	var shouldNotifySub bool

	rt.mu.Lock()
	if mediaEpoch != 0 && rt.mediaEpoch != mediaEpoch {
		if streamID == brokerStreamNV12 {
			rt.metrics.subIngressDrops++
			rt.totalSubDrops++
		}
		rt.mu.Unlock()
		return
	}
	if role := workerFanoutRoleForMuxStream(streamID); role != "" {
		if rt.fanoutRoleGates[role] != "" || !fanoutRuntimeRoleEnabled(rt.plan, role) {
			rt.mu.Unlock()
			return
		}
	}
	header, ok := rt.buildBrokerHeaderLocked(streamID, payload)
	if !ok {
		rt.mu.Unlock()
		return
	}
	var clients map[int]*brokerClient
	switch streamID {
	case brokerStreamH264:
		clients = cloneBrokerClients(rt.mainClients)
	case brokerStreamH265, brokerMuxStreamArchiveH264Native, brokerMuxStreamArchiveH264Generated:
		clients = cloneBrokerClients(rt.archiveClients)
	case brokerStreamNV12:
		clients = cloneBrokerClients(rt.subClients)
	}
	var packet []byte
	var packets [][]byte
	if streamID != brokerStreamNV12 || len(clients) > 0 {
		packet = marshalBrokerPacket(header, payload)
		if len(packet) == 0 {
			rt.mu.Unlock()
			return
		}
		packets = [][]byte{packet}
	}
	now := time.Now()
	switch streamID {
	case brokerStreamH264:
		rt.attestCapabilityStageRoleLocked(capability.StageEncode, "live", brokerLiveCodec(rt.plan.MainFormat))
		if rt.observedRoleResizeBackend["live"] != "" {
			rt.attestCapabilityStageRoleLocked(capability.StageResize, "live", "nv12")
		}
	case brokerStreamNV12:
		rt.attestCapabilityStageLocked(capability.StageDecode, rt.observedCaptureCodec)
		if rt.observedResizeBackend != "" {
			rt.attestCapabilityStageRoleLocked(capability.StageResize, "analysis", "nv12")
		}
	case brokerStreamH265:
		rt.attestCapabilityStageRoleLocked(capability.StageEncode, "archive", "h265")
		if rt.observedRoleResizeBackend["archive"] != "" {
			rt.attestCapabilityStageRoleLocked(capability.StageResize, "archive", "nv12")
		}
	case brokerMuxStreamArchiveH264Generated:
		rt.attestCapabilityStageRoleLocked(capability.StageEncode, "archive", "h264")
		if rt.observedRoleResizeBackend["archive"] != "" {
			rt.attestCapabilityStageRoleLocked(capability.StageResize, "archive", "nv12")
		}
	}
	rt.lastFrameAt = now
	rt.everProducedFrames = true
	if streamID == brokerStreamH264 {
		if rt.mainAwaitingEncoderEpoch {
			rt.mu.Unlock()
			return
		}
		updateBrokerBootstrap(
			&rt.mainBootstrap,
			&rt.mainBootstrapBytes,
			&rt.mainCodecConfig,
			packet,
			header.Flags,
			brokerMainBootstrapMaxBytes,
		)
		if (header.Flags&brokerFlagKeyframe) != 0 &&
			(header.Flags&brokerFlagCodecConfig) == 0 && len(rt.mainCodecConfig) > 0 {
			// A TrackLocal binding owns its H265 payloader and therefore its own
			// VPS/SPS/PPS cache. Replaying codec config only when the Unix client
			// attaches leaves a peer added to a retained WebRTC track unable to
			// decode later IDRs. Prefix every IDR-only access unit so each active
			// peer can bootstrap independently after join or reconnect.
			packets = append(append([][]byte(nil), rt.mainCodecConfig...), packet)
		}
		if rt.mainAwaitingKeyframe {
			if brokerMainBootstrapReady(rt.plan.MainFormat, rt.mainBootstrap) {
				rt.mainAwaitingKeyframe = false
				rt.mainBootstrapRecoveryAt = time.Time{}
				packets = append([][]byte(nil), rt.mainBootstrap...)
			} else {
				// A random-access picture is not a decoder bootstrap without the
				// complete parameter-set family. Keep every client behind the same
				// gate and ask the persistent hardware encoder to regenerate headers
				// plus IDR without creating another encoder epoch.
				packets = nil
				rt.requestMainBootstrapRecoveryLocked(now)
			}
		}
	} else if isBrokerArchiveMuxStream(streamID) {
		if streamID == brokerStreamH265 {
			rt.observedArchiveCodec = "h265"
		} else {
			rt.observedArchiveCodec = "h264"
		}
		if rt.archiveCodecStreamID != 0 && rt.archiveCodecStreamID != header.StreamID {
			rt.clearArchiveBootstrapLocked()
		}
		rt.archiveCodecStreamID = header.StreamID
		updateBrokerBootstrap(
			&rt.archiveBootstrap,
			&rt.archiveBootstrapBytes,
			&rt.archiveCodecConfig,
			packet,
			header.Flags,
			brokerArchiveBootstrapMaxBytes,
		)
	}
	rt.observeCameraAdmissionFrameLocked(streamID, payload, now)
	if streamID == brokerStreamNV12 {
		if rt.subShared != nil {
			if err := rt.subShared.Write(header.FrameID, header.TimestampNS, header.Width, header.Height, header.Flags, payload); err != nil {
				log.Printf("[video-broker] sub shared frame write failed path=%s err=%v", rt.plan.SubShmPath, err)
				rt.metrics.subShmWriteErrors++
			} else {
				rt.metrics.subShmWrites++
				if rt.subNotifier != nil {
					subNotifyHeader = header
					subNotifier = rt.subNotifier
					shouldNotifySub = true
				}
			}
		}
	}
	rt.recordBrokerFrameLocked(streamID, len(payload), len(clients), now)
	rt.maybeLogBrokerMetricsLocked(now)
	rt.mu.Unlock()

	if shouldNotifySub {
		result := subNotifier.Notify(subNotifyHeader.FrameID, subNotifyHeader.TimestampNS, subNotifyHeader.Flags, time.Now())
		if result.Attempted {
			rt.mu.Lock()
			if result.Sent {
				rt.metrics.subNotifyWrites++
			} else {
				rt.metrics.subNotifyErrors++
			}
			rt.mu.Unlock()
		}
	}

	if len(clients) == 0 || len(packets) == 0 {
		return
	}

	totalDropped := 0
	maxQueue := 0
	for _, packetToFanout := range packets {
		for clientID, conn := range clients {
			ok, dropped, queueLen := conn.enqueue(packetToFanout)
			totalDropped += dropped
			if queueLen > maxQueue {
				maxQueue = queueLen
			}
			if !ok {
				delete(clients, clientID)
				rt.mu.Lock()
				endpoint := streamID
				if isBrokerArchiveMuxStream(streamID) {
					endpoint = brokerEndpointArchive
				}
				rt.detachClientLocked(endpoint, clientID)
				rt.mu.Unlock()
			}
		}
	}
	rt.mu.Lock()
	rt.recordBrokerFanoutLocked(streamID, totalDropped, maxQueue)
	rt.maybeLogBrokerMetricsLocked(time.Now())
	rt.mu.Unlock()
}

func (rt *videoBrokerRuntime) attestCapabilityStageLocked(stage, format string) {
	rt.attestCapabilityStageRoleLocked(stage, "", format)
}

func (rt *videoBrokerRuntime) attestCapabilityStageRoleLocked(stage, role, format string) {
	format = capability.NormalizeFormat(format)
	for index := range rt.capabilityPlan.Stages {
		selection := &rt.capabilityPlan.Stages[index]
		if selection.Stage != stage || (role != "" && selection.Role != role) || (format != "" && capability.NormalizeFormat(selection.Format) != format) {
			continue
		}
		selection.State = capability.StateAttested
		rt.attestCapabilityComponentLocked(selection.Component, stage, selection.Format)
	}
}

func (rt *videoBrokerRuntime) attestCapabilityComponentLocked(componentID, stage, format string) {
	for index := range rt.plan.CapabilityInventory.Components {
		component := &rt.plan.CapabilityInventory.Components[index]
		if component.ID != componentID {
			continue
		}
		component.State = capability.StateAttested
		evidenceValue := stage + ":" + capability.NormalizeFormat(format)
		for _, observed := range component.Evidence {
			if observed.Kind == "runtime_media" && observed.Value == evidenceValue {
				return
			}
		}
		component.Evidence = append(component.Evidence, capability.Evidence{Kind: "runtime_media", Value: evidenceValue})
		return
	}
}

func (rt *videoBrokerRuntime) attestToneMapperLocked(accelerator string) {
	backendSuffix := strings.ToLower(strings.TrimSpace(accelerator))
	if backendSuffix == "" {
		rt.attestCapabilityStageLocked(capability.StageToneMap, "nv12")
		return
	}
	backendPrefix := "gstreamer_"
	if rt.capabilityPlan.EncodeProvider == "native_sp" {
		backendPrefix = "native_"
	}
	backend := backendPrefix + backendSuffix
	for index := range rt.capabilityPlan.Stages {
		selection := &rt.capabilityPlan.Stages[index]
		if selection.Stage != capability.StageToneMap {
			continue
		}
		if component, ok := rt.plan.CapabilityInventory.Find(backend, capability.StageToneMap, "nv12", false); ok {
			selection.Component = component.ID
			selection.Backend = component.Backend
			selection.Hardware = component.Hardware
		}
		selection.State = capability.StateAttested
		rt.attestCapabilityComponentLocked(selection.Component, selection.Stage, selection.Format)
	}
}

func (rt *videoBrokerRuntime) applyToneMappingTelemetryLocked(tone brokerToneMappingTelemetry) {
	rt.observedToneMode = tone.ActiveMode
	rt.observedToneAccelerator = tone.Accelerator
	rt.observedToneActive = tone.Active
	rt.observedToneGainMin = tone.GainMin
	rt.observedToneGainMax = tone.GainMax
	rt.observedToneUpdates = tone.Updates
	rt.attestToneMapperLocked(tone.Accelerator)
}

func (rt *videoBrokerRuntime) attestCaptureDecoderLocked(observed brokerCaptureDecoderTelemetry) {
	for index := range rt.capabilityPlan.Stages {
		selection := &rt.capabilityPlan.Stages[index]
		if selection.Stage != capability.StageDecode || capability.NormalizeFormat(selection.Format) != capability.NormalizeFormat(observed.Codec) {
			continue
		}
		wantHardware := observed.Selected == "jpu"
		for _, component := range rt.plan.CapabilityInventory.Components {
			if !component.Available || component.Backend != selection.Backend || component.Hardware != wantHardware {
				continue
			}
			for _, operation := range component.Operations {
				if operation.Stage == capability.StageDecode && capability.NormalizeFormat(operation.Format) == capability.NormalizeFormat(observed.Codec) {
					selection.Component = component.ID
					selection.Hardware = component.Hardware
					break
				}
			}
		}
		selection.Fallback = observed.Fallback
		selection.State = capability.StateAttested
		rt.attestCapabilityComponentLocked(selection.Component, selection.Stage, selection.Format)
	}
}

// captureDecoderHardwareFailureComponentLocked maps adapter telemetry back to
// the selected graph component. The lifecycle policy therefore degrades and
// retries that component, not a board, camera model or hard-coded accelerator.
func captureDecoderTelemetryReportsHardwareFailure(observed brokerCaptureDecoderTelemetry) bool {
	fallback := strings.ToLower(strings.TrimSpace(observed.Fallback))
	return strings.EqualFold(observed.Selected, "cpu") &&
		strings.HasPrefix(fallback, "jpu_") && strings.HasSuffix(fallback, "_failed")
}

func (rt *videoBrokerRuntime) captureDecoderHardwareFailureComponentLocked(observed brokerCaptureDecoderTelemetry) string {
	if !captureDecoderTelemetryReportsHardwareFailure(observed) {
		return ""
	}
	for _, selection := range rt.capabilityPlan.Stages {
		if selection.Stage != capability.StageDecode ||
			selection.Role != "capture" ||
			capability.NormalizeFormat(selection.Format) != capability.NormalizeFormat(observed.Codec) ||
			!selection.Hardware {
			continue
		}
		return strings.TrimSpace(selection.Component)
	}
	return ""
}

func (rt *videoBrokerRuntime) hardwareRecoveryForTelemetryLocked(
	retryToken, mediaEpoch uint64,
	childPID int,
) (hardwareComponentRecovery, bool) {
	if retryToken == 0 || childPID <= 0 {
		return hardwareComponentRecovery{}, false
	}
	for _, recovery := range rt.componentRecovery {
		if recovery.TrialToken != retryToken || recovery.TrialMediaEpoch != mediaEpoch ||
			recovery.TrialChildPID != childPID {
			continue
		}
		if recovery.StableAfter.IsZero() && recovery.TrialDeadline.IsZero() {
			return hardwareComponentRecovery{}, false
		}
		return recovery, true
	}
	return hardwareComponentRecovery{}, false
}

func (rt *videoBrokerRuntime) captureDecoderHardwareComponentLocked(observed brokerCaptureDecoderTelemetry) string {
	if !strings.EqualFold(observed.Selected, "jpu") {
		return ""
	}
	for _, selection := range rt.capabilityPlan.Stages {
		if selection.Stage == capability.StageDecode &&
			selection.Role == "capture" &&
			capability.NormalizeFormat(selection.Format) == capability.NormalizeFormat(observed.Codec) {
			if selection.Hardware {
				return strings.TrimSpace(selection.Component)
			}
			// During CPU continuity the selected stage intentionally names the
			// software component. Resolve the hardware candidate from the same
			// provider contract before mutating the plan so token-zero recovery can
			// be validated causally.
			if component, ok := rt.plan.CapabilityInventory.Find(
				selection.Backend, capability.StageDecode, observed.Codec, true); ok {
				return strings.TrimSpace(component.ID)
			}
		}
	}
	return ""
}

func (rt *videoBrokerRuntime) reconcileRuntimeStageCapabilityLocked(observed brokerStageCapabilityTelemetry) {
	stageName := observed.Stage
	format := capability.NormalizeFormat(observed.Format)
	for index := range rt.capabilityPlan.Stages {
		selection := &rt.capabilityPlan.Stages[index]
		if selection.Stage != stageName || selection.Role != observed.Role || capability.NormalizeFormat(selection.Format) != format {
			continue
		}
		component, ok := rt.plan.CapabilityInventory.Find(observed.Selected, stageName, format, false)
		if !ok {
			selection.State = capability.StateRejected
			selection.Fallback = "runtime_component_missing"
			continue
		}
		selection.Component = component.ID
		selection.Backend = component.Backend
		selection.Hardware = component.Hardware
		selection.Fallback = observed.Fallback
		selection.State = capability.StateCandidate
		if observed.Role == "archive" {
			rt.capabilityPlan.ArchiveEncodeProvider = observed.Selected
		} else if observed.Role == "live" {
			rt.capabilityPlan.EncodeProvider = observed.Selected
		} else if observed.Role == "capture" {
			rt.capabilityPlan.DecodeProvider = observed.Selected
		}
		if observed.Stage == capability.StageResize {
			if rt.observedRoleResizeBackend == nil {
				rt.observedRoleResizeBackend = make(map[string]string, 3)
			}
			rt.observedRoleResizeBackend[observed.Role] = observed.Selected
			switch observed.Role {
			case "analysis":
				rt.observedResizeBackend = observed.Selected
				rt.capabilityPlan.ResizeBackend = observed.Selected
			}
		}
	}
}

func updateBrokerBootstrap(
	bootstrap *[][]byte,
	bootstrapBytes *int,
	codecConfig *[][]byte,
	packet []byte,
	flags uint16,
	maxBytes int,
) {
	hasKeyframe := (flags & brokerFlagKeyframe) != 0
	hasConfig := (flags & brokerFlagCodecConfig) != 0
	if hasConfig {
		configPacket := brokerCodecConfigPacket(packet)
		if len(configPacket) > 0 {
			*codecConfig = append(*codecConfig, configPacket)
		}
		if len(*codecConfig) > 4 {
			*codecConfig = append([][]byte(nil), (*codecConfig)[len(*codecConfig)-4:]...)
		}
		for len(*codecConfig) > 0 && maxBytes > 0 && brokerPacketListBytes(*codecConfig) > maxBytes {
			*codecConfig = append([][]byte(nil), (*codecConfig)[1:]...)
		}
		if !hasKeyframe && len(*bootstrap) > 0 {
			// Some hardware exposes the IDR before the separately dequeued
			// parameter sets. Canonicalize that ordering in the retained GOP so
			// the gate can publish VPS/SPS/PPS (or SPS/PPS) before the IDR.
			mediaPackets := make([][]byte, 0, len(*bootstrap))
			for _, retained := range *bootstrap {
				if len(retained) < brokerPacketHeaderSize {
					continue
				}
				retainedHeader, err := parseBrokerPacketHeader(retained[:brokerPacketHeaderSize])
				if err == nil &&
					(retainedHeader.Flags&brokerFlagCodecConfig) != 0 &&
					(retainedHeader.Flags&brokerFlagKeyframe) == 0 {
					continue
				}
				mediaPackets = append(mediaPackets, retained)
			}
			*bootstrap = append(append([][]byte(nil), (*codecConfig)...), mediaPackets...)
			*bootstrapBytes = brokerPacketListBytes(*bootstrap)
		}
	}
	if hasKeyframe {
		if hasConfig {
			*bootstrap = [][]byte{packet}
		} else {
			*bootstrap = append(append([][]byte(nil), (*codecConfig)...), packet)
		}
		*bootstrapBytes = brokerPacketListBytes(*bootstrap)
	} else if len(*bootstrap) > 0 && !hasConfig {
		*bootstrap = append(*bootstrap, packet)
		*bootstrapBytes += len(packet)
	}
	if maxBytes > 0 && *bootstrapBytes > maxBytes {
		// Never splice a GOP: a truncated inter-frame sequence is worse than
		// waiting for the next keyframe.
		*bootstrap = nil
		*bootstrapBytes = 0
	}
}

func brokerMainBootstrapReady(codec string, packets [][]byte) bool {
	tracker := cameraAdmissionRuntimeTracker{}
	for _, packet := range packets {
		if len(packet) < brokerPacketHeaderSize {
			continue
		}
		header, err := parseBrokerPacketHeader(packet[:brokerPacketHeaderSize])
		if err != nil || int(header.PayloadSize) != len(packet)-brokerPacketHeaderSize {
			continue
		}
		observeH26xNALs(
			packet[brokerPacketHeaderSize:],
			brokerLiveCodec(codec),
			&tracker.LiveSeenVPS,
			&tracker.LiveSeenSPS,
			&tracker.LiveSeenPPS,
			&tracker.LiveSeenKeyframe,
		)
	}
	return liveCodecBootstrapReady(codec, &tracker)
}

func (rt *videoBrokerRuntime) requestMainBootstrapRecoveryLocked(now time.Time) {
	// Post-cutover verification needs a complete decoder bootstrap before an
	// external viewer can attach. Its bounded routing demand must be able to
	// request headers plus IDR from an already-warmed hardware encoder too.
	if !rt.childControlSupported || (len(rt.mainClients) == 0 && !rt.routingLiveDemand) {
		return
	}
	if !rt.mainBootstrapRecoveryAt.IsZero() &&
		now.Sub(rt.mainBootstrapRecoveryAt) < brokerMainBootstrapRecoveryEvery {
		return
	}
	rt.mainBootstrapRecoveryAt = now
	rt.mainKeyframeRequestToken++
	if rt.mainKeyframeRequestToken == 0 {
		rt.mainKeyframeRequestToken++
	}
	rt.queueBridgeControlLocked("live_bootstrap_missing_codec_configuration")
	log.Printf(
		"[video-broker] event=live_bootstrap_recovery_requested codec=%s request_keyframe_token=%d encoder_epoch=%d action=hardware_headers_plus_idr epoch_change=false",
		brokerLiveCodec(rt.plan.MainFormat),
		rt.mainKeyframeRequestToken,
		rt.observedLiveEpoch,
	)
}

func brokerPacketListBytes(packets [][]byte) int {
	total := 0
	for _, packet := range packets {
		total += len(packet)
	}
	return total
}

func (rt *videoBrokerRuntime) clearMainBootstrapLocked() {
	rt.mainBootstrap = nil
	rt.mainCodecConfig = nil
	rt.mainBootstrapBytes = 0
	rt.mainAwaitingKeyframe = true
	rt.mainBootstrapRecoveryAt = time.Time{}
}

// A persistent live encoder keeps its codec instance, geometry and profile
// across demand epochs. Preserve the last attested parameter sets while
// discarding the old GOP so the first IDR after activation can be decoded even
// when the vendor VPU does not repeat VPS/SPS/PPS on that IDR. Runtime FPS and
// bitrate changes do not create an epoch and therefore never arm this gate.
func (rt *videoBrokerRuntime) armMainKeyframeGateLocked() {
	rt.mainBootstrap = nil
	rt.mainBootstrapBytes = 0
	rt.mainAwaitingKeyframe = true
	rt.mainBootstrapRecoveryAt = time.Time{}
}

func (rt *videoBrokerRuntime) clearMainBootstrapWithoutKeyframeGateLocked() {
	rt.mainBootstrap = nil
	rt.mainCodecConfig = nil
	rt.mainBootstrapBytes = 0
	rt.mainAwaitingEncoderEpoch = false
	rt.mainAwaitingKeyframe = false
	rt.mainBootstrapRecoveryAt = time.Time{}
}

func cloneBrokerClients(src map[int]*brokerClient) map[int]*brokerClient {
	if len(src) == 0 {
		return nil
	}
	dst := make(map[int]*brokerClient, len(src))
	for id, client := range src {
		dst[id] = client
	}
	return dst
}

func (rt *videoBrokerRuntime) recordBrokerFrameLocked(streamID byte, payloadSize int, clientCount int, now time.Time) {
	switch streamID {
	case brokerStreamH264:
		rt.metrics.mainFrames++
		rt.totalMainFrames++
		rt.metrics.mainBytes += uint64(payloadSize)
		if !rt.lastMainFrameAt.IsZero() {
			if gap := now.Sub(rt.lastMainFrameAt); gap > rt.metrics.mainFrameGapMax {
				rt.metrics.mainFrameGapMax = gap
			}
		}
		rt.lastMainFrameAt = now
		if clientCount == 0 {
			rt.metrics.mainNoClientFrames++
		}
	case brokerStreamNV12:
		rt.metrics.subFrames++
		rt.totalSubFrames++
		rt.metrics.subBytes += uint64(payloadSize)
		if !rt.lastSubFrameAt.IsZero() {
			if gap := now.Sub(rt.lastSubFrameAt); gap > rt.metrics.subFrameGapMax {
				rt.metrics.subFrameGapMax = gap
			}
		}
		rt.lastSubFrameAt = now
		if clientCount == 0 {
			rt.metrics.subNoClientFrames++
		}
	case brokerStreamH265, brokerMuxStreamArchiveH264Native, brokerMuxStreamArchiveH264Generated:
		rt.metrics.archiveFrames++
		rt.totalArchiveFrames++
		rt.metrics.archiveBytes += uint64(payloadSize)
		if !rt.metrics.lastArchiveFrameAt.IsZero() {
			if gap := now.Sub(rt.metrics.lastArchiveFrameAt); gap > rt.metrics.archiveFrameGapMax {
				rt.metrics.archiveFrameGapMax = gap
			}
		}
		rt.metrics.lastArchiveFrameAt = now
		rt.lastArchiveFrameAt = now
		if clientCount == 0 {
			rt.metrics.archiveNoClientFrames++
		}
	}
}

func (rt *videoBrokerRuntime) recordBrokerFanoutLocked(streamID byte, dropped int, maxQueue int) {
	if dropped < 0 {
		dropped = 0
	}
	switch streamID {
	case brokerStreamH264:
		rt.metrics.mainClientDrops += uint64(dropped)
		rt.totalMainDrops += uint64(dropped)
		if maxQueue > rt.metrics.mainMaxQueue {
			rt.metrics.mainMaxQueue = maxQueue
		}
	case brokerStreamNV12:
		rt.metrics.subClientDrops += uint64(dropped)
		rt.totalSubDrops += uint64(dropped)
		if maxQueue > rt.metrics.subMaxQueue {
			rt.metrics.subMaxQueue = maxQueue
		}
	case brokerStreamH265, brokerMuxStreamArchiveH264Native, brokerMuxStreamArchiveH264Generated:
		if maxQueue > rt.metrics.archiveMaxQueue {
			rt.metrics.archiveMaxQueue = maxQueue
		}
	}
}

func (rt *videoBrokerRuntime) maybeLogBrokerMetricsLocked(now time.Time) {
	if rt.metrics.lastLog.IsZero() {
		rt.metrics.lastLog = now
		return
	}
	elapsed := now.Sub(rt.metrics.lastLog)
	control := rt.bridgeControlTargetsLocked()
	logEvery := brokerMetricsLogEvery
	if control.MainActive {
		logEvery = brokerLiveMetricsLogEvery
	}
	if elapsed < logEvery {
		return
	}
	seconds := elapsed.Seconds()
	if seconds <= 0 {
		seconds = 1
	}
	mainTargetBitrateKbps := control.MainBitrateKbps
	if !rt.childControlSupported {
		mainTargetBitrateKbps = positiveOr(rt.mainBitrateKbps, rt.plan.LiveBitrateKbps)
	}
	// Refresh only at log cadence; no external status poll is required.
	health := rt.healthSnapshotLocked(now)
	log.Printf(
		"[video-broker] event=video_pipeline_summary incident_id=%s media_epoch=%d child_pid=%d admitted=%t health_state=%s cadence_degraded=%t elapsed_sec=%.1f capture_codec=%s capture=%dx%d capture_fps=%d archive_codec=%s archive_delivery=%s archive_target_fps=%d archive_target_mbps=%.3f live_active=%t live_control_mode=%s main_target_mbps=%.3f main_fps=%.2f main_mbps=%.2f archive_fps=%.2f archive_mbps=%.2f sub_fps=%.2f sub_mbps=%.2f main_gap_max_ms=%d archive_gap_max_ms=%d sub_gap_max_ms=%d main_clients=%d archive_clients=%d sub_clients=%d main_drops=%d archive_disconnects=%d sub_client_drops=%d sub_ingress_drops=%d main_no_client=%d archive_no_client=%d sub_no_client=%d main_max_queue=%d archive_max_queue=%d sub_max_queue=%d sub_shm_writes=%d sub_shm_errors=%d sub_notify_writes=%d sub_notify_errors=%d",
		captureIncidentID(rt.mediaEpoch),
		rt.mediaEpoch,
		brokerChildPID(rt.childCmd),
		rt.cameraAdmitted,
		fallbackBrokerTelemetry(health.State),
		rt.cadenceDegraded,
		seconds,
		fallbackBrokerTelemetry(rt.observedCaptureCodec),
		rt.observedCaptureWidth,
		rt.observedCaptureHeight,
		rt.observedCaptureFPS,
		fallbackBrokerTelemetry(rt.observedArchiveCodec),
		fallbackBrokerTelemetry(rt.observedArchiveDelivery),
		rt.observedArchiveFPS,
		float64(positiveOr(rt.archiveBitrateKbps, rt.plan.ArchiveBitrateKbps))/1000.0,
		control.MainActive,
		fallbackBrokerTelemetry(rt.childLiveControlMode),
		float64(mainTargetBitrateKbps)/1000.0,
		float64(rt.metrics.mainFrames)/seconds,
		(float64(rt.metrics.mainBytes)*8.0)/seconds/1_000_000.0,
		float64(rt.metrics.archiveFrames)/seconds,
		(float64(rt.metrics.archiveBytes)*8.0)/seconds/1_000_000.0,
		float64(rt.metrics.subFrames)/seconds,
		(float64(rt.metrics.subBytes)*8.0)/seconds/1_000_000.0,
		rt.metrics.mainFrameGapMax.Milliseconds(),
		rt.metrics.archiveFrameGapMax.Milliseconds(),
		rt.metrics.subFrameGapMax.Milliseconds(),
		len(rt.mainClients),
		len(rt.archiveClients),
		len(rt.subClients),
		rt.metrics.mainClientDrops,
		rt.metrics.archiveClientDisconnects,
		rt.metrics.subClientDrops,
		rt.metrics.subIngressDrops,
		rt.metrics.mainNoClientFrames,
		rt.metrics.archiveNoClientFrames,
		rt.metrics.subNoClientFrames,
		rt.metrics.mainMaxQueue,
		rt.metrics.archiveMaxQueue,
		rt.metrics.subMaxQueue,
		rt.metrics.subShmWrites,
		rt.metrics.subShmWriteErrors,
		rt.metrics.subNotifyWrites,
		rt.metrics.subNotifyErrors,
	)
	rt.metrics = videoBrokerMetrics{lastLog: now}
}

func (rt *videoBrokerRuntime) buildBrokerPacketLocked(streamID byte, payload []byte) []byte {
	header, ok := rt.buildBrokerHeaderLocked(streamID, payload)
	if !ok {
		return nil
	}
	return marshalBrokerPacket(header, payload)
}

func (rt *videoBrokerRuntime) buildBrokerHeaderLocked(streamID byte, payload []byte) (brokerPacketHeader, bool) {
	if len(payload) == 0 {
		return brokerPacketHeader{}, false
	}
	header := brokerPacketHeader{
		StreamID:    streamID,
		TimestampNS: brokerTimestampNow(),
		PayloadSize: uint32(len(payload)),
	}
	switch streamID {
	case brokerStreamH264:
		rt.nextMainFrame++
		liveCodecID := brokerLiveCodecStreamID(rt.plan.MainFormat)
		header.StreamID = liveCodecID
		header.FrameID = rt.nextMainFrame
		header.Width = uint32(positiveOr(rt.activeMainWidth, rt.plan.MainWidth))
		header.Height = uint32(positiveOr(rt.activeMainHeight, rt.plan.MainHeight))
		header.Flags = brokerH26xFlags(liveCodecID, payload)
	case brokerStreamH265:
		rt.nextArchiveFrame++
		header.StreamID = brokerStreamH265
		header.FrameID = rt.nextArchiveFrame
		archiveWidth, archiveHeight := rt.activeArchiveGeometryLocked()
		header.Width = uint32(archiveWidth)
		header.Height = uint32(archiveHeight)
		header.Flags = brokerH26xFlags(brokerStreamH265, payload)
	case brokerMuxStreamArchiveH264Native:
		rt.nextArchiveFrame++
		header.StreamID = brokerStreamH264
		header.FrameID = rt.nextArchiveFrame
		header.Width = uint32(positiveOr(rt.activeCaptureWidth, rt.plan.CaptureWidth))
		header.Height = uint32(positiveOr(rt.activeCaptureHeight, rt.plan.CaptureHeight))
		header.Flags = brokerH26xFlags(brokerStreamH264, payload)
	case brokerMuxStreamArchiveH264Generated:
		rt.nextArchiveFrame++
		header.StreamID = brokerStreamH264
		header.FrameID = rt.nextArchiveFrame
		archiveWidth, archiveHeight := rt.activeArchiveGeometryLocked()
		header.Width = uint32(archiveWidth)
		header.Height = uint32(archiveHeight)
		header.Flags = brokerH26xFlags(brokerStreamH264, payload)
	case brokerStreamNV12:
		rt.nextSubFrame++
		header.FrameID = rt.nextSubFrame
		header.Width = uint32(rt.plan.SubWidth)
		header.Height = uint32(rt.plan.SubHeight)
	default:
		return brokerPacketHeader{}, false
	}
	return header, true
}

func brokerLiveCodec(codec string) string {
	switch strings.ToLower(strings.TrimSpace(codec)) {
	case "h264":
		return "h264"
	default:
		return "h265"
	}
}

func brokerLiveCodecStreamID(codec string) byte {
	if brokerLiveCodec(codec) == "h264" {
		return brokerStreamH264
	}
	return brokerStreamH265
}

func (rt *videoBrokerRuntime) activeArchiveGeometryLocked() (int, int) {
	captureWidth := positiveOr(rt.activeCaptureWidth, rt.plan.CaptureWidth)
	captureHeight := positiveOr(rt.activeCaptureHeight, rt.plan.CaptureHeight)
	if strings.EqualFold(strings.TrimSpace(rt.observedArchiveDelivery), "native_passthrough") {
		return captureWidth, captureHeight
	}
	if rt.activeArchiveWidth > 0 && rt.activeArchiveHeight > 0 {
		return rt.activeArchiveWidth, rt.activeArchiveHeight
	}
	return videoproviders.FitArchiveGeometryWithAlignment(
		positiveOr(rt.plan.ArchiveWidth, captureWidth),
		positiveOr(rt.plan.ArchiveHeight, captureHeight),
		captureWidth,
		captureHeight,
		rt.plan.ArchiveWidthAlignment,
		rt.plan.ArchiveHeightAlignment,
	)
}

func isBrokerArchiveMuxStream(streamID byte) bool {
	switch streamID {
	case brokerStreamH265, brokerMuxStreamArchiveH264Native, brokerMuxStreamArchiveH264Generated:
		return true
	default:
		return false
	}
}

func writeBrokerPacket(conn net.Conn, packet []byte) error {
	for written := 0; written < len(packet); {
		n, err := conn.Write(packet[written:])
		if err != nil {
			return err
		}
		if n <= 0 {
			return io.ErrShortWrite
		}
		written += n
	}
	return nil
}

func newBrokerClient(streamID byte, conn net.Conn, mainWriteTimeout time.Duration, archiveFPS int) *brokerClient {
	queueDepth := brokerClientQueueDepth(streamID, archiveFPS)
	if mainWriteTimeout <= 0 {
		mainWriteTimeout = brokerDefaultMainWriteTimeout
	}
	if streamID == brokerEndpointArchive {
		mainWriteTimeout = brokerArchiveWriteTimeout
	}
	return &brokerClient{
		conn:            conn,
		streamID:        streamID,
		send:            make(chan []byte, queueDepth),
		writeTimeout:    mainWriteTimeout,
		dropAllowed:     streamID != brokerEndpointArchive,
		queueBytesLimit: brokerClientQueueByteLimit(streamID),
		closed:          make(chan struct{}),
	}
}

func brokerClientQueueDepth(streamID byte, archiveFPS int) int {
	switch streamID {
	case brokerStreamNV12:
		return brokerSubClientQueueDepth
	case brokerEndpointArchive:
		if archiveFPS <= 0 {
			archiveFPS = 30
		}
		depth := archiveFPS * brokerArchiveQueueSeconds
		if depth < brokerArchiveQueueMinDepth {
			return brokerArchiveQueueMinDepth
		}
		if depth > brokerArchiveQueueMaxDepth {
			return brokerArchiveQueueMaxDepth
		}
		return depth
	default:
		return brokerMainClientQueueDepth
	}
}

func brokerClientQueueByteLimit(streamID byte) int64 {
	if streamID == brokerEndpointArchive {
		return brokerArchiveQueueMaxBytes
	}
	return 0
}

func (rt *videoBrokerRuntime) clearArchiveBootstrapLocked() {
	rt.archiveBootstrap = nil
	rt.archiveCodecConfig = nil
	rt.archiveBootstrapBytes = 0
	rt.archiveCodecStreamID = 0
}

func (c *brokerClient) enqueue(payload []byte) (bool, int, int) {
	// Packets are immutable after dispatch; sharing avoids one full frame copy
	// per client while preserving drop-latest queue semantics.
	dropped := 0
	for {
		if !c.reserveQueueBytes(len(payload)) {
			c.fail("byte_capacity")
			return false, dropped, len(c.send)
		}
		select {
		case <-c.closed:
			c.releaseQueueBytes(len(payload))
			return false, dropped, len(c.send)
		case c.send <- payload:
			atomic.AddUint64(&c.enqueued, 1)
			if dropped > 0 {
				atomic.AddUint64(&c.dropped, uint64(dropped))
			}
			return true, dropped, len(c.send)
		default:
			c.releaseQueueBytes(len(payload))
			if !c.dropAllowed {
				c.fail("frame_capacity")
				return false, dropped, len(c.send)
			}
			select {
			case <-c.closed:
				return false, dropped, len(c.send)
			case stale := <-c.send:
				c.releaseQueueBytes(len(stale))
				dropped++
			default:
				return false, dropped, len(c.send)
			}
		}
	}
}

func (c *brokerClient) reserveQueueBytes(size int) bool {
	if size < 0 {
		return false
	}
	limit := c.queueBytesLimit
	if limit <= 0 {
		return true
	}
	add := int64(size)
	for {
		current := atomic.LoadInt64(&c.queuedBytes)
		if add > limit-current {
			return false
		}
		if atomic.CompareAndSwapInt64(&c.queuedBytes, current, current+add) {
			return true
		}
	}
}

func (c *brokerClient) releaseQueueBytes(size int) {
	if c.queueBytesLimit <= 0 || size <= 0 {
		return
	}
	atomic.AddInt64(&c.queuedBytes, -int64(size))
}

func (c *brokerClient) writeLoop() {
	c.writeLoopWithBootstrap(nil)
}

func (c *brokerClient) writeLoopWithBootstrap(bootstrap [][]byte) {
	// A newly published encoder may not have a retained bootstrap when its
	// first consumer attaches. The publication boundary belongs to the first
	// packet actually delivered, regardless of whether it came from the cache
	// or the live queue. Copy only that packet; fan-out buffers stay immutable.
	resetPending := c.streamID != brokerStreamNV12
	write := func(payload []byte) bool {
		if resetPending {
			payload = brokerPacketWithAdditionalFlags(payload, brokerFlagStreamReset)
			if len(payload) == 0 {
				c.fail("invalid_bootstrap")
				c.close()
				return false
			}
		}
		if err := c.writePayload(payload); err != nil {
			c.fail(brokerWriteFailureReason(err))
			c.close()
			return false
		}
		resetPending = false
		atomic.AddUint64(&c.written, 1)
		return true
	}
	for _, payload := range bootstrap {
		select {
		case <-c.closed:
			return
		default:
		}
		if !write(payload) {
			return
		}
	}
	for {
		select {
		case <-c.closed:
			return
		case payload, ok := <-c.send:
			if !ok {
				return
			}
			c.releaseQueueBytes(len(payload))
			if !write(payload) {
				return
			}
		}
	}
}

func (c *brokerClient) writePayload(payload []byte) error {
	timeout := c.writeTimeout
	if c.streamID == brokerStreamNV12 {
		timeout = brokerSubWriteTimeout
	}
	if timeout <= 0 {
		timeout = brokerDefaultMainWriteTimeout
	}
	_ = c.conn.SetWriteDeadline(time.Now().Add(timeout))
	err := writeBrokerPacket(c.conn, payload)
	_ = c.conn.SetWriteDeadline(time.Time{})
	return err
}

func isBrokerWriteTimeout(err error) bool {
	if err == nil {
		return false
	}
	if netErr, ok := err.(net.Error); ok {
		return netErr.Timeout()
	}
	return errors.Is(err, os.ErrDeadlineExceeded)
}

func brokerWriteFailureReason(err error) string {
	if isBrokerWriteTimeout(err) {
		return "write_timeout"
	}
	if err != nil {
		return "write_error"
	}
	return ""
}

func (c *brokerClient) fail(reason string) {
	reason = strings.TrimSpace(reason)
	if reason == "" {
		return
	}
	c.failureMu.Lock()
	if c.failureReason == "" {
		c.failureReason = reason
	}
	c.failureMu.Unlock()
}

func (c *brokerClient) failure() string {
	c.failureMu.Lock()
	defer c.failureMu.Unlock()
	return c.failureReason
}

func (c *brokerClient) close() {
	c.closeOnce.Do(func() {
		close(c.closed)
		_ = c.conn.Close()
	})
}

func (rt *videoBrokerRuntime) finishChild(ctx context.Context) {
	rt.mu.Lock()
	cmd := rt.childCmd
	stderr := rt.childStderr
	stderrDone := rt.childStderrDone
	exitDone := rt.childExitDone
	childCancel := rt.childCancel
	childFrames := rt.childFrames
	childFramesDone := rt.childFramesDone
	mediaEpoch := rt.mediaEpoch
	childPID := brokerChildPID(cmd)
	if cmd != nil {
		if rt.childFinishing {
			rt.mu.Unlock()
			return
		}
		rt.childFinishing = true
		rt.mu.Unlock()
	}
	var waitErr error
	if cmd != nil {
		waitErr = cmd.Wait()
		if exitDone != nil {
			close(exitDone)
		}
		if stderrDone != nil {
			select {
			case <-stderrDone:
			case <-time.After(time.Second):
				log.Printf(
					"[video-broker] event=capture_stderr_drain_timeout incident_id=%s media_epoch=%d child_pid=%d timeout_ms=1000",
					captureIncidentID(mediaEpoch), mediaEpoch, childPID,
				)
			}
		}
		// Broker-owned read ends are deliberately not closed by Cmd.Wait.
		// Release even after a bounded drain timeout (for example a descendant
		// retaining stderr); that also wakes a scanner blocked in Read.
		if stderr != nil {
			_ = stderr.Close()
		}
		if childCancel != nil {
			childCancel()
		}
		// The process has been reaped. Join frame operations before releasing the
		// native channel, source arena or generation; no second cmd.Wait exists.
		framesJoined := childFramesDone == nil
		if childFramesDone != nil {
			select {
			case <-childFramesDone:
				framesJoined = true
			case <-time.After(5 * time.Second):
				log.Printf("[video-broker] frame_source stage=join_pending media_epoch=%d child_pid=%d timeout_ms=5000 action=hold_admission", mediaEpoch, childPID)
			}
		}
		var frameCleanupErr error
		if !framesJoined {
			frameCleanupErr = fmt.Errorf("frame receive still running after process exit: %w", context.DeadlineExceeded)
		} else if childFrames != nil {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Second)
			frameCleanupErr = childFrames.Retire(cleanupCtx)
			cancel()
		}
		rt.mu.Lock()
		if frameCleanupErr != nil {
			rt.frameCleanupErr = frameCleanupErr
			if errors.Is(frameCleanupErr, context.DeadlineExceeded) || errors.Is(frameCleanupErr, context.Canceled) {
				rt.pendingFrameCleanup = childFrames
				if !framesJoined {
					rt.pendingFrameJoin = childFramesDone
				}
			}
			persistFrameCleanupGuard(rt.plan.StateDirectory, captureIncidentID(mediaEpoch), frameCleanupErr)
			log.Printf("[video-broker] frame_source stage=cleanup_unconfirmed media_epoch=%d child_pid=%d err=%v action=hold_admission", mediaEpoch, childPID, frameCleanupErr)
		}
		if rt.childCmd != cmd {
			rt.childFinishing = false
			rt.mu.Unlock()
			return
		}
	}
	defer rt.mu.Unlock()
	startedAt := rt.childStartedAt
	admitted := rt.cameraAdmitted
	captureCodec := rt.observedCaptureCodec
	captureWidth := rt.observedCaptureWidth
	captureHeight := rt.observedCaptureHeight
	captureFPS := rt.observedCaptureFPS
	archiveCodec := rt.observedArchiveCodec
	archiveFPS := rt.observedArchiveFPS
	mainFrames := counterDelta(rt.totalMainFrames, rt.childStartMainFrames)
	archiveFrames := counterDelta(rt.totalArchiveFrames, rt.childStartArchiveFrames)
	subFrames := counterDelta(rt.totalSubFrames, rt.childStartSubFrames)
	lastFrameAt := rt.lastFrameAt
	lastArchiveFrameAt := rt.lastArchiveFrameAt
	lastSubFrameAt := rt.lastSubFrameAt
	if rt.cameraAdmission.DevicePath != "" {
		rt.writeCameraAdmissionStateLocked("recovering", "broker_child_exited")
	}
	now := time.Now()
	lastFrameAgeMS := timestampAgeMilliseconds(now, lastFrameAt)
	lastArchiveAgeMS := timestampAgeMilliseconds(now, lastArchiveFrameAt)
	lastSubAgeMS := timestampAgeMilliseconds(now, lastSubFrameAt)
	failureReason, failureAt := rt.childFailure(mediaEpoch)
	outcome := "clean_exit"
	if ctx.Err() != nil {
		outcome = "shutdown"
		if failureReason == "" {
			failureReason = "lifecycle_shutdown"
			failureAt = now
		}
	} else if failureReason != "" || waitErr != nil {
		outcome = "failure"
		if failureReason == "" {
			failureReason = "child_exit_without_terminal_reason"
			rt.recordChildFailure(mediaEpoch, childPID, failureReason)
			failureAt = time.Now()
		}
	}
	exitCode, exitSignal := brokerProcessExit(cmd)
	durationMS := int64(0)
	if !startedAt.IsZero() {
		durationMS = now.Sub(startedAt).Milliseconds()
		if durationMS < 0 {
			durationMS = 0
		}
	}
	failureAgeMS := timestampAgeMilliseconds(now, failureAt)
	rt.reanchorUVCQuietAfterChildLocked(now, mediaEpoch, failureReason)
	if cmd != nil {
		log.Printf(
			"[video-broker] event=capture_epoch_ended outcome=%s incident_id=%s media_epoch=%d child_pid=%d duration_ms=%d admitted=%t reason=%s failure_age_ms=%d exit_code=%d signal=%s wait_error=%q capture_codec=%s capture=%dx%d@%d archive_codec=%s archive_fps=%d frames_main=%d frames_archive=%d frames_sub=%d last_frame_age_ms=%d last_archive_age_ms=%d last_sub_age_ms=%d",
			outcome,
			captureIncidentID(mediaEpoch),
			mediaEpoch,
			childPID,
			durationMS,
			admitted,
			fallbackBrokerTelemetry(failureReason),
			failureAgeMS,
			exitCode,
			fallbackBrokerTelemetry(exitSignal),
			waitErrorString(waitErr),
			fallbackBrokerTelemetry(captureCodec),
			captureWidth,
			captureHeight,
			captureFPS,
			fallbackBrokerTelemetry(archiveCodec),
			archiveFPS,
			mainFrames,
			archiveFrames,
			subFrames,
			lastFrameAgeMS,
			lastArchiveAgeMS,
			lastSubAgeMS,
		)
	}
	rt.abortHardwareComponentTrialsForChildLocked(mediaEpoch, childPID)
	rt.noteUnadmittedChildExitLocked(admitted, ctx.Err() != nil)
	rt.childCmd = nil
	rt.childOut = nil
	rt.childStderr = nil
	rt.childStderrDone = nil
	rt.childExitDone = nil
	rt.childIn = nil
	rt.childControl = nil
	rt.childFanoutControl = nil
	rt.childFanoutSupported = false
	rt.childFanoutControlMode = ""
	rt.failWorkerFanoutWaitersLocked(errors.New("capture worker exited before fan-out receipt"))
	rt.workerFanoutWaiters = nil
	rt.childControlAvailable = false
	rt.childControlSupported = false
	rt.childLiveControlMode = ""
	rt.childComponentRetryScopes = nil
	rt.observedCaptureCodec = ""
	rt.observedCaptureWidth = 0
	rt.observedCaptureHeight = 0
	rt.observedCaptureFPS = 0
	rt.observedArchiveCodec = ""
	rt.observedArchiveDelivery = ""
	rt.observedArchiveFPS = 0
	rt.observedDecodeCodec = ""
	rt.observedDecodeRequested = ""
	rt.observedDecodeBackend = ""
	rt.observedDecodeFallback = ""
	rt.observedResizeBackend = ""
	rt.observedRoleResizeBackend = nil
	rt.observedLiveEpoch = 0
	rt.toneMappingSupported = false
	rt.toneMappingBackend = ""
	rt.observedToneMode = ""
	rt.observedToneAccelerator = ""
	rt.observedToneActive = false
	rt.observedToneGainMin = 0
	rt.observedToneGainMax = 0
	rt.observedToneUpdates = 0
	rt.activeCaptureWidth = 0
	rt.activeCaptureHeight = 0
	rt.activeCaptureFPS = 0
	rt.activeMainWidth = 0
	rt.activeMainHeight = 0
	rt.activeArchiveWidth = 0
	rt.activeArchiveHeight = 0
	rt.cameraAdmissionTracker = cameraAdmissionRuntimeTracker{}
	rt.cameraAdmitted = false
	rt.childCancel = nil
	rt.childFrames = nil
	rt.childFramesDone = nil
	rt.causalObserver = nil
	rt.causalSubFrame, rt.causalSubSourceEpoch = 0, 0
	rt.childRunning = false
	rt.resetBridgeControlReadyLocked()
	rt.childFinishing = false
	rt.mainBootstrap = nil
	rt.mainCodecConfig = nil
	rt.mainBootstrapBytes = 0
	rt.mainAwaitingEncoderEpoch = false
	rt.mainAwaitingKeyframe = false
	rt.clearArchiveBootstrapLocked()
	rt.nextMainFrame = 0
	rt.nextSubFrame = 0
	rt.childStartedAt = time.Time{}
	rt.childStartMainFrames = 0
	rt.childStartArchiveFrames = 0
	rt.childStartSubFrames = 0
	rt.lastFrameAt = time.Time{}
	rt.lastMainFrameAt = time.Time{}
	rt.lastSubFrameAt = time.Time{}
	rt.lastArchiveFrameAt = time.Time{}
	rt.resetCadenceHealthLocked()
	rt.resetChildFailure(0)
	if ctx.Err() == nil && !rt.childRecovery {
		rt.scheduleChildRecoveryLocked(ctx)
	}
}

func counterDelta(current, baseline uint64) uint64 {
	if current < baseline {
		return current
	}
	return current - baseline
}

func timestampAgeMilliseconds(now, observed time.Time) int64 {
	if now.IsZero() || observed.IsZero() || now.Before(observed) {
		return -1
	}
	return now.Sub(observed).Milliseconds()
}

func brokerProcessExit(cmd *exec.Cmd) (int, string) {
	if cmd == nil || cmd.ProcessState == nil {
		return -1, "unknown"
	}
	exitCode := cmd.ProcessState.ExitCode()
	if status, ok := cmd.ProcessState.Sys().(syscall.WaitStatus); ok && status.Signaled() {
		return exitCode, status.Signal().String()
	}
	return exitCode, "none"
}

func waitErrorString(err error) string {
	if err == nil {
		return ""
	}
	return strings.Join(strings.Fields(err.Error()), " ")
}

func (rt *videoBrokerRuntime) recoverChild(ctx context.Context) {
	delay := brokerChildRestartDelay
	rt.mu.Lock()
	if !rt.nextChildStartAt.IsZero() {
		if until := time.Until(rt.nextChildStartAt); until > delay {
			delay = until
		}
	}
	rt.mu.Unlock()
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		rt.mu.Lock()
		rt.childRecovery = false
		rt.mu.Unlock()
		return
	case <-timer.C:
	}
	rt.reconcilePendingFrameCleanup(ctx)

	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.childRecovery = false
	if ctx.Err() != nil || rt.childRunning {
		return
	}
	if err := rt.ensureChildLocked(ctx); err != nil {
		rt.logf(
			"[video-broker] event=capture_recovery_start_failed after_media_epoch=%d err=%v",
			rt.mediaEpoch,
			err,
		)
		rt.scheduleChildRecoveryLocked(ctx)
	}
}

func (rt *videoBrokerRuntime) reconcilePendingFrameCleanup(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	// A timed-out caller did not prove physical cleanup failure. Observe the
	// original retirement operation after its frame reader has joined; never
	// create a second release or clear a genuine SDK receipt failure.
	rt.mu.Lock()
	pending, joined := rt.pendingFrameCleanup, rt.pendingFrameJoin
	rt.mu.Unlock()
	if pending != nil {
		joinReady := joined == nil
		if joined != nil {
			select {
			case <-joined:
				joinReady = true
			default:
			}
		}
		if joinReady {
			cleanupCtx, cancel := context.WithTimeout(ctx, time.Second)
			err := pending.Retire(cleanupCtx)
			cancel()
			rt.mu.Lock()
			if rt.pendingFrameCleanup != nil && !errors.Is(err, context.DeadlineExceeded) &&
				!errors.Is(err, context.Canceled) {
				if err == nil {
					err = clearPendingFrameCleanupGuard(rt.plan.StateDirectory, captureIncidentID(rt.mediaEpoch))
					if err != nil {
						log.Printf("[video-broker] frame_source stage=cleanup_guard_clear_failed media_epoch=%d err=%v action=hold_admission", rt.mediaEpoch, err)
						rt.frameCleanupErr = err
						rt.mu.Unlock()
						return
					}
				}
				rt.pendingFrameCleanup = nil
				rt.pendingFrameJoin = nil
				rt.frameCleanupErr = err
				if err == nil {
					log.Printf("[video-broker] frame_source stage=cleanup_confirmed media_epoch=%d action=resume_admission", rt.mediaEpoch)
				} else {
					persistFrameCleanupGuard(rt.plan.StateDirectory, captureIncidentID(rt.mediaEpoch), err)
					log.Printf("[video-broker] frame_source stage=cleanup_failed media_epoch=%d err=%v action=hold_admission", rt.mediaEpoch, err)
				}
			}
			rt.mu.Unlock()
		}
	}
}

func (rt *videoBrokerRuntime) scheduleChildRecoveryLocked(ctx context.Context) {
	if ctx.Err() != nil || rt.childRecovery {
		return
	}
	rt.childRecovery = true
	retryIn := brokerChildRestartDelay
	if !rt.nextChildStartAt.IsZero() {
		if until := time.Until(rt.nextChildStartAt); until > retryIn {
			retryIn = until
		}
	}
	if retryIn < 0 {
		retryIn = 0
	}
	log.Printf(
		"[video-broker] event=capture_recovery_scheduled incident_id=%s after_media_epoch=%d retry_in_ms=%d backoff_ms=%d",
		captureIncidentID(rt.mediaEpoch),
		rt.mediaEpoch,
		retryIn.Milliseconds(),
		rt.childStartBackoff.Milliseconds(),
	)
	rt.startGoroutine(func() { rt.recoverChild(ctx) })
}

func (rt *videoBrokerRuntime) noteChildStartFailureLocked() {
	if rt.childStartBackoff <= 0 {
		rt.childStartBackoff = brokerChildRestartDelay
	} else {
		rt.childStartBackoff *= 2
		if rt.childStartBackoff > brokerChildRestartMaxDelay {
			rt.childStartBackoff = brokerChildRestartMaxDelay
		}
	}
	retryAt := time.Now().Add(rt.childStartBackoff)
	// A component-specific recovery boundary (for example the next permitted
	// USB reset) may have scheduled a later retry. Generic process backoff must
	// never shorten that boundary and recreate the failure loop it protects.
	if rt.nextChildStartAt.Before(retryAt) {
		rt.nextChildStartAt = retryAt
	}
}

func (rt *videoBrokerRuntime) noteUnadmittedChildExitLocked(admitted, shuttingDown bool) {
	if !admitted && !shuttingDown {
		rt.noteChildStartFailureLocked()
	}
}

func (rt *videoBrokerRuntime) resetChildStartFailureLocked() {
	rt.childStartBackoff = 0
	rt.nextChildStartAt = time.Time{}
}

func (rt *videoBrokerRuntime) logf(format string, args ...interface{}) {
	if rt != nil && rt.logLimiter != nil {
		rt.logLimiter.Printf(format, args...)
		return
	}
	log.Printf(format, args...)
}

type coalescedLogLimiter struct {
	mu         sync.Mutex
	interval   time.Duration
	entries    map[string]*coalescedLogEntry
	now        func() time.Time
	maxEntries int
	ttl        time.Duration
}

type coalescedLogEntry struct {
	last       time.Time
	suppressed uint64
}

func newCoalescedLogLimiter(interval time.Duration) *coalescedLogLimiter {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	return &coalescedLogLimiter{
		interval:   interval,
		entries:    make(map[string]*coalescedLogEntry),
		now:        time.Now,
		maxEntries: brokerMaxCoalescedLogEntries,
		ttl:        4 * interval,
	}
}

func (l *coalescedLogLimiter) Printf(format string, args ...interface{}) {
	if l == nil {
		log.Printf(format, args...)
		return
	}
	message := fmt.Sprintf(format, args...)
	suppressed, emit := l.record(normalizeCoalescedLogKey(message))
	if !emit {
		return
	}
	if suppressed > 0 {
		log.Printf("%s suppressed_repeats=%d", message, suppressed)
		return
	}
	log.Print(message)
}

func (l *coalescedLogLimiter) record(key string) (uint64, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	if now.IsZero() {
		now = time.Now()
	}
	entry, ok := l.entries[key]
	if !ok {
		l.evictExpiredOrOldest(now)
		l.entries[key] = &coalescedLogEntry{last: now}
		return 0, true
	}
	if now.Sub(entry.last) < l.interval {
		entry.suppressed++
		return 0, false
	}
	suppressed := entry.suppressed
	entry.last = now
	entry.suppressed = 0
	return suppressed, true
}

func (l *coalescedLogLimiter) evictExpiredOrOldest(now time.Time) {
	limit := l.maxEntries
	if limit <= 0 {
		limit = brokerMaxCoalescedLogEntries
	}
	if len(l.entries) < limit {
		return
	}
	oldestKey := ""
	var oldest time.Time
	for key, entry := range l.entries {
		if l.ttl > 0 && now.Sub(entry.last) >= l.ttl {
			delete(l.entries, key)
			continue
		}
		if oldestKey == "" || entry.last.Before(oldest) {
			oldestKey, oldest = key, entry.last
		}
	}
	if len(l.entries) >= limit && oldestKey != "" {
		delete(l.entries, oldestKey)
	}
}

func normalizeCoalescedLogKey(message string) string {
	message = brokerLogRetryAfterRE.ReplaceAllString(message, "retry_after=X")
	return strings.Join(strings.Fields(message), " ")
}

func isBrokerUVCStreamOnFailure(line string) bool {
	lower := strings.ToLower(line)
	return strings.Contains(lower, "vidioc_streamon failed") ||
		strings.Contains(lower, "capture stalled after") ||
		strings.Contains(lower, "[camera-admission]") &&
			strings.Contains(lower, "reason=runtime_attestation_timeout")
}

func resolveVideoBrokerCaptureDevice(plan videoBrokerPlan) string {
	return strings.TrimSpace(plan.CaptureDevice)
}

type missingV4LDeviceError struct {
	device            string
	visibleVideoNodes string
	usbHint           string
}

func (e missingV4LDeviceError) Error() string {
	return fmt.Sprintf(
		"capture device unavailable device=%s reason=no_v4l2_node visible_video_nodes=%s usb_hint=%s",
		e.device,
		e.visibleVideoNodes,
		e.usbHint,
	)
}

func validateBrokerCaptureDeviceReady(transport string, device string) error {
	if !strings.EqualFold(strings.TrimSpace(transport), videoproviders.TransportV4L2UVC) {
		return nil
	}
	device = strings.TrimSpace(device)
	if device == "" {
		return errors.New("capture device is required for v4l2_uvc")
	}
	if _, err := brokerStatPath(device); err != nil {
		if os.IsNotExist(err) {
			return missingV4LDeviceError{
				device:            device,
				visibleVideoNodes: describeVisibleVideoNodes(),
				usbHint:           describeUSBCameraHint(),
			}
		}
		return fmt.Errorf("capture device unavailable device=%s err=%w", device, err)
	}
	return nil
}

func describeVisibleVideoNodes() string {
	nodes, err := brokerGlobPaths("/dev/video*")
	if err != nil || len(nodes) == 0 {
		return "none"
	}
	if len(nodes) > 8 {
		nodes = nodes[:8]
	}
	names := make([]string, 0, len(nodes))
	for _, node := range nodes {
		names = append(names, filepath.Base(node))
	}
	sort.Strings(names)
	return strings.Join(names, ",")
}

func describeUSBCameraHint() string {
	candidate, ok := findUSBCameraCandidate()
	if !ok {
		return "no_usb_camera_candidate"
	}
	return candidate.summary()
}

type usbCameraCandidate struct {
	path      string
	product   string
	vendor    string
	productID string
	hasV4L    bool
}

func (c usbCameraCandidate) summary() string {
	hasV4L := "no"
	if c.hasV4L {
		hasV4L = "yes"
	}
	return fmt.Sprintf("product=%q id=%s:%s v4l=%s", compactLogValue(c.product, 48), valueOrUnknown(c.vendor), valueOrUnknown(c.productID), hasV4L)
}

func findUSBCameraCandidate() (usbCameraCandidate, bool) {
	entries, err := brokerReadDir(brokerUSBDevicesDir)
	if err != nil {
		return usbCameraCandidate{}, false
	}
	for _, entry := range entries {
		root := filepath.Join(brokerUSBDevicesDir, entry.Name())
		product := strings.TrimSpace(readBrokerText(filepath.Join(root, "product")))
		if product == "" || !looksLikeCameraProduct(product) {
			continue
		}
		return usbCameraCandidate{
			path:      root,
			product:   product,
			vendor:    strings.TrimSpace(readBrokerText(filepath.Join(root, "idVendor"))),
			productID: strings.TrimSpace(readBrokerText(filepath.Join(root, "idProduct"))),
			hasV4L:    usbPathHasVideo4Linux(root, 4),
		}, true
	}
	return usbCameraCandidate{}, false
}

func readBrokerText(path string) string {
	data, err := brokerReadFile(path)
	if err != nil {
		return ""
	}
	return string(data)
}

func looksLikeCameraProduct(product string) bool {
	lower := strings.ToLower(product)
	return strings.Contains(lower, "camera") ||
		strings.Contains(lower, "webcam") ||
		strings.Contains(lower, "uvc") ||
		strings.Contains(lower, "video")
}

func usbPathHasVideo4Linux(root string, depth int) bool {
	if depth <= 0 {
		return false
	}
	entries, err := brokerReadDir(root)
	if err != nil {
		return false
	}
	for _, entry := range entries {
		if entry.Name() == "video4linux" {
			return true
		}
		if usbPathHasVideo4Linux(filepath.Join(root, entry.Name()), depth-1) {
			return true
		}
	}
	return false
}

func compactLogValue(value string, max int) string {
	value = strings.Join(strings.Fields(value), " ")
	if max <= 0 || len(value) <= max {
		return value
	}
	if max <= 3 {
		return value[:max]
	}
	return value[:max-3] + "..."
}

func valueOrUnknown(value string) string {
	if strings.TrimSpace(value) == "" {
		return "unknown"
	}
	return strings.TrimSpace(value)
}

func resolveEngineDirectory(configured string) string {
	if directory := strings.TrimSpace(configured); directory != "" {
		return filepath.Clean(directory)
	}
	executable, err := os.Executable()
	if err == nil {
		return filepath.Join(filepath.Dir(executable), "bin")
	}
	return "bin"
}

func deriveBrokerSensorPrefix(configured string) string {
	sensorID := strings.TrimSpace(configured)
	if sensorID == "" {
		return ""
	}
	if idx := strings.Index(sensorID, "_"); idx > 0 {
		sensorID = sensorID[:idx]
	}
	return strings.ToLower(sensorID)
}

func deriveBrokerVideoIndex(configured int) int {
	if configured == 0 || configured == 1 || configured == 2 {
		return configured
	}
	return -1
}
