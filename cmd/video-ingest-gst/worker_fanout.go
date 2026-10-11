package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	videoproviders "github.com/Josepavese/videosluice/internal/source"
)

const gstreamerFanoutFrameTimeout = 8 * time.Second

var gstreamerFanoutIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._:-]{0,127}$`)

type gstreamerCandidateSpec struct {
	cfg      ingestConfig
	elements gstElements
	epoch    uint64
}

type gstreamerFanoutTransaction struct {
	id            string
	stage         string
	changes       []videoproviders.WorkerBranchChange
	originals     map[string]*isolatedGStreamerRole
	specs         map[string]gstreamerCandidateSpec
	candidates    map[string]*isolatedGStreamerRole
	cutoverFrames map[string]uint64
}

type gstreamerFanoutController struct {
	ctx      context.Context
	cfg      ingestConfig
	elements gstElements
	buses    gstreamerBusPaths
	router   *gstreamerFrameRouter

	mu           sync.Mutex
	stable       map[string]*isolatedGStreamerRole
	nextEpoch    map[string]uint64
	lastSequence uint64
	transaction  *gstreamerFanoutTransaction
	fatal        chan error

	startRole func(
		context.Context,
		ingestConfig,
		gstElements,
		gstreamerBusPaths,
		string,
		uint64,
	) (*isolatedGStreamerRole, error)
	startForwarder func(
		context.Context,
		*isolatedGStreamerRole,
		*gstreamerFrameRouter,
		time.Time,
	)
}

func gstreamerFanoutIdentityValid(cfg ingestConfig) bool {
	return gstreamerFanoutIDPattern.MatchString(cfg.sourceID) && cfg.mediaEpoch > 0
}

func newGStreamerFanoutController(
	ctx context.Context,
	cfg ingestConfig,
	elements gstElements,
	buses gstreamerBusPaths,
	router *gstreamerFrameRouter,
	roles []*isolatedGStreamerRole,
) (*gstreamerFanoutController, error) {
	if ctx == nil || !gstreamerFanoutIdentityValid(cfg) || router == nil {
		return nil, errors.New("GStreamer fan-out controller identity is incomplete")
	}
	controller := &gstreamerFanoutController{
		ctx: ctx, cfg: cfg, elements: elements, buses: buses, router: router,
		stable:    make(map[string]*isolatedGStreamerRole),
		nextEpoch: map[string]uint64{"analysis": 1, "archive": 1, "live": 1},
		fatal:     make(chan error, 1), startRole: startIsolatedGStreamerRole,
		startForwarder: startGStreamerRoleForwarder,
	}
	for _, role := range roles {
		if role == nil || controller.stable[role.role] != nil || role.branchEpoch == 0 {
			return nil, errors.New("GStreamer fan-out initial roles are invalid")
		}
		controller.stable[role.role] = role
		controller.nextEpoch[role.role] = role.branchEpoch
		controller.monitorRole(role)
	}
	return controller, nil
}

func (controller *gstreamerFanoutController) monitorRole(role *isolatedGStreamerRole) {
	go func() {
		<-role.forwardDone
		controller.mu.Lock()
		stable := controller.stable[role.role] == role
		candidate := controller.transaction != nil && controller.transaction.candidates[role.role] == role
		controller.mu.Unlock()
		if !stable || candidate || controller.ctx.Err() != nil {
			return
		}
		err, _ := role.forwardResult()
		select {
		case controller.fatal <- fmt.Errorf("stable GStreamer %s role exited: %w", role.role, err):
		default:
		}
	}()
}

func (controller *gstreamerFanoutController) stopAll() error {
	if controller == nil {
		return nil
	}
	controller.mu.Lock()
	unique := make(map[*isolatedGStreamerRole]struct{})
	for _, role := range controller.stable {
		if role != nil {
			unique[role] = struct{}{}
		}
	}
	if controller.transaction != nil {
		for _, role := range controller.transaction.candidates {
			if role != nil {
				unique[role] = struct{}{}
			}
		}
	}
	controller.transaction = nil
	controller.mu.Unlock()
	var stopErr error
	for role := range unique {
		if err := stopGStreamerRole(role); err != nil {
			stopErr = errors.Join(stopErr, err)
		}
	}
	return stopErr
}

func (controller *gstreamerFanoutController) handle(
	command videoproviders.WorkerFanoutCommand,
) videoproviders.WorkerFanoutReceipt {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	receipt := videoproviders.WorkerFanoutReceipt{
		Schema:        videoproviders.WorkerFanoutReceiptSchema,
		TransactionID: command.TransactionID, Sequence: command.Sequence,
		SourceID: command.SourceID, CapturePID: command.CapturePID,
		MediaEpoch: command.MediaEpoch,
	}
	reject := func(code string) videoproviders.WorkerFanoutReceipt {
		receipt.Event = videoproviders.WorkerFanoutEventRejected
		receipt.ErrorCode = videoproviders.WorkerFanoutSanitizedErrorCode(code)
		receipt.Branches = nil
		return receipt
	}
	if command.SourceID != controller.cfg.sourceID || command.CapturePID != os.Getpid() ||
		command.MediaEpoch != controller.cfg.mediaEpoch {
		return reject("invalid_fence")
	}
	if command.Sequence <= controller.lastSequence {
		return reject("stale_sequence")
	}
	controller.lastSequence = command.Sequence
	if command.Phase == videoproviders.WorkerFanoutPhasePrepare {
		if controller.transaction != nil {
			return reject("transaction_busy")
		}
		transaction, err := controller.prepareLocked(command)
		if err != nil {
			return reject("prepare_failed")
		}
		controller.transaction = transaction
		receipt.Event = videoproviders.WorkerFanoutEventPrepared
		return receipt
	}
	transaction := controller.transaction
	if transaction == nil || transaction.id != command.TransactionID {
		return reject("invalid_transaction")
	}
	var err error
	switch command.Phase {
	case videoproviders.WorkerFanoutPhaseWarmup:
		if transaction.stage != videoproviders.WorkerFanoutEventPrepared {
			return reject("invalid_phase")
		}
		err = controller.warmupLocked(transaction)
		if err == nil {
			transaction.stage = videoproviders.WorkerFanoutEventWarmed
			receipt.Event = videoproviders.WorkerFanoutEventWarmed
		}
	case videoproviders.WorkerFanoutPhaseAdmit:
		if transaction.stage != videoproviders.WorkerFanoutEventWarmed {
			return reject("invalid_phase")
		}
		err = controller.checkCandidatesLocked(transaction)
		if err == nil {
			transaction.stage = videoproviders.WorkerFanoutEventAdmitted
			receipt.Event = videoproviders.WorkerFanoutEventAdmitted
		}
	case videoproviders.WorkerFanoutPhaseFirstFrame:
		if transaction.stage != videoproviders.WorkerFanoutEventAdmitted {
			return reject("invalid_phase")
		}
		receipt.Branches, err = controller.waitCandidateFramesLocked(transaction, nil)
		if err == nil {
			transaction.stage = videoproviders.WorkerFanoutEventFirstFrame
			receipt.Event = videoproviders.WorkerFanoutEventFirstFrame
		}
	case videoproviders.WorkerFanoutPhaseCutover:
		if transaction.stage != videoproviders.WorkerFanoutEventFirstFrame {
			return reject("invalid_phase")
		}
		err = controller.cutoverLocked(transaction)
		if err == nil {
			transaction.stage = videoproviders.WorkerFanoutEventCutover
			receipt.Event = videoproviders.WorkerFanoutEventCutover
		}
	case videoproviders.WorkerFanoutPhaseVerify:
		if transaction.stage != videoproviders.WorkerFanoutEventCutover {
			return reject("invalid_phase")
		}
		_, err = controller.waitCandidateFramesLocked(transaction, transaction.cutoverFrames)
		if err == nil {
			transaction.stage = videoproviders.WorkerFanoutEventVerified
			receipt.Event = videoproviders.WorkerFanoutEventVerified
		}
	case videoproviders.WorkerFanoutPhaseRollback:
		err = controller.rollbackLocked(transaction)
		if err == nil {
			controller.transaction = nil
			receipt.Event = videoproviders.WorkerFanoutEventRolledBack
		}
	case videoproviders.WorkerFanoutPhaseAbort:
		if transaction.stage == videoproviders.WorkerFanoutEventCutover ||
			transaction.stage == videoproviders.WorkerFanoutEventVerified {
			return reject("invalid_phase")
		}
		err = controller.abortLocked(transaction)
		if err == nil {
			controller.transaction = nil
			receipt.Event = videoproviders.WorkerFanoutEventAborted
		}
	case videoproviders.WorkerFanoutPhaseRetire:
		if transaction.stage != videoproviders.WorkerFanoutEventVerified {
			return reject("invalid_phase")
		}
		err = controller.retireLocked(transaction)
		if err == nil {
			controller.transaction = nil
			receipt.Event = videoproviders.WorkerFanoutEventRetired
		}
	default:
		return reject("invalid_phase")
	}
	if err != nil {
		return reject(gstreamerFanoutPhaseErrorCode(command.Phase, err))
	}
	return receipt
}

func (controller *gstreamerFanoutController) prepareLocked(
	command videoproviders.WorkerFanoutCommand,
) (*gstreamerFanoutTransaction, error) {
	transaction := &gstreamerFanoutTransaction{
		id: command.TransactionID, stage: videoproviders.WorkerFanoutEventPrepared,
		changes:       append([]videoproviders.WorkerBranchChange(nil), command.Changes...),
		originals:     make(map[string]*isolatedGStreamerRole),
		specs:         make(map[string]gstreamerCandidateSpec),
		candidates:    make(map[string]*isolatedGStreamerRole),
		cutoverFrames: make(map[string]uint64),
	}
	for _, change := range command.Changes {
		current := controller.stable[change.Role]
		transaction.originals[change.Role] = current
		switch change.Action {
		case videoproviders.WorkerFanoutActionAttach:
			if current != nil {
				return nil, fmt.Errorf("role %s is already attached", change.Role)
			}
		case videoproviders.WorkerFanoutActionDetach, videoproviders.WorkerFanoutActionReplace:
			if current == nil {
				return nil, fmt.Errorf("role %s has no incumbent", change.Role)
			}
		}
		if change.Action == videoproviders.WorkerFanoutActionDetach {
			continue
		}
		candidateCfg, err := gstreamerCandidateConfig(controller.cfg, change)
		if err != nil {
			return nil, err
		}
		candidateElements, err := resolveGStreamerCandidateElements(
			controller.ctx, candidateCfg, controller.elements, change.Role)
		if err != nil {
			return nil, err
		}
		epoch := controller.nextEpoch[change.Role] + 1
		if epoch == 0 {
			epoch = 1
		}
		if _, err := buildGStreamerRoleArgs(
			candidateCfg, candidateElements, controller.buses, change.Role,
		); err != nil {
			return nil, err
		}
		transaction.specs[change.Role] = gstreamerCandidateSpec{
			cfg: candidateCfg, elements: candidateElements, epoch: epoch,
		}
	}
	return transaction, nil
}

func gstreamerCandidateConfig(
	base ingestConfig,
	change videoproviders.WorkerBranchChange,
) (ingestConfig, error) {
	if change.Profile == nil {
		return ingestConfig{}, errors.New("candidate profile is required")
	}
	cfg := base
	profile := change.Profile
	switch change.Role {
	case "analysis":
		cfg.analysisEnabled = true
		cfg.subWidth, cfg.subHeight, cfg.subFPS = profile.Width, profile.Height, profile.FPS
	case "live":
		cfg.liveEnabled = true
		cfg.liveCodec = profile.Codec
		cfg.mainWidth, cfg.mainHeight, cfg.mainFPS = profile.Width, profile.Height, profile.FPS
		cfg.liveBitrateKbps = profile.BitrateKbps
	case "archive":
		if profile.RateControl != "cbr" {
			return ingestConfig{}, errors.New("portable GStreamer archive requires CBR")
		}
		cfg.archiveEnabled = true
		cfg.archiveCodec, cfg.archiveContent = profile.Codec, profile.Content
		cfg.archiveWidth, cfg.archiveHeight, cfg.archiveFPS = profile.Width, profile.Height, profile.FPS
		cfg.archiveBitrateKbps = profile.BitrateKbps
	default:
		return ingestConfig{}, fmt.Errorf("unsupported candidate role %q", change.Role)
	}
	if err := validateGStreamerWorkerConfig(cfg); err != nil {
		return ingestConfig{}, err
	}
	return cfg, nil
}

func resolveGStreamerCandidateElements(
	ctx context.Context,
	cfg ingestConfig,
	base gstElements,
	role string,
) (gstElements, error) {
	elements := base
	resolveEncoder := func(provider, codec string) (string, error) {
		allowSoftware := strings.TrimSpace(provider) == "software"
		candidates, err := encoderCandidates(provider, codec, allowSoftware)
		if err != nil {
			return "", err
		}
		selected := firstAvailableElement(ctx, candidates)
		if selected == "" {
			return "", fmt.Errorf("no admitted %s encoder is available for provider %q", codec, provider)
		}
		return selected, nil
	}
	var err error
	switch role {
	case "live":
		if !canUseNativeLive(cfg) {
			elements.liveEncoder, err = resolveEncoder(cfg.encodeProvider, cfg.liveCodec)
		}
	case "archive":
		if archiveRequiresEncode(cfg) {
			elements.archiveEncoder, err = resolveEncoder(cfg.archiveEncodeProvider, cfg.archiveCodec)
		}
	case "analysis":
	default:
		return gstElements{}, fmt.Errorf("unsupported GStreamer candidate role %q", role)
	}
	return elements, err
}

func (controller *gstreamerFanoutController) warmupLocked(
	transaction *gstreamerFanoutTransaction,
) error {
	for _, change := range transaction.changes {
		spec, exists := transaction.specs[change.Role]
		if !exists || transaction.candidates[change.Role] != nil {
			continue
		}
		role, err := controller.startRole(
			controller.ctx, spec.cfg, spec.elements, controller.buses, change.Role, spec.epoch)
		if err != nil {
			return err
		}
		transaction.candidates[change.Role] = role
		controller.startForwarder(controller.ctx, role, controller.router, time.Time{})
		controller.monitorRole(role)
	}
	return controller.checkCandidatesLocked(transaction)
}

func (controller *gstreamerFanoutController) checkCandidatesLocked(
	transaction *gstreamerFanoutTransaction,
) error {
	for role, candidate := range transaction.candidates {
		if err, exited := candidate.forwardResult(); exited {
			return fmt.Errorf("candidate %s exited: %w", role, err)
		}
	}
	return nil
}

func (controller *gstreamerFanoutController) waitCandidateFramesLocked(
	transaction *gstreamerFanoutTransaction,
	minimum map[string]uint64,
) ([]videoproviders.WorkerFanoutBranchReceipt, error) {
	ctx, cancel := context.WithTimeout(controller.ctx, gstreamerFanoutFrameTimeout)
	defer cancel()
	roles := make([]string, 0, len(transaction.candidates))
	for role := range transaction.candidates {
		roles = append(roles, role)
	}
	sort.Strings(roles)
	receipts := make([]videoproviders.WorkerFanoutBranchReceipt, 0, len(roles))
	for _, role := range roles {
		floor := uint64(0)
		if minimum != nil {
			floor = minimum[role]
		}
		evidence, err := transaction.candidates[role].waitEvidence(ctx, floor)
		if err != nil {
			return nil, fmt.Errorf("candidate %s frame: %w", role, err)
		}
		receipts = append(receipts, evidence)
	}
	return receipts, nil
}

func (controller *gstreamerFanoutController) cutoverLocked(
	transaction *gstreamerFanoutTransaction,
) error {
	if err := controller.checkCandidatesLocked(transaction); err != nil {
		return err
	}
	replacements := make(map[string]*isolatedGStreamerRole, len(transaction.changes))
	for _, change := range transaction.changes {
		replacements[change.Role] = transaction.candidates[change.Role]
	}
	if err := controller.router.switchRoles(replacements); err != nil {
		return err
	}
	for role, candidate := range transaction.candidates {
		transaction.cutoverFrames[role] = candidate.frameCount()
	}
	return nil
}

func (controller *gstreamerFanoutController) rollbackLocked(
	transaction *gstreamerFanoutTransaction,
) error {
	if err := controller.router.switchRoles(transaction.originals); err != nil {
		return err
	}
	return controller.stopCandidatesLocked(transaction)
}

func (controller *gstreamerFanoutController) abortLocked(
	transaction *gstreamerFanoutTransaction,
) error {
	return controller.stopCandidatesLocked(transaction)
}

func (controller *gstreamerFanoutController) stopCandidatesLocked(
	transaction *gstreamerFanoutTransaction,
) error {
	var stopErr error
	for _, change := range transaction.changes {
		candidate := transaction.candidates[change.Role]
		if candidate == nil {
			continue
		}
		if err := stopGStreamerRole(candidate); err != nil {
			stopErr = errors.Join(stopErr, fmt.Errorf("stop candidate %s: %w", change.Role, err))
		}
	}
	return stopErr
}

func (controller *gstreamerFanoutController) retireLocked(
	transaction *gstreamerFanoutTransaction,
) error {
	if err := controller.checkCandidatesLocked(transaction); err != nil {
		return err
	}
	var stopErr error
	for _, change := range transaction.changes {
		if incumbent := transaction.originals[change.Role]; incumbent != nil {
			if err := stopGStreamerRole(incumbent); err != nil {
				stopErr = errors.Join(stopErr, fmt.Errorf("stop incumbent %s: %w", change.Role, err))
			}
		}
	}
	if stopErr != nil {
		return stopErr
	}
	for _, change := range transaction.changes {
		candidate := transaction.candidates[change.Role]
		if candidate == nil {
			delete(controller.stable, change.Role)
		} else {
			controller.stable[change.Role] = candidate
			controller.nextEpoch[change.Role] = candidate.branchEpoch
			spec := transaction.specs[change.Role]
			controller.cfg = applyGStreamerRoleConfig(controller.cfg, spec.cfg, change.Role)
			if change.Role == "live" {
				controller.elements.liveEncoder = spec.elements.liveEncoder
			} else if change.Role == "archive" {
				controller.elements.archiveEncoder = spec.elements.archiveEncoder
			}
		}
		if candidate == nil {
			controller.cfg = disableGStreamerRole(controller.cfg, change.Role)
		}
	}
	return nil
}

func applyGStreamerRoleConfig(base, candidate ingestConfig, role string) ingestConfig {
	switch role {
	case "analysis":
		base.analysisEnabled = true
		base.subWidth, base.subHeight, base.subFPS = candidate.subWidth, candidate.subHeight, candidate.subFPS
	case "live":
		base.liveEnabled = true
		base.liveCodec = candidate.liveCodec
		base.mainWidth, base.mainHeight, base.mainFPS = candidate.mainWidth, candidate.mainHeight, candidate.mainFPS
		base.liveBitrateKbps = candidate.liveBitrateKbps
	case "archive":
		base.archiveEnabled = true
		base.archiveCodec, base.archiveContent = candidate.archiveCodec, candidate.archiveContent
		base.archiveWidth, base.archiveHeight, base.archiveFPS = candidate.archiveWidth, candidate.archiveHeight, candidate.archiveFPS
		base.archiveBitrateKbps = candidate.archiveBitrateKbps
	}
	return base
}

func disableGStreamerRole(cfg ingestConfig, role string) ingestConfig {
	switch role {
	case "analysis":
		cfg.analysisEnabled = false
	case "archive":
		cfg.archiveEnabled = false
	case "live":
		cfg.liveEnabled = false
	}
	return cfg
}

func stopGStreamerRole(role *isolatedGStreamerRole) error {
	if role == nil {
		return nil
	}
	err := role.child.stop(gstChildTerminationGrace)
	if role.reader != nil {
		err = errors.Join(err, role.reader.Close())
	}
	return err
}

func gstreamerFanoutPhaseErrorCode(phase string, err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return "first_frame_timeout"
	}
	switch phase {
	case videoproviders.WorkerFanoutPhaseWarmup:
		return "warmup_failed"
	case videoproviders.WorkerFanoutPhaseAdmit:
		return "admission_failed"
	case videoproviders.WorkerFanoutPhaseFirstFrame:
		return "first_frame_failed"
	case videoproviders.WorkerFanoutPhaseCutover:
		return "cutover_failed"
	case videoproviders.WorkerFanoutPhaseVerify:
		return "verify_failed"
	case videoproviders.WorkerFanoutPhaseRollback:
		return "rollback_failed"
	case videoproviders.WorkerFanoutPhaseAbort:
		return "abort_failed"
	case videoproviders.WorkerFanoutPhaseRetire:
		return "retire_failed"
	default:
		return "worker_rejected"
	}
}

func runGStreamerFanoutControl(
	ctx context.Context,
	input io.Reader,
	output io.Writer,
	controller *gstreamerFanoutController,
) error {
	if ctx == nil || input == nil || output == nil || controller == nil {
		return errors.New("GStreamer fan-out control channel is incomplete")
	}
	scanner := bufio.NewScanner(input)
	scanner.Buffer(make([]byte, 1024), videoproviders.WorkerFanoutMaxLineBytes)
	for scanner.Scan() {
		line := append(append([]byte(nil), scanner.Bytes()...), '\n')
		command, err := videoproviders.DecodeWorkerFanoutCommand(line)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[video-ingest-gst][fanout-control] rejected invalid command: %v\n", err)
			continue
		}
		receipt := controller.handle(command)
		payload, err := videoproviders.EncodeWorkerFanoutReceipt(receipt)
		if err != nil {
			return fmt.Errorf("encode GStreamer fan-out receipt: %w", err)
		}
		if err := writeFull(output, payload); err != nil {
			return fmt.Errorf("write GStreamer fan-out receipt: %w", err)
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read GStreamer fan-out command: %w", err)
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return errors.New("GStreamer fan-out control channel closed")
}
