package broker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/Josepavese/videosluice/capability"
	productconfig "github.com/Josepavese/videosluice/config"
	videoproviders "github.com/Josepavese/videosluice/internal/source"
	"github.com/Josepavese/videosluice/internal/unixsocket"
)

var fanoutPublicationRoles = [...]string{"analysis", "archive", "live"}

type runtimeFanoutPublicationCoordinator struct {
	listen    func(string) (*unixsocket.Publication, error)
	newShared func(string, int) (*brokerSubSharedFrame, error)
}

func newRuntimeFanoutPublicationCoordinator() *runtimeFanoutPublicationCoordinator {
	return &runtimeFanoutPublicationCoordinator{
		listen: listenBrokerSocket, newShared: newBrokerSubSharedFrame,
	}
}

type fanoutPublicationRoleKey struct {
	sourceID string
	role     string
}

type runtimeFanoutPublicationRole struct {
	key       fanoutPublicationRoleKey
	runtime   *videoBrokerRuntime
	current   *productconfig.NamedStream
	candidate *productconfig.NamedStream

	currentPublication       *unixsocket.Publication
	currentPublicationCancel context.CancelFunc
	currentShared            *brokerSubSharedFrame
	currentNotifier          *brokerSubFrameNotifier

	candidatePublication       *unixsocket.Publication
	candidatePublicationCancel context.CancelFunc
	candidatePublicationPath   string
	candidateShared            *brokerSubSharedFrame
	candidateSharedPath        string
	candidateNotifier          *brokerSubFrameNotifier
	candidateRuntimeMode       string

	snapshot      runtimeFanoutRoleSnapshot
	cutoverFrames uint64
}

type runtimeFanoutRoleSnapshot struct {
	mainBootstrap            [][]byte
	mainCodecConfig          [][]byte
	mainBootstrapBytes       int
	mainAwaitingEncoderEpoch bool
	mainAwaitingKeyframe     bool
	mainBootstrapRecoveryAt  time.Time
	activeMainWidth          int
	activeMainHeight         int
	mainBitrateKbps          int
	childLiveControlMode     string
	childControlSupported    bool
	routingLiveDemand        bool

	archiveBootstrap        [][]byte
	archiveCodecConfig      [][]byte
	archiveBootstrapBytes   int
	archiveCodecStreamID    byte
	activeArchiveWidth      int
	activeArchiveHeight     int
	archiveBitrateKbps      int
	observedArchiveCodec    string
	observedArchiveDelivery string
	observedArchiveFPS      int
}

type runtimeFanoutPublicationSource struct {
	sourceID            string
	runtime             *videoBrokerRuntime
	workerParticipates  bool
	currentPlan         videoBrokerPlan
	candidatePlan       videoBrokerPlan
	currentCapability   capability.Plan
	candidateCapability capability.Plan
	roles               []*runtimeFanoutPublicationRole
}

type runtimeFanoutPublicationMove struct {
	streamID string
	role     string
	from     *runtimeFanoutPublicationRole
	to       *runtimeFanoutPublicationRole

	socketSwapped bool
	sharedSwapped bool
}
type runtimeFanoutPreparedPublications struct {
	mu sync.Mutex

	transactionID  string
	sources        []*runtimeFanoutPublicationSource
	roles          map[fanoutPublicationRoleKey]*runtimeFanoutPublicationRole
	moves          []*runtimeFanoutPublicationMove
	phase          int
	workerAttested bool
	cutoverStaged  bool
	cutover        bool
	terminal       bool
}

func (coordinator *runtimeFanoutPublicationCoordinator) ValidateFanoutPublications(
	ctx context.Context,
	transition preservedOwnerFanout,
	handles map[string]sourceFanoutHandle,
	set workerFanoutCapabilitySet,
) error {
	_, err := coordinator.validateFanoutPublicationState(ctx, transition, handles, set)
	return err
}

func (coordinator *runtimeFanoutPublicationCoordinator) validateFanoutPublicationState(
	ctx context.Context,
	transition preservedOwnerFanout,
	handles map[string]sourceFanoutHandle,
	set workerFanoutCapabilitySet,
) (map[fanoutPublicationRoleKey]*runtimeFanoutPublicationRole, error) {
	if err := coordinator.validateDependencies(); err != nil {
		return nil, err
	}
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	roles, err := fanoutPublicationRoleChanges(transition, handles)
	if err != nil {
		return nil, err
	}
	if len(set.Current) != len(transition.SourceIDs) ||
		len(set.Candidate) != len(transition.SourceIDs) ||
		len(set.Changes) != len(transition.SourceIDs) {
		return nil, errors.New("fan-out publication capability set is incomplete")
	}
	for _, sourceID := range transition.SourceIDs {
		handle := handles[sourceID]
		handle.Runtime.mu.Lock()
		for _, role := range fanoutPublicationRoles {
			state := roles[fanoutPublicationRoleKey{sourceID: sourceID, role: role}]
			if state == nil {
				continue
			}
			if err := validateFanoutCurrentPublicationRoleLocked(state); err != nil {
				handle.Runtime.mu.Unlock()
				return nil, err
			}
		}
		if !sameFanoutCapabilityIdentity(handle.Runtime.capabilityPlan, set.Current[sourceID]) {
			handle.Runtime.mu.Unlock()
			return nil, fmt.Errorf("fan-out publication source %q capability plan changed", sourceID)
		}
		handle.Runtime.mu.Unlock()
	}
	return roles, nil
}

func (coordinator *runtimeFanoutPublicationCoordinator) PrepareFanoutPublications(
	ctx context.Context,
	transactionID string,
	transition preservedOwnerFanout,
	handles map[string]sourceFanoutHandle,
	set workerFanoutCapabilitySet,
) (fanoutPreparedPublications, error) {
	if strings.TrimSpace(transactionID) == "" || strings.TrimSpace(transactionID) != transactionID {
		return nil, errors.New("fan-out publication transaction id is not canonical")
	}
	roles, err := coordinator.validateFanoutPublicationState(ctx, transition, handles, set)
	if err != nil {
		return nil, err
	}
	prepared := &runtimeFanoutPreparedPublications{
		transactionID: transactionID, roles: roles,
	}
	for _, sourceID := range transition.SourceIDs {
		handle := handles[sourceID]
		handle.Runtime.mu.Lock()
		currentPlan := cloneFanoutVideoBrokerPlan(handle.Runtime.plan)
		currentCapability := cloneFanoutCapabilityPlan(handle.Runtime.capabilityPlan)
		handle.Runtime.mu.Unlock()
		candidatePlan, err := fanoutCandidateRuntimePlan(
			currentPlan, handle.CandidateGraph, set.ArchiveCodecs[sourceID])
		if err != nil {
			prepared.cleanupCandidates()
			return nil, err
		}
		source := &runtimeFanoutPublicationSource{
			sourceID: sourceID, runtime: handle.Runtime,
			workerParticipates: handle.CaptureAvailable,
			currentPlan:        currentPlan, candidatePlan: candidatePlan,
			currentCapability:   currentCapability,
			candidateCapability: cloneFanoutCapabilityPlan(set.Candidate[sourceID]),
		}
		for _, role := range fanoutPublicationRoles {
			if state := roles[fanoutPublicationRoleKey{sourceID: sourceID, role: role}]; state != nil {
				source.roles = append(source.roles, state)
			}
		}
		prepared.sources = append(prepared.sources, source)
	}
	for _, source := range prepared.sources {
		for _, role := range source.roles {
			if err := coordinator.prepareCandidateRole(role); err != nil {
				prepared.cleanupCandidates()
				return nil, fmt.Errorf(
					"prepare fan-out publication source=%q role=%q: %w",
					role.key.sourceID, role.key.role, err)
			}
		}
	}
	for _, move := range transition.Moves {
		from := roles[fanoutPublicationRoleKey{sourceID: move.From, role: move.Type}]
		to := roles[fanoutPublicationRoleKey{sourceID: move.To, role: move.Type}]
		if from == nil || to == nil || from.current == nil || to.candidate == nil ||
			from.current.ID != move.StreamID || to.candidate.ID != move.StreamID {
			prepared.cleanupCandidates()
			return nil, fmt.Errorf("fan-out publication move %q has no exact role endpoints", move.StreamID)
		}
		prepared.moves = append(prepared.moves, &runtimeFanoutPublicationMove{
			streamID: move.StreamID, role: move.Type, from: from, to: to,
		})
	}
	return prepared, nil
}

func (coordinator *runtimeFanoutPublicationCoordinator) validateDependencies() error {
	if coordinator == nil || coordinator.listen == nil || coordinator.newShared == nil {
		return errors.New("fan-out publication coordinator is unavailable")
	}
	return nil
}

func (coordinator *runtimeFanoutPublicationCoordinator) prepareCandidateRole(
	role *runtimeFanoutPublicationRole,
) error {
	if role.candidate == nil {
		return nil
	}
	stream := role.candidate.Config
	if stream.Socket != "" {
		stagePath, err := newFanoutPublicationStagePath(stream.Socket)
		if err != nil {
			return err
		}
		publication, err := coordinator.listen(stagePath)
		if err != nil {
			return err
		}
		role.candidatePublication = publication
		role.candidatePublicationPath = stagePath
	}
	if role.key.role == "analysis" && stream.SharedMemory != "" {
		payloadCap := (stream.Width * stream.Height * 3) / 2
		if payloadCap <= 0 {
			return errors.New("analysis candidate shared-memory capacity is invalid")
		}
		stagePath, err := newFanoutPublicationStagePath(stream.SharedMemory)
		if err != nil {
			return err
		}
		shared, err := coordinator.newShared(stagePath, payloadCap)
		if err != nil {
			return err
		}
		role.candidateShared = shared
		role.candidateSharedPath = stagePath
	}
	if role.key.role == "analysis" {
		role.candidateNotifier = newBrokerSubFrameNotifier(stream.NotifySocket)
	}
	return nil
}

func fanoutPublicationRoleChanges(
	transition preservedOwnerFanout,
	handles map[string]sourceFanoutHandle,
) (map[fanoutPublicationRoleKey]*runtimeFanoutPublicationRole, error) {
	roles := make(map[fanoutPublicationRoleKey]*runtimeFanoutPublicationRole)
	for _, sourceID := range transition.SourceIDs {
		handle, exists := handles[sourceID]
		if !exists || handle.Runtime == nil || handle.SourceID != sourceID {
			return nil, fmt.Errorf("fan-out publication source %q has no runtime handle", sourceID)
		}
		for _, role := range fanoutPublicationRoles {
			current := workerFanoutNamedStream(handle.CurrentGraph, role)
			candidate := workerFanoutNamedStream(handle.CandidateGraph, role)
			if reflect.DeepEqual(current, candidate) {
				continue
			}
			key := fanoutPublicationRoleKey{sourceID: sourceID, role: role}
			roles[key] = &runtimeFanoutPublicationRole{
				key: key, runtime: handle.Runtime, current: current, candidate: candidate,
			}
		}
	}
	if len(roles) == 0 {
		return nil, errors.New("fan-out publication transaction has no changed roles")
	}
	return roles, nil
}

func validateFanoutCurrentPublicationRoleLocked(role *runtimeFanoutPublicationRole) error {
	runtime := role.runtime
	if runtime.fanoutRoleGates[role.key.role] != "" {
		return fmt.Errorf(
			"fan-out publication source=%q role=%q is already gated",
			role.key.sourceID, role.key.role)
	}
	if !fanoutRuntimePlanMatchesRole(runtime.plan, role.key.role, role.current) {
		return fmt.Errorf(
			"fan-out publication source=%q role=%q differs from current plan",
			role.key.sourceID, role.key.role)
	}
	publication, cancel, shared, notifier := fanoutRuntimeRoleBindingLocked(runtime, role.key.role)
	role.currentPublication = publication
	role.currentPublicationCancel = cancel
	role.currentShared = shared
	role.currentNotifier = notifier
	switch {
	case role.current == nil:
		if publication != nil || shared != nil || notifier != nil {
			return fmt.Errorf(
				"fan-out publication source=%q role=%q has an undeclared owner",
				role.key.sourceID, role.key.role)
		}
	case role.current.Config.Socket != "" &&
		(publication == nil || !publication.Owns(role.current.Config.Socket)):
		return fmt.Errorf(
			"fan-out publication source=%q role=%q socket owner differs",
			role.key.sourceID, role.key.role)
	case role.key.role == "analysis" && role.current.Config.SharedMemory != "" &&
		(shared == nil || !shared.Owns(role.current.Config.SharedMemory)):
		return fmt.Errorf("fan-out publication source=%q analysis shared-memory owner differs", role.key.sourceID)
	case role.key.role == "analysis" && role.current.Config.NotifySocket != "" && notifier == nil:
		return fmt.Errorf("fan-out publication source=%q analysis notifier is unavailable", role.key.sourceID)
	}
	if role.candidate != nil {
		if err := validateFanoutPublicationPaths(role.candidate.Config, role.key.role); err != nil {
			return fmt.Errorf(
				"fan-out publication source=%q role=%q candidate: %w",
				role.key.sourceID, role.key.role, err)
		}
	}
	return nil
}

func validateFanoutPublicationPaths(stream productconfig.StreamConfig, role string) error {
	paths := []string{stream.Socket}
	if role == "analysis" {
		paths = append(paths, stream.SharedMemory, stream.NotifySocket)
	}
	for _, path := range paths {
		if path == "" {
			continue
		}
		clean := filepath.Clean(path)
		if !filepath.IsAbs(clean) || clean == "/" || clean != path {
			return errors.New("publication path is not absolute and canonical")
		}
	}
	return nil
}

func fanoutRuntimePlanMatchesRole(
	plan videoBrokerPlan,
	role string,
	stream *productconfig.NamedStream,
) bool {
	if stream == nil {
		switch role {
		case "live":
			return !plan.LiveEnabled
		case "archive":
			return !plan.ArchiveEnabled
		case "analysis":
			return !plan.AnalysisEnabled
		default:
			return false
		}
	}
	value := stream.Config
	switch role {
	case "live":
		return plan.LiveEnabled && plan.MainSocketPath == value.Socket &&
			strings.EqualFold(plan.MainFormat, value.Codec) &&
			plan.MainWidth == value.Width && plan.MainHeight == value.Height &&
			plan.MainFPS == value.FPS && plan.LiveBitrateKbps == value.BitrateKbps
	case "archive":
		codecMatches := strings.EqualFold(plan.ArchiveCodec, value.Codec) ||
			(strings.EqualFold(value.Codec, "auto") &&
				(strings.EqualFold(plan.ArchiveCodec, "h264") || strings.EqualFold(plan.ArchiveCodec, "h265")))
		return plan.ArchiveEnabled && plan.ArchiveSocketPath == value.Socket && codecMatches &&
			plan.ArchiveWidth == value.Width && plan.ArchiveHeight == value.Height &&
			plan.ArchiveFPS == value.FPS && plan.ArchiveBitrateKbps == value.BitrateKbps &&
			strings.EqualFold(plan.ArchiveRateControl, value.RateControl)
	case "analysis":
		return plan.AnalysisEnabled && plan.SubSocketPath == value.Socket &&
			plan.SubShmPath == value.SharedMemory && plan.SubNotifySocketPath == value.NotifySocket &&
			strings.EqualFold(plan.SubFormat, value.PixelFormat) &&
			plan.SubWidth == value.Width && plan.SubHeight == value.Height && plan.SubFPS == value.FPS
	default:
		return false
	}
}

func fanoutCandidateRuntimePlan(
	current videoBrokerPlan,
	graph productconfig.SourceGraph,
	archiveCodec string,
) (videoBrokerPlan, error) {
	if current.SourceID == "" || current.SourceID != graph.SourceID {
		return videoBrokerPlan{}, errors.New("fan-out candidate plan differs from runtime source")
	}
	plan := current
	plan.LiveEnabled = graph.Live != nil
	plan.MainSocketPath, plan.MainFormat = "", ""
	plan.MainWidth, plan.MainHeight, plan.MainFPS, plan.LiveBitrateKbps = 0, 0, 0, 0
	if graph.Live != nil {
		stream := graph.Live.Config
		plan.MainSocketPath, plan.MainFormat = stream.Socket, stream.Codec
		plan.MainWidth, plan.MainHeight, plan.MainFPS = stream.Width, stream.Height, stream.FPS
		plan.LiveBitrateKbps = stream.BitrateKbps
	}
	plan.AnalysisEnabled = graph.Analysis != nil
	plan.AnalysisStreamID = ""
	plan.SubSocketPath, plan.SubShmPath, plan.SubNotifySocketPath, plan.SubFormat = "", "", "", ""
	plan.SubWidth, plan.SubHeight, plan.SubFPS = 0, 0, 0
	if graph.Analysis != nil {
		plan.AnalysisStreamID = graph.Analysis.ID
		stream := graph.Analysis.Config
		plan.SubSocketPath, plan.SubShmPath = stream.Socket, stream.SharedMemory
		plan.SubNotifySocketPath, plan.SubFormat = stream.NotifySocket, stream.PixelFormat
		plan.SubWidth, plan.SubHeight, plan.SubFPS = stream.Width, stream.Height, stream.FPS
	}
	plan.ArchiveEnabled = graph.Archive != nil
	plan.ArchiveSocketPath, plan.ArchiveCodec, plan.ArchiveContent, plan.ArchiveRateControl = "", "", "", ""
	plan.ArchiveWidth, plan.ArchiveHeight, plan.ArchiveFPS, plan.ArchiveBitrateKbps = 0, 0, 0, 0
	if graph.Archive != nil {
		stream := graph.Archive.Config
		codec := strings.ToLower(strings.TrimSpace(stream.Codec))
		if codec == "auto" {
			codec = strings.ToLower(strings.TrimSpace(archiveCodec))
		}
		if codec != "h264" && codec != "h265" {
			return videoBrokerPlan{}, errors.New("fan-out archive candidate codec is unresolved")
		}
		plan.ArchiveSocketPath, plan.ArchiveCodec = stream.Socket, codec
		plan.ArchiveContent, plan.ArchiveRateControl = stream.Content, stream.RateControl
		plan.ArchiveWidth, plan.ArchiveHeight, plan.ArchiveFPS = stream.Width, stream.Height, stream.FPS
		plan.ArchiveBitrateKbps = stream.BitrateKbps
	}
	plan.Enabled = plan.LiveEnabled || plan.AnalysisEnabled || plan.ArchiveEnabled
	return plan, nil
}

func cloneFanoutCapabilityPlan(plan capability.Plan) capability.Plan {
	plan.Stages = append([]capability.StageSelection(nil), plan.Stages...)
	return plan
}

// Runtime attestation advances only State. That evidence may arrive while a
// candidate graph is warming up and must neither invalidate the transaction
// nor be lost on rollback. Every field that selects resources or reports a
// fallback remains part of the immutable transaction identity.
func sameFanoutCapabilityIdentity(left, right capability.Plan) bool {
	left = cloneFanoutCapabilityPlan(left)
	right = cloneFanoutCapabilityPlan(right)
	for index := range left.Stages {
		left.Stages[index].State = ""
	}
	for index := range right.Stages {
		right.Stages[index].State = ""
	}
	return reflect.DeepEqual(left, right)
}

func cloneFanoutVideoBrokerPlan(plan videoBrokerPlan) videoBrokerPlan {
	inventory := plan.CapabilityInventory
	inventory.Components = append([]capability.Component(nil), inventory.Components...)
	for index := range inventory.Components {
		component := &inventory.Components[index]
		component.Operations = append([]capability.Operation(nil), component.Operations...)
		component.Evidence = append([]capability.Evidence(nil), component.Evidence...)
	}
	plan.CapabilityInventory = inventory
	return plan
}

func sameFanoutRuntimePlanIdentity(left, right videoBrokerPlan) bool {
	left = cloneFanoutVideoBrokerPlan(left)
	right = cloneFanoutVideoBrokerPlan(right)
	// Inventory state and evidence are observations, not routing settings.
	left.CapabilityInventory = capability.Inventory{}
	right.CapabilityInventory = capability.Inventory{}
	return reflect.DeepEqual(left, right)
}

func newFanoutPublicationStagePath(logical string) (string, error) {
	logical = filepath.Clean(logical)
	if !filepath.IsAbs(logical) || logical == "/" {
		return "", errors.New("fan-out logical publication path is invalid")
	}
	for attempt := 0; attempt < 8; attempt++ {
		var nonce [6]byte
		if _, err := rand.Read(nonce[:]); err != nil {
			return "", err
		}
		candidate := filepath.Join(filepath.Dir(logical), ".vb-fanout-"+hex.EncodeToString(nonce[:]))
		if _, err := os.Lstat(candidate); errors.Is(err, os.ErrNotExist) {
			return candidate, nil
		} else if err != nil {
			return "", err
		}
	}
	return "", errors.New("could not allocate fan-out publication stage path")
}

func (prepared *runtimeFanoutPreparedPublications) Warmup(ctx context.Context) error {
	return prepared.advancePrivatePhase(ctx, 0)
}

func (prepared *runtimeFanoutPreparedPublications) Admit(ctx context.Context) error {
	return prepared.advancePrivatePhase(ctx, 1)
}

func (prepared *runtimeFanoutPreparedPublications) AwaitFirstFrame(ctx context.Context) error {
	return prepared.advancePrivatePhase(ctx, 2)
}

func (prepared *runtimeFanoutPreparedPublications) AttestFirstFrame(
	receipts map[string]videoproviders.WorkerFanoutReceipt,
) error {
	prepared.mu.Lock()
	defer prepared.mu.Unlock()
	if err := prepared.requirePhaseLocked(2); err != nil {
		return err
	}
	if prepared.workerAttested {
		return errors.New("fan-out publication worker evidence is already attested")
	}
	requiredReceipts := 0
	for _, source := range prepared.sources {
		if source.workerParticipates {
			requiredReceipts++
		}
	}
	if len(receipts) != requiredReceipts {
		return errors.New("fan-out publication worker evidence has an incomplete source set")
	}
	modes := make(map[fanoutPublicationRoleKey]string)
	for _, source := range prepared.sources {
		if !source.workerParticipates {
			for _, role := range source.roles {
				if role.candidate != nil {
					return fmt.Errorf(
						"fan-out publication unavailable source %q has a candidate role", source.sourceID)
				}
			}
			continue
		}
		receipt, exists := receipts[source.sourceID]
		if !exists || receipt.SourceID != source.sourceID {
			return fmt.Errorf(
				"fan-out publication source %q has no exact worker evidence", source.sourceID)
		}
		if err := videoproviders.ValidateWorkerFanoutReceipt(receipt); err != nil {
			return fmt.Errorf(
				"fan-out publication source %q worker evidence: %w", source.sourceID, err)
		}
		if receipt.TransactionID != prepared.transactionID ||
			receipt.Event != videoproviders.WorkerFanoutEventFirstFrame {
			return fmt.Errorf(
				"fan-out publication source %q worker evidence differs from transaction",
				source.sourceID)
		}
		expected := make(map[string]*runtimeFanoutPublicationRole)
		for _, role := range source.roles {
			if role.candidate != nil {
				expected[role.key.role] = role
			}
		}
		if len(receipt.Branches) != len(expected) {
			return fmt.Errorf(
				"fan-out publication source %q worker evidence differs from candidate roles",
				source.sourceID)
		}
		for _, branch := range receipt.Branches {
			role := expected[branch.Role]
			if role == nil {
				return fmt.Errorf(
					"fan-out publication source %q worker evidence has unexpected role %q",
					source.sourceID, branch.Role)
			}
			modes[role.key] = branch.RuntimeMode
			delete(expected, branch.Role)
		}
		if len(expected) != 0 {
			return fmt.Errorf(
				"fan-out publication source %q worker evidence omits candidate roles",
				source.sourceID)
		}
	}
	for key, mode := range modes {
		prepared.roles[key].candidateRuntimeMode = mode
	}
	prepared.workerAttested = true
	return nil
}

func (prepared *runtimeFanoutPreparedPublications) advancePrivatePhase(
	ctx context.Context,
	expected int,
) error {
	prepared.mu.Lock()
	defer prepared.mu.Unlock()
	if err := prepared.requirePhaseLocked(expected); err != nil {
		return err
	}
	if err := contextError(ctx); err != nil {
		return err
	}
	if expected == 2 && !prepared.workerAttested {
		return errors.New("fan-out publication first frame lacks worker runtime attestation")
	}
	if err := prepared.validatePrivateCandidatesLocked(); err != nil {
		return err
	}
	prepared.phase++
	return nil
}

func (prepared *runtimeFanoutPreparedPublications) StageCutover(ctx context.Context) error {
	prepared.mu.Lock()
	defer prepared.mu.Unlock()
	if err := prepared.requirePhaseLocked(3); err != nil {
		return err
	}
	if prepared.cutoverStaged {
		return errors.New("fan-out publication cutover is already staged")
	}
	if err := contextError(ctx); err != nil {
		return err
	}
	locked := prepared.lockRuntimes()
	defer unlockFanoutRuntimes(locked)
	for _, source := range prepared.sources {
		if !sameFanoutRuntimePlanIdentity(source.runtime.plan, source.currentPlan) ||
			!sameFanoutCapabilityIdentity(source.runtime.capabilityPlan, source.currentCapability) {
			return fmt.Errorf("fan-out publication source %q changed before cutover", source.sourceID)
		}
		for _, role := range source.roles {
			if owner := role.runtime.fanoutRoleGates[role.key.role]; owner != "" {
				return fmt.Errorf("stage fan-out source=%q role=%q: role gate is owned by %q",
					source.sourceID, role.key.role, owner)
			}
		}
	}
	for _, source := range prepared.sources {
		// Preserve observations that arrived during warmup. The candidate changes
		// only routing roles, so it inherits the newest inventory evidence.
		source.currentPlan = cloneFanoutVideoBrokerPlan(source.runtime.plan)
		source.currentCapability = cloneFanoutCapabilityPlan(source.runtime.capabilityPlan)
		source.candidatePlan.CapabilityInventory =
			cloneFanoutVideoBrokerPlan(source.runtime.plan).CapabilityInventory
		for _, role := range source.roles {
			role.snapshot = snapshotFanoutRoleLocked(role.runtime, role.key.role)
			if err := setFanoutRoleGateLocked(
				role.runtime, role.key.role, prepared.transactionID,
			); err != nil {
				return err
			}
		}
	}
	prepared.cutoverStaged = true
	return nil
}

func (prepared *runtimeFanoutPreparedPublications) Cutover(ctx context.Context) error {
	prepared.mu.Lock()
	defer prepared.mu.Unlock()
	if err := prepared.requirePhaseLocked(3); err != nil {
		return err
	}
	if !prepared.cutoverStaged || prepared.cutover {
		return errors.New("fan-out publication cutover gate is unavailable")
	}
	if err := contextError(ctx); err != nil {
		return err
	}
	locked := prepared.lockRuntimes()
	defer unlockFanoutRuntimes(locked)
	if err := prepared.validateGatesLocked(); err != nil {
		return err
	}
	for _, move := range prepared.moves {
		if move.to.candidatePublication != nil {
			err := move.to.candidatePublication.Replace(move.from.currentPublication)
			move.socketSwapped = move.to.candidatePublication.Owns(move.to.candidate.Config.Socket)
			if err != nil {
				return fmt.Errorf("cut over fan-out stream %q socket: %w", move.streamID, err)
			}
		}
		if move.to.candidateShared != nil {
			err := move.to.candidateShared.Replace(move.from.currentShared)
			move.sharedSwapped = move.to.candidateShared.Owns(move.to.candidate.Config.SharedMemory)
			if err != nil {
				return fmt.Errorf("cut over fan-out stream %q shared memory: %w", move.streamID, err)
			}
		}
	}
	for _, source := range prepared.sources {
		source.runtime.plan = source.candidatePlan
		source.runtime.qualityInputSince = time.Now()
		source.runtime.capabilityPlan = cloneFanoutCapabilityPlan(source.candidateCapability)
		for _, role := range source.roles {
			if role.currentNotifier != nil {
				role.currentNotifier.Rebind("")
			}
			setFanoutRuntimeRoleBindingLocked(
				role.runtime, role.key.role,
				role.candidatePublication, nil, role.candidateShared, role.candidateNotifier,
			)
			activateFanoutCandidateRoleLocked(role, source.candidatePlan)
			disconnectFanoutRoleClientsLocked(role.runtime, role.key.role)
		}
	}
	for _, source := range prepared.sources {
		for _, role := range source.roles {
			if role.candidatePublication != nil {
				cancel, err := role.runtime.startFanoutPublicationAcceptLoopLocked(
					role.candidatePublication, fanoutEndpointForRole(role.key.role))
				if err != nil {
					return fmt.Errorf(
						"start fan-out source=%q role=%q publication: %w",
						role.key.sourceID, role.key.role, err)
				}
				role.candidatePublicationCancel = cancel
				setFanoutRuntimePublicationCancelLocked(role.runtime, role.key.role, cancel)
			}
			role.cutoverFrames = fanoutRoleFrameCountLocked(role.runtime, role.key.role)
		}
	}
	for _, source := range prepared.sources {
		for _, role := range source.roles {
			if err := clearFanoutRoleGateLocked(
				role.runtime, role.key.role, prepared.transactionID,
			); err != nil {
				return err
			}
		}
		for _, role := range source.roles {
			if role.key.role == "live" && role.runtime.childControlAvailable {
				// Publish the reconciled controls even if the governor did not
				// change and no viewer connected during this transaction.
				role.runtime.queueBridgeControlForceLocked("fanout_live_cutover")
			}
		}
	}
	prepared.cutover = true
	prepared.phase = 4
	return nil
}

func (prepared *runtimeFanoutPreparedPublications) Verify(ctx context.Context) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	prepared.mu.Lock()
	if err := prepared.requirePhaseLocked(4); err != nil {
		prepared.mu.Unlock()
		return err
	}
	prepared.mu.Unlock()
	ticker := time.NewTicker(routingFramePollInterval)
	defer ticker.Stop()
	for {
		prepared.mu.Lock()
		locked := prepared.lockRuntimes()
		ready, err := prepared.candidatePublicationsReadyLocked()
		if err == nil && ready {
			for _, source := range prepared.sources {
				for _, role := range source.roles {
					if role.key.role == "live" {
						role.runtime.routingLiveDemand = role.snapshot.routingLiveDemand
						role.runtime.queueBridgeControlLocked("fanout_live_verified")
					}
				}
			}
			prepared.phase = 5
			unlockFanoutRuntimes(locked)
			prepared.mu.Unlock()
			return nil
		}
		unlockFanoutRuntimes(locked)
		prepared.mu.Unlock()
		if err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (prepared *runtimeFanoutPreparedPublications) StageRollback(ctx context.Context) error {
	prepared.mu.Lock()
	defer prepared.mu.Unlock()
	if prepared == nil || prepared.terminal || !prepared.cutoverStaged {
		return errors.New("fan-out publication rollback is unavailable")
	}
	if err := contextError(ctx); err != nil {
		return err
	}
	locked := prepared.lockRuntimes()
	defer unlockFanoutRuntimes(locked)
	for _, source := range prepared.sources {
		for _, role := range source.roles {
			owner := role.runtime.fanoutRoleGates[role.key.role]
			if owner != "" && owner != prepared.transactionID {
				return fmt.Errorf("fan-out rollback source=%q role=%q gate is owned by %q",
					source.sourceID, role.key.role, owner)
			}
		}
	}
	for _, source := range prepared.sources {
		for _, role := range source.roles {
			if err := setFanoutRoleGateLocked(
				role.runtime, role.key.role, prepared.transactionID,
			); err != nil {
				return err
			}
		}
	}
	return nil
}

func (prepared *runtimeFanoutPreparedPublications) Rollback(ctx context.Context) error {
	prepared.mu.Lock()
	defer prepared.mu.Unlock()
	if prepared == nil || prepared.terminal || !prepared.cutoverStaged {
		return nil
	}
	if err := contextError(ctx); err != nil {
		return err
	}
	locked := prepared.lockRuntimes()
	var rollbackErr error
	for index := len(prepared.moves) - 1; index >= 0; index-- {
		move := prepared.moves[index]
		if move.sharedSwapped {
			rollbackErr = errors.Join(
				rollbackErr, move.from.currentShared.Replace(move.to.candidateShared))
		}
		if move.socketSwapped {
			rollbackErr = errors.Join(
				rollbackErr, move.from.currentPublication.Replace(move.to.candidatePublication))
		}
	}
	if rollbackErr != nil {
		unlockFanoutRuntimes(locked)
		return rollbackErr
	}
	for _, source := range prepared.sources {
		source.runtime.plan = source.currentPlan
		source.runtime.qualityInputSince = time.Now()
		source.runtime.capabilityPlan = cloneFanoutCapabilityPlan(source.currentCapability)
		for _, role := range source.roles {
			disconnectFanoutRoleClientsLocked(role.runtime, role.key.role)
			if role.candidatePublicationCancel != nil {
				role.candidatePublicationCancel()
			}
			setFanoutRuntimeRoleBindingLocked(
				role.runtime, role.key.role,
				role.currentPublication, role.currentPublicationCancel,
				role.currentShared, role.currentNotifier,
			)
			if role.currentNotifier != nil && role.current != nil {
				role.currentNotifier.Rebind(role.current.Config.NotifySocket)
			}
			restoreFanoutRoleSnapshotLocked(role.runtime, role.key.role, role.snapshot)
			if role.key.role == "live" && role.runtime.childControlAvailable {
				role.runtime.queueBridgeControlForceLocked("fanout_live_rollback")
			}
			rollbackErr = errors.Join(rollbackErr, clearFanoutRoleGateLocked(
				role.runtime, role.key.role, prepared.transactionID))
		}
	}
	unlockFanoutRuntimes(locked)
	if rollbackErr == nil {
		prepared.cleanupCandidates()
		prepared.terminal = true
	}
	return rollbackErr
}

func (prepared *runtimeFanoutPreparedPublications) Abort(ctx context.Context) error {
	prepared.mu.Lock()
	defer prepared.mu.Unlock()
	if prepared == nil || prepared.terminal {
		return nil
	}
	if prepared.cutoverStaged {
		return errors.New("staged fan-out publication requires rollback")
	}
	if err := contextError(ctx); err != nil {
		return err
	}
	err := prepared.cleanupCandidates()
	prepared.terminal = err == nil
	return err
}

func (prepared *runtimeFanoutPreparedPublications) Retire(ctx context.Context) error {
	prepared.mu.Lock()
	defer prepared.mu.Unlock()
	if err := prepared.requirePhaseLocked(5); err != nil {
		return err
	}
	if err := contextError(ctx); err != nil {
		return err
	}
	var retireErr error
	for _, source := range prepared.sources {
		for _, role := range source.roles {
			if role.currentPublicationCancel != nil {
				role.currentPublicationCancel()
			}
			if role.currentPublication != nil {
				role.currentPublication.Cleanup()
			}
			if role.currentShared != nil {
				retireErr = errors.Join(retireErr, role.currentShared.Close())
			}
			if role.currentNotifier != nil {
				retireErr = errors.Join(retireErr, role.currentNotifier.Close())
			}
		}
	}
	prepared.terminal = true
	return retireErr
}

func (prepared *runtimeFanoutPreparedPublications) validatePrivateCandidatesLocked() error {
	for _, source := range prepared.sources {
		for _, role := range source.roles {
			if role.candidatePublication != nil &&
				!role.candidatePublication.Owns(role.candidatePublicationPath) {
				return fmt.Errorf(
					"fan-out source=%q role=%q private socket changed owner",
					role.key.sourceID, role.key.role)
			}
			if role.candidateShared != nil && !role.candidateShared.Owns(role.candidateSharedPath) {
				return fmt.Errorf(
					"fan-out source=%q analysis private shared memory changed owner",
					role.key.sourceID)
			}
		}
	}
	return nil
}

func (prepared *runtimeFanoutPreparedPublications) validateGatesLocked() error {
	for _, source := range prepared.sources {
		for _, role := range source.roles {
			if source.runtime.fanoutRoleGates[role.key.role] != prepared.transactionID {
				return fmt.Errorf(
					"fan-out source=%q role=%q delivery gate changed",
					source.sourceID, role.key.role)
			}
		}
	}
	return nil
}

func (prepared *runtimeFanoutPreparedPublications) candidatePublicationsReadyLocked() (bool, error) {
	for _, move := range prepared.moves {
		role := move.to
		if role.candidatePublication != nil && !role.candidatePublication.Owns(role.candidate.Config.Socket) {
			return false, fmt.Errorf("fan-out stream %q socket lost candidate ownership", move.streamID)
		}
		if role.candidateShared != nil && !role.candidateShared.Owns(role.candidate.Config.SharedMemory) {
			return false, fmt.Errorf("fan-out stream %q shared memory lost candidate ownership", move.streamID)
		}
		if role.runtime.fanoutRoleGates[role.key.role] != "" ||
			!fanoutRuntimePlanMatchesRole(role.runtime.plan, role.key.role, role.candidate) {
			return false, fmt.Errorf("fan-out stream %q runtime publication differs", move.streamID)
		}
		if fanoutRoleFrameCountLocked(role.runtime, role.key.role) <= role.cutoverFrames {
			return false, nil
		}
		switch role.key.role {
		case "live":
			expectedControl := role.runtime.childControlAvailable &&
				role.candidateRuntimeMode == videoproviders.WorkerFanoutRuntimePersistentEncoder
			if role.runtime.childLiveControlMode != role.candidateRuntimeMode ||
				role.runtime.childControlSupported != expectedControl {
				return false, fmt.Errorf(
					"fan-out stream %q live runtime control differs from worker evidence", move.streamID)
			}
			if !brokerMainBootstrapReady(role.runtime.plan.MainFormat, role.runtime.mainBootstrap) {
				return false, nil
			}
		case "archive":
			if role.runtime.observedArchiveCodec != role.runtime.plan.ArchiveCodec ||
				role.runtime.observedArchiveDelivery != role.candidateRuntimeMode ||
				role.runtime.observedArchiveFPS != role.runtime.plan.ArchiveFPS {
				return false, fmt.Errorf(
					"fan-out stream %q archive runtime metadata differs from worker evidence",
					move.streamID)
			}
			if !brokerMainBootstrapReady(role.runtime.plan.ArchiveCodec, role.runtime.archiveBootstrap) {
				return false, nil
			}
		case "analysis":
			if role.candidateShared != nil && role.candidateShared.Epoch() == 0 {
				return false, nil
			}
		}
	}
	return true, nil
}

func (prepared *runtimeFanoutPreparedPublications) lockRuntimes() []*videoBrokerRuntime {
	locked := make([]*videoBrokerRuntime, 0, len(prepared.sources))
	for _, source := range prepared.sources {
		source.runtime.mu.Lock()
		locked = append(locked, source.runtime)
	}
	return locked
}

func unlockFanoutRuntimes(locked []*videoBrokerRuntime) {
	for index := len(locked) - 1; index >= 0; index-- {
		locked[index].mu.Unlock()
	}
}

func (prepared *runtimeFanoutPreparedPublications) cleanupCandidates() error {
	var cleanupErr error
	for _, source := range prepared.sources {
		for _, role := range source.roles {
			if role.candidatePublicationCancel != nil {
				role.candidatePublicationCancel()
			}
			if role.candidatePublication != nil {
				role.candidatePublication.Cleanup()
			}
			if role.candidateShared != nil {
				cleanupErr = errors.Join(cleanupErr, role.candidateShared.Close())
			}
			if role.candidateNotifier != nil {
				cleanupErr = errors.Join(cleanupErr, role.candidateNotifier.Close())
			}
		}
	}
	return cleanupErr
}

func (prepared *runtimeFanoutPreparedPublications) requirePhaseLocked(expected int) error {
	if prepared == nil || prepared.transactionID == "" || prepared.terminal {
		return errors.New("fan-out publication transaction is invalid")
	}
	if prepared.phase != expected {
		return fmt.Errorf(
			"fan-out publication phase order invalid current=%d expected=%d",
			prepared.phase, expected)
	}
	return nil
}

func fanoutRuntimeRoleBindingLocked(
	runtime *videoBrokerRuntime,
	role string,
) (*unixsocket.Publication, context.CancelFunc, *brokerSubSharedFrame, *brokerSubFrameNotifier) {
	switch role {
	case "live":
		return runtime.mainPublication, runtime.mainPublicationCancel, nil, nil
	case "archive":
		return runtime.archivePublication, runtime.archivePublicationCancel, nil, nil
	case "analysis":
		return runtime.subPublication, runtime.subPublicationCancel, runtime.subShared, runtime.subNotifier
	default:
		return nil, nil, nil, nil
	}
}

func setFanoutRuntimeRoleBindingLocked(
	runtime *videoBrokerRuntime,
	role string,
	publication *unixsocket.Publication,
	cancel context.CancelFunc,
	shared *brokerSubSharedFrame,
	notifier *brokerSubFrameNotifier,
) {
	switch role {
	case "live":
		runtime.mainPublication, runtime.mainPublicationCancel = publication, cancel
	case "archive":
		runtime.archivePublication, runtime.archivePublicationCancel = publication, cancel
	case "analysis":
		runtime.subPublication, runtime.subPublicationCancel = publication, cancel
		runtime.subShared, runtime.subNotifier = shared, notifier
	}
}

func setFanoutRuntimePublicationCancelLocked(
	runtime *videoBrokerRuntime,
	role string,
	cancel context.CancelFunc,
) {
	switch role {
	case "live":
		runtime.mainPublicationCancel = cancel
	case "archive":
		runtime.archivePublicationCancel = cancel
	case "analysis":
		runtime.subPublicationCancel = cancel
	}
}

func snapshotFanoutRoleLocked(runtime *videoBrokerRuntime, role string) runtimeFanoutRoleSnapshot {
	snapshot := runtimeFanoutRoleSnapshot{}
	switch role {
	case "live":
		snapshot.mainBootstrap = cloneFanoutPackets(runtime.mainBootstrap)
		snapshot.mainCodecConfig = cloneFanoutPackets(runtime.mainCodecConfig)
		snapshot.mainBootstrapBytes = runtime.mainBootstrapBytes
		snapshot.mainAwaitingEncoderEpoch = runtime.mainAwaitingEncoderEpoch
		snapshot.mainAwaitingKeyframe = runtime.mainAwaitingKeyframe
		snapshot.mainBootstrapRecoveryAt = runtime.mainBootstrapRecoveryAt
		snapshot.activeMainWidth, snapshot.activeMainHeight = runtime.activeMainWidth, runtime.activeMainHeight
		snapshot.mainBitrateKbps = runtime.mainBitrateKbps
		snapshot.childLiveControlMode = runtime.childLiveControlMode
		snapshot.childControlSupported = runtime.childControlSupported
		snapshot.routingLiveDemand = runtime.routingLiveDemand
	case "archive":
		snapshot.archiveBootstrap = cloneFanoutPackets(runtime.archiveBootstrap)
		snapshot.archiveCodecConfig = cloneFanoutPackets(runtime.archiveCodecConfig)
		snapshot.archiveBootstrapBytes = runtime.archiveBootstrapBytes
		snapshot.archiveCodecStreamID = runtime.archiveCodecStreamID
		snapshot.activeArchiveWidth, snapshot.activeArchiveHeight =
			runtime.activeArchiveWidth, runtime.activeArchiveHeight
		snapshot.archiveBitrateKbps = runtime.archiveBitrateKbps
		snapshot.observedArchiveCodec = runtime.observedArchiveCodec
		snapshot.observedArchiveDelivery = runtime.observedArchiveDelivery
		snapshot.observedArchiveFPS = runtime.observedArchiveFPS
	}
	return snapshot
}

func restoreFanoutRoleSnapshotLocked(
	runtime *videoBrokerRuntime,
	role string,
	snapshot runtimeFanoutRoleSnapshot,
) {
	switch role {
	case "live":
		runtime.mainBootstrap = cloneFanoutPackets(snapshot.mainBootstrap)
		runtime.mainCodecConfig = cloneFanoutPackets(snapshot.mainCodecConfig)
		runtime.mainBootstrapBytes = snapshot.mainBootstrapBytes
		runtime.mainAwaitingEncoderEpoch = snapshot.mainAwaitingEncoderEpoch
		runtime.mainAwaitingKeyframe = snapshot.mainAwaitingKeyframe
		runtime.mainBootstrapRecoveryAt = snapshot.mainBootstrapRecoveryAt
		runtime.activeMainWidth, runtime.activeMainHeight =
			snapshot.activeMainWidth, snapshot.activeMainHeight
		runtime.mainBitrateKbps = snapshot.mainBitrateKbps
		runtime.childLiveControlMode = snapshot.childLiveControlMode
		runtime.childControlSupported = snapshot.childControlSupported
		runtime.routingLiveDemand = snapshot.routingLiveDemand
	case "archive":
		runtime.archiveBootstrap = cloneFanoutPackets(snapshot.archiveBootstrap)
		runtime.archiveCodecConfig = cloneFanoutPackets(snapshot.archiveCodecConfig)
		runtime.archiveBootstrapBytes = snapshot.archiveBootstrapBytes
		runtime.archiveCodecStreamID = snapshot.archiveCodecStreamID
		runtime.activeArchiveWidth, runtime.activeArchiveHeight =
			snapshot.activeArchiveWidth, snapshot.activeArchiveHeight
		runtime.archiveBitrateKbps = snapshot.archiveBitrateKbps
		runtime.observedArchiveCodec = snapshot.observedArchiveCodec
		runtime.observedArchiveDelivery = snapshot.observedArchiveDelivery
		runtime.observedArchiveFPS = snapshot.observedArchiveFPS
	}
}

func cloneFanoutPackets(packets [][]byte) [][]byte {
	cloned := make([][]byte, len(packets))
	for index, packet := range packets {
		cloned[index] = append([]byte(nil), packet...)
	}
	return cloned
}

func activateFanoutCandidateRoleLocked(
	role *runtimeFanoutPublicationRole,
	plan videoBrokerPlan,
) {
	stream := role.candidate
	switch role.key.role {
	case "live":
		role.runtime.clearMainBootstrapWithoutKeyframeGateLocked()
		if stream != nil {
			role.runtime.childLiveControlMode = role.candidateRuntimeMode
			role.runtime.activeMainWidth, role.runtime.activeMainHeight =
				stream.Config.Width, stream.Config.Height
			role.runtime.mainBitrateKbps = stream.Config.BitrateKbps
			role.runtime.clearMainBootstrapLocked()
		} else {
			role.runtime.childLiveControlMode = "disabled"
		}
		// A retained capture may have booted without live. The worker's
		// attested replacement mode, not its old bootstrap, determines whether
		// viewer demand and governor caps can now control this encoder.
		role.runtime.childControlSupported = role.runtime.childControlAvailable &&
			role.runtime.childLiveControlMode == videoproviders.WorkerFanoutRuntimePersistentEncoder
		// Keep producing the post-cutover proof frame until Verify completes.
		// This demand is released on verification or restored on rollback;
		// it does not bypass thermal caps.
		if role.runtime.childControlSupported {
			role.runtime.routingLiveDemand = true
		}
	case "archive":
		role.runtime.clearArchiveBootstrapLocked()
		if stream != nil {
			role.runtime.observedArchiveCodec = plan.ArchiveCodec
			role.runtime.observedArchiveDelivery = role.candidateRuntimeMode
			role.runtime.observedArchiveFPS = plan.ArchiveFPS
			role.runtime.activeArchiveWidth, role.runtime.activeArchiveHeight =
				stream.Config.Width, stream.Config.Height
			role.runtime.archiveBitrateKbps = stream.Config.BitrateKbps
		} else {
			role.runtime.observedArchiveCodec = ""
			role.runtime.observedArchiveDelivery = ""
			role.runtime.observedArchiveFPS = 0
			role.runtime.activeArchiveWidth, role.runtime.activeArchiveHeight = 0, 0
			role.runtime.archiveBitrateKbps = 0
		}
	}
}

func disconnectFanoutRoleClientsLocked(runtime *videoBrokerRuntime, role string) {
	switch role {
	case "live":
		for clientID := range runtime.mainClients {
			runtime.detachClientLocked(brokerStreamH264, clientID)
		}
	case "archive":
		for clientID := range runtime.archiveClients {
			runtime.detachClientLocked(brokerEndpointArchive, clientID)
		}
	case "analysis":
		for clientID := range runtime.subClients {
			runtime.detachClientLocked(brokerStreamNV12, clientID)
		}
	}
}

func setFanoutRoleGateLocked(runtime *videoBrokerRuntime, role, transactionID string) error {
	if runtime.fanoutRoleGates == nil {
		runtime.fanoutRoleGates = make(map[string]string, len(fanoutPublicationRoles))
	}
	if owner := runtime.fanoutRoleGates[role]; owner != "" && owner != transactionID {
		return fmt.Errorf("fan-out role gate is owned by transaction %q", owner)
	}
	runtime.fanoutRoleGates[role] = transactionID
	if role == "analysis" {
		runtime.causalObserver.configureLocked(runtime.plan, false)
	}
	return nil
}

func clearFanoutRoleGateLocked(runtime *videoBrokerRuntime, role, transactionID string) error {
	if owner := runtime.fanoutRoleGates[role]; owner != transactionID {
		return fmt.Errorf("fan-out role gate owner changed to %q", owner)
	}
	delete(runtime.fanoutRoleGates, role)
	if role == "analysis" {
		runtime.causalObserver.configureLocked(runtime.plan, true)
	}
	return nil
}

func workerFanoutRoleForMuxStream(streamID byte) string {
	switch streamID {
	case brokerStreamH264:
		return "live"
	case brokerStreamNV12:
		return "analysis"
	case brokerStreamH265, brokerMuxStreamArchiveH264Native, brokerMuxStreamArchiveH264Generated:
		return "archive"
	default:
		return ""
	}
}

func fanoutEndpointForRole(role string) byte {
	switch role {
	case "live":
		return brokerStreamH264
	case "archive":
		return brokerEndpointArchive
	case "analysis":
		return brokerStreamNV12
	default:
		return 0
	}
}

func fanoutRuntimeRoleEnabled(plan videoBrokerPlan, role string) bool {
	switch role {
	case "live":
		return plan.LiveEnabled
	case "archive":
		return plan.ArchiveEnabled
	case "analysis":
		return plan.AnalysisEnabled
	default:
		return false
	}
}

func fanoutRoleFrameCountLocked(runtime *videoBrokerRuntime, role string) uint64 {
	switch role {
	case "live":
		return runtime.totalMainFrames
	case "archive":
		return runtime.totalArchiveFrames
	case "analysis":
		return runtime.totalSubFrames
	default:
		return 0
	}
}

var _ fanoutPublicationCoordinator = (*runtimeFanoutPublicationCoordinator)(nil)
var _ fanoutPreparedPublications = (*runtimeFanoutPreparedPublications)(nil)
