package source

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strings"
)

const (
	WorkerFanoutCommandSchema = "video-broker.worker-fanout-command.v1"
	WorkerFanoutReceiptSchema = "video-broker.worker-fanout-receipt.v1"
	WorkerFanoutMaxLineBytes  = 4096

	WorkerFanoutPhasePrepare    = "prepare"
	WorkerFanoutPhaseWarmup     = "warmup"
	WorkerFanoutPhaseAdmit      = "admit"
	WorkerFanoutPhaseFirstFrame = "first_frame"
	WorkerFanoutPhaseCutover    = "cutover"
	WorkerFanoutPhaseVerify     = "verify"
	WorkerFanoutPhaseRollback   = "rollback"
	WorkerFanoutPhaseAbort      = "abort"
	WorkerFanoutPhaseRetire     = "retire"

	WorkerFanoutActionAttach  = "attach"
	WorkerFanoutActionDetach  = "detach"
	WorkerFanoutActionReplace = "replace"

	WorkerFanoutEventPrepared   = "prepared"
	WorkerFanoutEventWarmed     = "warmed"
	WorkerFanoutEventAdmitted   = "admitted"
	WorkerFanoutEventFirstFrame = "first_frame"
	WorkerFanoutEventCutover    = "cutover"
	WorkerFanoutEventVerified   = "verified"
	WorkerFanoutEventRolledBack = "rolled_back"
	WorkerFanoutEventAborted    = "aborted"
	WorkerFanoutEventRetired    = "retired"
	WorkerFanoutEventRejected   = "rejected"

	WorkerFanoutBootstrapComplete      = "complete"
	WorkerFanoutBootstrapNotApplicable = "not_applicable"

	WorkerFanoutRuntimeNativePassthrough = "native_passthrough"
	WorkerFanoutRuntimeSourceHWEncode    = "source_hardware_encode"
	WorkerFanoutRuntimeProcessedHWEncode = "processed_hardware_encode"
	WorkerFanoutRuntimeSoftwareEncode    = "software_encode"
	WorkerFanoutRuntimePersistentEncoder = "persistent_encoder_epoch"
	WorkerFanoutRuntimeNativeStatic      = "native_passthrough_static"
	WorkerFanoutRuntimeBackendStatic     = "backend_static"

	WorkerFanoutControlDisabled            = "disabled"
	WorkerFanoutControlBranchTransactionV1 = "branch_transaction_v1"
)

var (
	workerFanoutIDPattern   = regexp.MustCompile(`^[a-z0-9][a-z0-9._:-]{0,127}$`)
	workerFanoutCodePattern = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)
)

// WorkerFanoutCommand is the private broker-to-capture-worker transaction
// envelope. A prepare command contains only the changed output branches. It
// deliberately cannot carry a source device, endpoint pathname, provider
// choice or application identity: those remain in the already validated graph
// and capability plan owned by the broker.
type WorkerFanoutCommand struct {
	Schema        string               `json:"schema"`
	TransactionID string               `json:"transaction_id"`
	Sequence      uint64               `json:"sequence"`
	SourceID      string               `json:"source_id"`
	CapturePID    int                  `json:"capture_pid"`
	MediaEpoch    uint64               `json:"media_epoch"`
	Phase         string               `json:"phase"`
	Changes       []WorkerBranchChange `json:"changes,omitempty"`
}

type WorkerBranchChange struct {
	Role    string               `json:"role"`
	Action  string               `json:"action"`
	Profile *WorkerBranchProfile `json:"profile,omitempty"`
}

// WorkerBranchProfile contains the concrete, planner-resolved media contract
// for one candidate branch. Hardware components are not selected here: their
// exact staged lease is supplied by the capability allocator.
type WorkerBranchProfile struct {
	Codec       string `json:"codec,omitempty"`
	PixelFormat string `json:"pixel_format,omitempty"`
	Width       int    `json:"width"`
	Height      int    `json:"height"`
	FPS         int    `json:"fps"`
	BitrateKbps int    `json:"bitrate_kbps,omitempty"`
	Content     string `json:"content,omitempty"`
	RateControl string `json:"rate_control,omitempty"`
}

// WorkerFanoutReceipt is emitted only by the exact capture child that received
// the command. The broker additionally binds the stderr pipe observation to
// its own PID and media epoch; echoing these values alone is never admission.
type WorkerFanoutReceipt struct {
	Schema        string                      `json:"schema"`
	TransactionID string                      `json:"transaction_id"`
	Sequence      uint64                      `json:"sequence"`
	SourceID      string                      `json:"source_id"`
	CapturePID    int                         `json:"capture_pid"`
	MediaEpoch    uint64                      `json:"media_epoch"`
	Event         string                      `json:"event"`
	Branches      []WorkerFanoutBranchReceipt `json:"branches,omitempty"`
	ErrorCode     string                      `json:"error_code,omitempty"`
}

type WorkerFanoutBranchReceipt struct {
	Role           string `json:"role"`
	BranchEpoch    uint64 `json:"branch_epoch"`
	FrameID        uint64 `json:"frame_id"`
	CodecBootstrap string `json:"codec_bootstrap"`
	RuntimeMode    string `json:"runtime_mode"`
}

// WorkerFanoutRejectionError reports a canonical worker rejection without
// admitting worker-supplied free text into broker control or public status.
type WorkerFanoutRejectionError struct {
	ErrorCode string
}

func (rejection *WorkerFanoutRejectionError) Error() string {
	return "worker fan-out rejected: " + rejection.ErrorCode
}

func EncodeWorkerFanoutCommand(command WorkerFanoutCommand) ([]byte, error) {
	if err := ValidateWorkerFanoutCommand(command); err != nil {
		return nil, err
	}
	return encodeWorkerFanoutLine(command)
}

func DecodeWorkerFanoutCommand(line []byte) (WorkerFanoutCommand, error) {
	var command WorkerFanoutCommand
	if err := decodeWorkerFanoutLine(line, &command); err != nil {
		return command, err
	}
	return command, ValidateWorkerFanoutCommand(command)
}

func ValidateWorkerFanoutCommand(command WorkerFanoutCommand) error {
	if command.Schema != WorkerFanoutCommandSchema {
		return fmt.Errorf("unsupported worker fan-out command schema=%q", command.Schema)
	}
	if err := validateWorkerFanoutIdentity(
		command.TransactionID, command.SourceID, command.Sequence,
		command.CapturePID, command.MediaEpoch,
	); err != nil {
		return err
	}
	if !workerFanoutPhaseAllowed(command.Phase) {
		return fmt.Errorf("unsupported worker fan-out phase=%q", command.Phase)
	}
	if command.Phase != WorkerFanoutPhasePrepare {
		if len(command.Changes) != 0 {
			return errors.New("only a worker fan-out prepare command may contain branch changes")
		}
		return nil
	}
	if len(command.Changes) == 0 || len(command.Changes) > 3 {
		return errors.New("worker fan-out prepare requires one to three branch changes")
	}
	previousRole := ""
	for _, change := range command.Changes {
		if !workerFanoutRoleAllowed(change.Role) || change.Role <= previousRole {
			return errors.New("worker fan-out branch roles must be unique and canonically sorted")
		}
		previousRole = change.Role
		switch change.Action {
		case WorkerFanoutActionDetach:
			if change.Profile != nil {
				return fmt.Errorf("detached worker fan-out role %q cannot carry a profile", change.Role)
			}
		case WorkerFanoutActionAttach, WorkerFanoutActionReplace:
			if change.Profile == nil {
				return fmt.Errorf("worker fan-out role %q requires a concrete profile", change.Role)
			}
			if err := validateWorkerBranchProfile(change.Role, *change.Profile); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unsupported worker fan-out action=%q", change.Action)
		}
	}
	return nil
}

func EncodeWorkerFanoutReceipt(receipt WorkerFanoutReceipt) ([]byte, error) {
	if err := ValidateWorkerFanoutReceipt(receipt); err != nil {
		return nil, err
	}
	return encodeWorkerFanoutLine(receipt)
}

func DecodeWorkerFanoutReceipt(line []byte) (WorkerFanoutReceipt, error) {
	var receipt WorkerFanoutReceipt
	if err := decodeWorkerFanoutLine(line, &receipt); err != nil {
		return receipt, err
	}
	return receipt, ValidateWorkerFanoutReceipt(receipt)
}

func ValidateWorkerFanoutReceipt(receipt WorkerFanoutReceipt) error {
	if receipt.Schema != WorkerFanoutReceiptSchema {
		return fmt.Errorf("unsupported worker fan-out receipt schema=%q", receipt.Schema)
	}
	if err := validateWorkerFanoutIdentity(
		receipt.TransactionID, receipt.SourceID, receipt.Sequence,
		receipt.CapturePID, receipt.MediaEpoch,
	); err != nil {
		return err
	}
	if !workerFanoutEventAllowed(receipt.Event) {
		return fmt.Errorf("unsupported worker fan-out event=%q", receipt.Event)
	}
	if receipt.Event == WorkerFanoutEventRejected {
		if !workerFanoutCodePattern.MatchString(receipt.ErrorCode) || len(receipt.Branches) != 0 {
			return errors.New("rejected worker fan-out receipt requires one canonical error code")
		}
		return nil
	}
	if receipt.ErrorCode != "" {
		return errors.New("successful worker fan-out receipt cannot carry an error code")
	}
	if receipt.Event != WorkerFanoutEventFirstFrame {
		if len(receipt.Branches) != 0 {
			return errors.New("only first-frame worker fan-out receipt may contain branch evidence")
		}
		return nil
	}
	if len(receipt.Branches) > 3 {
		return errors.New("worker fan-out first-frame receipt has too much branch evidence")
	}
	previousRole := ""
	for _, branch := range receipt.Branches {
		if !workerFanoutRoleAllowed(branch.Role) || branch.Role <= previousRole ||
			branch.BranchEpoch == 0 || branch.FrameID == 0 ||
			(branch.CodecBootstrap != WorkerFanoutBootstrapComplete &&
				branch.CodecBootstrap != WorkerFanoutBootstrapNotApplicable) {
			return errors.New("worker fan-out branch receipt is not canonical")
		}
		if branch.Role == "analysis" && branch.CodecBootstrap != WorkerFanoutBootstrapNotApplicable {
			return errors.New("analysis worker fan-out receipt cannot claim codec bootstrap")
		}
		if branch.Role != "analysis" && branch.CodecBootstrap != WorkerFanoutBootstrapComplete {
			return errors.New("encoded worker fan-out receipt requires complete codec bootstrap")
		}
		switch branch.Role {
		case "analysis":
			if branch.RuntimeMode != "" {
				return errors.New("analysis worker fan-out receipt cannot claim an encoded runtime mode")
			}
		case "archive":
			if !workerFanoutArchiveRuntimeModeAllowed(branch.RuntimeMode) {
				return errors.New("archive worker fan-out receipt has an invalid runtime mode")
			}
		case "live":
			if !workerFanoutLiveRuntimeModeAllowed(branch.RuntimeMode) {
				return errors.New("live worker fan-out receipt has an invalid runtime mode")
			}
		}
		previousRole = branch.Role
	}
	return nil
}

func workerFanoutArchiveRuntimeModeAllowed(mode string) bool {
	switch mode {
	case WorkerFanoutRuntimeNativePassthrough,
		WorkerFanoutRuntimeSourceHWEncode,
		WorkerFanoutRuntimeProcessedHWEncode,
		WorkerFanoutRuntimeSoftwareEncode:
		return true
	default:
		return false
	}
}

func workerFanoutLiveRuntimeModeAllowed(mode string) bool {
	switch mode {
	case WorkerFanoutRuntimePersistentEncoder,
		WorkerFanoutRuntimeNativeStatic,
		WorkerFanoutRuntimeBackendStatic:
		return true
	default:
		return false
	}
}

// ValidateWorkerFanoutFirstFrameEvidence binds first-frame readiness to the
// candidate branches declared by the transaction's prepare phase. A detach-only
// source has no candidate branch and therefore must return no branch evidence.
func ValidateWorkerFanoutFirstFrameEvidence(
	receipt WorkerFanoutReceipt,
	preparedChanges []WorkerBranchChange,
) error {
	if err := ValidateWorkerFanoutReceipt(receipt); err != nil {
		return err
	}
	if receipt.Event != WorkerFanoutEventFirstFrame {
		return errors.New("worker fan-out receipt is not first-frame evidence")
	}
	probe := WorkerFanoutCommand{
		Schema: WorkerFanoutCommandSchema, TransactionID: receipt.TransactionID,
		Sequence: 1, SourceID: receipt.SourceID, CapturePID: receipt.CapturePID,
		MediaEpoch: receipt.MediaEpoch, Phase: WorkerFanoutPhasePrepare,
		Changes: CanonicalizeWorkerFanoutChanges(preparedChanges),
	}
	if err := ValidateWorkerFanoutCommand(probe); err != nil {
		return fmt.Errorf("worker fan-out prepared changes: %w", err)
	}
	expectedRoles := make([]string, 0, len(preparedChanges))
	for _, change := range probe.Changes {
		if change.Action == WorkerFanoutActionAttach || change.Action == WorkerFanoutActionReplace {
			expectedRoles = append(expectedRoles, change.Role)
		}
	}
	if len(receipt.Branches) != len(expectedRoles) {
		return errors.New("worker fan-out first-frame evidence differs from candidate branches")
	}
	for index, role := range expectedRoles {
		if receipt.Branches[index].Role != role {
			return errors.New("worker fan-out first-frame evidence differs from candidate branches")
		}
	}
	return nil
}

// MatchWorkerFanoutReceipt binds a parsed receipt to the exact command written
// to the exact child pipe. It rejects a valid receipt from another source,
// process, media epoch, transaction or sequence and also prevents an event from
// satisfying the wrong transaction phase.
func MatchWorkerFanoutReceipt(
	command WorkerFanoutCommand,
	receipt WorkerFanoutReceipt,
) error {
	if err := ValidateWorkerFanoutCommand(command); err != nil {
		return fmt.Errorf("worker fan-out command fence: %w", err)
	}
	if err := ValidateWorkerFanoutReceipt(receipt); err != nil {
		return fmt.Errorf("worker fan-out receipt fence: %w", err)
	}
	if receipt.TransactionID != command.TransactionID || receipt.Sequence != command.Sequence ||
		receipt.SourceID != command.SourceID || receipt.CapturePID != command.CapturePID ||
		receipt.MediaEpoch != command.MediaEpoch {
		return errors.New("worker fan-out receipt identity differs from command fence")
	}
	if receipt.Event == WorkerFanoutEventRejected {
		return &WorkerFanoutRejectionError{ErrorCode: receipt.ErrorCode}
	}
	validEvent := false
	switch command.Phase {
	case WorkerFanoutPhasePrepare:
		validEvent = receipt.Event == WorkerFanoutEventPrepared
	case WorkerFanoutPhaseWarmup:
		validEvent = receipt.Event == WorkerFanoutEventWarmed
	case WorkerFanoutPhaseAdmit:
		validEvent = receipt.Event == WorkerFanoutEventAdmitted
	case WorkerFanoutPhaseFirstFrame:
		validEvent = receipt.Event == WorkerFanoutEventFirstFrame
	case WorkerFanoutPhaseCutover:
		validEvent = receipt.Event == WorkerFanoutEventCutover
	case WorkerFanoutPhaseVerify:
		validEvent = receipt.Event == WorkerFanoutEventVerified
	case WorkerFanoutPhaseRollback:
		validEvent = receipt.Event == WorkerFanoutEventRolledBack
	case WorkerFanoutPhaseAbort:
		validEvent = receipt.Event == WorkerFanoutEventAborted
	case WorkerFanoutPhaseRetire:
		validEvent = receipt.Event == WorkerFanoutEventRetired
	}
	if !validEvent {
		return fmt.Errorf(
			"worker fan-out event %q cannot satisfy phase %q", receipt.Event, command.Phase)
	}
	return nil
}

func validateWorkerFanoutIdentity(
	transactionID string,
	sourceID string,
	sequence uint64,
	capturePID int,
	mediaEpoch uint64,
) error {
	if !workerFanoutIDPattern.MatchString(transactionID) || !workerFanoutIDPattern.MatchString(sourceID) {
		return errors.New("worker fan-out identity is not canonical")
	}
	if sequence == 0 || capturePID <= 0 || mediaEpoch == 0 {
		return errors.New("worker fan-out process fence is incomplete")
	}
	return nil
}

func validateWorkerBranchProfile(role string, profile WorkerBranchProfile) error {
	if profile.Width <= 0 || profile.Width > 16384 || profile.Height <= 0 || profile.Height > 16384 ||
		profile.Width%2 != 0 || profile.Height%2 != 0 || profile.FPS <= 0 || profile.FPS > 240 {
		return fmt.Errorf("worker fan-out role %q has an invalid media mode", role)
	}
	switch role {
	case "analysis":
		if profile.PixelFormat != "nv12" || profile.Codec != "" || profile.BitrateKbps != 0 ||
			profile.Content != "" || profile.RateControl != "" {
			return errors.New("analysis worker fan-out profile must be exact NV12")
		}
	case "live":
		if (profile.Codec != "h264" && profile.Codec != "h265") || profile.PixelFormat != "" ||
			profile.BitrateKbps <= 0 || profile.BitrateKbps > 1<<20 ||
			profile.Content != "processed" || profile.RateControl != "cbr" {
			return errors.New("live worker fan-out profile is not a concrete encoded contract")
		}
	case "archive":
		if (profile.Codec != "h264" && profile.Codec != "h265") || profile.PixelFormat != "" ||
			profile.BitrateKbps <= 0 || profile.BitrateKbps > 1<<20 ||
			!oneOfWorkerFanout(profile.Content, "source", "processed", "adaptive") ||
			!oneOfWorkerFanout(profile.RateControl, "cbr", "avbr") {
			return errors.New("archive worker fan-out profile is not a concrete encoded contract")
		}
	default:
		return fmt.Errorf("unsupported worker fan-out role=%q", role)
	}
	return nil
}

func workerFanoutPhaseAllowed(value string) bool {
	return oneOfWorkerFanout(value,
		WorkerFanoutPhasePrepare, WorkerFanoutPhaseWarmup, WorkerFanoutPhaseCutover,
		WorkerFanoutPhaseAdmit, WorkerFanoutPhaseFirstFrame, WorkerFanoutPhaseVerify,
		WorkerFanoutPhaseRollback, WorkerFanoutPhaseAbort, WorkerFanoutPhaseRetire,
	)
}

func workerFanoutEventAllowed(value string) bool {
	return oneOfWorkerFanout(value,
		WorkerFanoutEventPrepared, WorkerFanoutEventWarmed, WorkerFanoutEventAdmitted,
		WorkerFanoutEventFirstFrame, WorkerFanoutEventCutover, WorkerFanoutEventVerified,
		WorkerFanoutEventRolledBack, WorkerFanoutEventAborted, WorkerFanoutEventRetired,
		WorkerFanoutEventRejected,
	)
}

func workerFanoutRoleAllowed(value string) bool {
	return oneOfWorkerFanout(value, "analysis", "archive", "live")
}

func oneOfWorkerFanout(value string, allowed ...string) bool {
	for _, candidate := range allowed {
		if value == candidate {
			return true
		}
	}
	return false
}

func encodeWorkerFanoutLine(value any) ([]byte, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	payload = append(payload, '\n')
	if len(payload) > WorkerFanoutMaxLineBytes {
		return nil, errors.New("worker fan-out envelope exceeds line limit")
	}
	return payload, nil
}

func decodeWorkerFanoutLine(line []byte, target any) error {
	if len(line) == 0 || len(line) > WorkerFanoutMaxLineBytes || line[len(line)-1] != '\n' ||
		bytes.Contains(line[:len(line)-1], []byte{'\n'}) || bytes.Contains(line, []byte{'\r'}) {
		return errors.New("worker fan-out envelope is not one bounded line")
	}
	decoder := json.NewDecoder(bytes.NewReader(line[:len(line)-1]))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("decode worker fan-out envelope: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("worker fan-out envelope contains trailing data")
	}
	return nil
}

// CanonicalizeWorkerFanoutChanges is used by the broker adapter before
// validation and encoding. Workers themselves still reject unsorted input so
// ordering cannot become an implicit compatibility path.
func CanonicalizeWorkerFanoutChanges(changes []WorkerBranchChange) []WorkerBranchChange {
	canonical := append([]WorkerBranchChange(nil), changes...)
	sort.Slice(canonical, func(i, j int) bool { return canonical[i].Role < canonical[j].Role })
	return canonical
}

func WorkerFanoutSanitizedErrorCode(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if !workerFanoutCodePattern.MatchString(value) {
		return "worker_rejected"
	}
	return value
}
