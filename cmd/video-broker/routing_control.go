package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"time"

	videobroker "github.com/Josepavese/videosluice"
	"github.com/Josepavese/videosluice/config"
	"github.com/Josepavese/videosluice/internal/unixsocket"
	"github.com/Josepavese/videosluice/routing"
)

const (
	routingControlMaxRequestBytes = 64 * 1024
	routingControlMaxClients      = 8
	routingControlReadTimeout     = 2 * time.Second
	routingControlWriteTimeout    = 2 * time.Second
	routingControlActionTimeout   = 90 * time.Second
)

var errRoutingAuthorityDiverged = errors.New("running and persisted routing authority differ")

var (
	errRoutingOperationBusy     = errors.New("a routing operation is already running")
	errRoutingOperationExists   = errors.New("routing operation id already exists")
	errRoutingOperationNotFound = errors.New("routing operation was not found")
)

func validateRoutingControlPath(path string, cfg config.Config, reservedPaths ...string) error {
	path = filepath.Clean(strings.TrimSpace(path))
	if !filepath.IsAbs(path) || path == "/" {
		return errors.New("routing control socket must be an absolute non-root path")
	}
	reserved := append([]string{
		cfg.Runtime.RuntimeDirectory,
		cfg.Runtime.StateDirectory,
		cfg.Runtime.EngineDirectory,
		cfg.Runtime.Status.Socket,
		cfg.Runtime.Governor.StatePath,
	}, reservedPaths...)
	for _, source := range cfg.Sources {
		reserved = append(reserved,
			source.Device,
			source.Processing.Quality.StatePath,
			source.Processing.Quality.AdmissionPath,
		)
	}
	for _, stream := range cfg.Streams {
		reserved = append(reserved, stream.Socket, stream.SharedMemory, stream.NotifySocket)
	}
	if cfg.Runtime.FrameAccess != nil {
		for _, reader := range cfg.Runtime.FrameAccess.Readers {
			reserved = append(reserved, reader.DeliverySocket, reader.ROISocket)
		}
	}
	if cfg.Runtime.Governor.Enabled {
		for streamID := range cfg.Streams {
			path, err := config.StreamGovernorStatePath(cfg.Runtime.Governor.StatePath, streamID)
			if err != nil {
				return err
			}
			reserved = append(reserved, path)
		}
	}
	for _, candidate := range reserved {
		candidate = strings.TrimSpace(candidate)
		if candidate != "" && filepath.Clean(candidate) == path {
			return fmt.Errorf("routing control socket collides with graph path %q", path)
		}
	}
	return nil
}

type routingController interface {
	RoutingPlan() (routing.Plan, error)
	Snapshot(time.Time) videobroker.Status
	PreviewRouting(context.Context, routing.Catalog, *routing.Store, uint64, map[string]string, ...map[string]string) (routing.Preview, error)
	ApplyRoutingObserved(context.Context, routing.Catalog, *routing.Store, uint64, map[string]string, routing.PhaseObserver, ...map[string]string) (routing.Plan, error)
	RollbackRoutingObserved(context.Context, routing.Catalog, *routing.Store, uint64, routing.PhaseObserver) (routing.Plan, error)
}

func serveRoutingControl(
	ctx context.Context,
	path string,
	catalog routing.Catalog,
	store *routing.Store,
	controller routingController,
) error {
	if ctx == nil || store == nil || controller == nil {
		return errors.New("routing control server dependencies are incomplete")
	}
	normalizedCatalog, err := routing.NormalizeCatalog(catalog)
	if err != nil {
		return err
	}
	catalog = normalizedCatalog
	catalogView, err := routing.ProjectControlCatalog(catalog)
	if err != nil {
		return err
	}
	serverCtx, cancelServer := context.WithCancel(ctx)
	operations := newRoutingOperationManager(serverCtx, catalog, store, controller)
	listener, err := unixsocket.ListenSingleOwner(path)
	if err != nil {
		cancelServer()
		return err
	}
	var workers sync.WaitGroup
	defer func() {
		cancelServer()
		listener.Cleanup()
		workers.Wait()
		operations.Wait()
	}()
	go func() {
		<-serverCtx.Done()
		_ = listener.Close()
	}()
	clients := make(chan struct{}, routingControlMaxClients)
	for {
		connection, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		select {
		case clients <- struct{}{}:
			workers.Add(1)
			go func() {
				defer workers.Done()
				defer func() { <-clients }()
				handleRoutingControlConnection(
					serverCtx, connection, catalog, catalogView, store, controller, operations)
			}()
		default:
			writeRoutingControlResponse(connection, routing.ControlResponse{
				Schema: routing.ControlSchema, OK: false,
				Error: &routing.ControlError{Code: "busy", Message: "routing control is busy"},
			})
			_ = connection.Close()
		}
	}
}

func handleRoutingControlConnection(
	ctx context.Context,
	connection net.Conn,
	catalog routing.Catalog,
	catalogView routing.ControlCatalog,
	store *routing.Store,
	controller routingController,
	operations *routingOperationManager,
) {
	defer connection.Close()
	_ = connection.SetReadDeadline(time.Now().Add(routingControlReadTimeout))
	request, err := decodeRoutingControlRequest(connection)
	if err != nil {
		writeRoutingControlResponse(connection, routing.ControlResponse{
			Schema: routing.ControlSchema, OK: false,
			Error: &routing.ControlError{Code: "invalid_request", Message: "routing request is invalid"},
		})
		return
	}
	_ = connection.SetReadDeadline(time.Time{})
	actionCtx, cancel := context.WithTimeout(ctx, routingControlActionTimeout)
	defer cancel()
	response := executeRoutingControl(
		actionCtx, request, catalog, catalogView, store, controller, operations)
	if !response.OK {
		log.Printf(
			"[video-broker] event=routing_control_rejected request_id=%s action=%s code=%s phase=%s",
			request.RequestID, request.Action, response.Error.Code, response.Error.Phase)
	}
	writeRoutingControlResponse(connection, response)
}

func decodeRoutingControlRequest(reader io.Reader) (routing.ControlRequest, error) {
	var request routing.ControlRequest
	buffered := bufio.NewReaderSize(reader, routingControlMaxRequestBytes+1)
	body, err := buffered.ReadSlice('\n')
	if errors.Is(err, bufio.ErrBufferFull) || len(body) > routingControlMaxRequestBytes {
		return request, errors.New("routing request exceeds size limit")
	}
	if errors.Is(err, io.EOF) {
		return request, errors.New("routing request is not newline terminated")
	}
	if err != nil {
		return request, err
	}
	if len(bytes.TrimSpace(body)) == 0 {
		return request, errors.New("routing request is empty")
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		return request, err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return request, errors.New("routing request contains multiple values")
		}
		return request, err
	}
	if err := routing.ValidateControlRequest(request); err != nil {
		return request, err
	}
	return request, nil
}

func executeRoutingControl(
	ctx context.Context,
	request routing.ControlRequest,
	catalog routing.Catalog,
	catalogView routing.ControlCatalog,
	store *routing.Store,
	controller routingController,
	operations *routingOperationManager,
) routing.ControlResponse {
	response := routing.ControlResponse{
		Schema: routing.ControlSchema, RequestID: request.RequestID,
	}
	var err error
	switch request.Action {
	case routing.ControlActionRead:
		err = operations.ReadIdle(func() error {
			plan, err := readRoutingAuthority(catalog, store, controller)
			if err != nil {
				return err
			}
			response.Plan = &plan
			response.Catalog = &catalogView
			response.Runtime = projectRoutingRuntime(catalogView, plan, controller.Snapshot(time.Now()))
			return nil
		})
	case routing.ControlActionPreview:
		err = operations.ReadIdle(func() error {
			preview, err := controller.PreviewRouting(
				ctx, catalog, store, request.ExpectedRevision, request.Selections, request.SourceSelections)
			if err != nil {
				return err
			}
			response.Preview = &preview
			return nil
		})
	case routing.ControlActionApply:
		var operation routing.ControlOperation
		operation, err = operations.Start(request)
		if err == nil {
			response.Operation = &operation
		}
	case routing.ControlActionRollback:
		var operation routing.ControlOperation
		operation, err = operations.Start(request)
		if err == nil {
			response.Operation = &operation
		}
	case routing.ControlActionProgress:
		var operation routing.ControlOperation
		operation, err = operations.Read(request.OperationID)
		if err == nil {
			response.Operation = &operation
		}
	default:
		err = errors.New("unsupported routing control action")
	}
	if err != nil {
		response.Error = safeRoutingControlError(err)
		logRoutingControlFailure(request, err)
		return response
	}
	response.OK = true
	return response
}

func projectRoutingRuntime(catalog routing.ControlCatalog, plan routing.Plan, status videobroker.Status) map[string]routing.ControlStreamRuntime {
	result := make(map[string]routing.ControlStreamRuntime, len(plan.Selections))
	for streamID, selection := range plan.Selections {
		option := catalog.Streams[streamID].Options[selection]
		item := routing.ControlStreamRuntime{
			SourceRef: option.SourceRef, Generation: plan.StreamGenerations[streamID],
			State: "unavailable", RequestedFPS: option.FPS, TargetFPS: option.FPS,
		}
		if stream, exists := status.Streams[streamID]; exists && stream.SourceRef == option.SourceRef {
			performance := stream.Performance
			item.Available = status.Sources[stream.SourceRef].State == "running"
			if item.Available && performance.State != "" {
				item.State = performance.State
			}
			item.TargetFPS, item.ObservedFPS = performance.TargetFPS, performance.ObservedFPS
			item.WindowMS, item.MeasuredAt = performance.WindowMS, performance.MeasuredAt
		}
		result[streamID] = item
	}
	return result
}

func readRoutingAuthority(
	catalog routing.Catalog,
	store *routing.Store,
	controller routingController,
) (routing.Plan, error) {
	persisted, err := store.Read(catalog)
	if err != nil {
		return routing.Plan{}, err
	}
	running, err := controller.RoutingPlan()
	if err != nil {
		return routing.Plan{}, err
	}
	if !reflect.DeepEqual(persisted, running) {
		return routing.Plan{}, errRoutingAuthorityDiverged
	}
	return running, nil
}

// Operator-local diagnostics retain the underlying validation cause; the
// public reply still contains only the safe projection below. Quote controls
// and bound text so a failed request cannot forge log records or dump a graph.
func logRoutingControlFailure(request routing.ControlRequest, err error) {
	const maxCauseBytes = 1024
	cause := err.Error()
	truncated := len(cause) > maxCauseBytes
	if truncated {
		cause = cause[:maxCauseBytes]
	}
	log.Printf("[video-broker] event=routing_control_failure request_id=%q action=%q expected_revision=%d error_type=%T cause=%q truncated=%t",
		request.RequestID, request.Action, request.ExpectedRevision, err, cause, truncated)
}

func safeRoutingControlError(err error) *routing.ControlError {
	controlErr := &routing.ControlError{Code: "rejected", Message: "routing operation was rejected"}
	switch {
	case errors.Is(err, routing.ErrStaleRevision):
		controlErr.Code = "stale_revision"
		controlErr.Message = "routing plan changed; refresh and retry"
	case errors.Is(err, routing.ErrNoChange):
		controlErr.Code = "no_change"
		controlErr.Message = "routing selections are unchanged"
	case errors.Is(err, routing.ErrNoRollback):
		controlErr.Code = "no_rollback"
		controlErr.Message = "no previous routing selection is available"
	case errors.Is(err, errRoutingAuthorityDiverged):
		controlErr.Code = "authority_diverged"
		controlErr.Message = "running routing state requires reconciliation"
	case errors.Is(err, errRoutingOperationBusy):
		controlErr.Code = "busy"
		controlErr.Message = "another routing change is still running"
	case errors.Is(err, errRoutingOperationExists):
		controlErr.Code = "duplicate_request"
		controlErr.Message = "routing operation id was already used"
	case errors.Is(err, errRoutingOperationNotFound):
		controlErr.Code = "operation_not_found"
		controlErr.Message = "routing operation is not available"
	default:
		var applyErr *routing.ApplyError
		if errors.As(err, &applyErr) {
			controlErr.Code = "apply_failed"
			controlErr.Message = "routing change failed and was not accepted"
			controlErr.Phase = applyErr.Phase
		}
	}
	return controlErr
}

func writeRoutingControlResponse(connection net.Conn, response routing.ControlResponse) {
	_ = connection.SetWriteDeadline(time.Now().Add(routingControlWriteTimeout))
	if err := json.NewEncoder(connection).Encode(response); err != nil {
		log.Printf("[video-broker] routing control response failed: %v", err)
	}
}

var _ routingController = (*videobroker.Service)(nil)
