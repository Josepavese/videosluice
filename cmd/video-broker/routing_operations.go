package main

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/Josepavese/videosluice/routing"
)

const routingControlOperationHistory = 32

// routingOperationManager detaches a bounded apply/rollback transaction from
// the short-lived client connection. It retains a small in-memory history for
// progress polling and permits exactly one mutation at a time. Store CAS and
// locking remain the durability authority.
type routingOperationManager struct {
	ctx        context.Context
	catalog    routing.Catalog
	store      *routing.Store
	controller routingController

	mu         sync.Mutex
	activeID   string
	operations map[string]routing.ControlOperation
	order      []string
	workers    sync.WaitGroup
}

func newRoutingOperationManager(
	ctx context.Context,
	catalog routing.Catalog,
	store *routing.Store,
	controller routingController,
) *routingOperationManager {
	return &routingOperationManager{
		ctx: ctx, catalog: catalog, store: store, controller: controller,
		operations: make(map[string]routing.ControlOperation),
	}
}

func (manager *routingOperationManager) Start(
	request routing.ControlRequest,
) (routing.ControlOperation, error) {
	if manager == nil || manager.store == nil || manager.controller == nil {
		return routing.ControlOperation{}, errors.New("routing operation manager is unavailable")
	}
	select {
	case <-manager.ctx.Done():
		return routing.ControlOperation{}, manager.ctx.Err()
	default:
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if _, exists := manager.operations[request.RequestID]; exists {
		return routing.ControlOperation{}, errRoutingOperationExists
	}
	if manager.activeID != "" {
		return routing.ControlOperation{}, errRoutingOperationBusy
	}
	operation := routing.ControlOperation{
		OperationID:      request.RequestID,
		Action:           request.Action,
		State:            routing.ControlOperationRunning,
		Phase:            routing.ApplyPhaseValidate,
		ExpectedRevision: request.ExpectedRevision,
		StartedAt:        time.Now().UTC().Format(time.RFC3339Nano),
	}
	manager.operations[operation.OperationID] = operation
	manager.order = append(manager.order, operation.OperationID)
	manager.activeID = operation.OperationID
	manager.pruneLocked()

	request.Selections = cloneRoutingSelections(request.Selections)
	request.SourceSelections = cloneRoutingSelections(request.SourceSelections)
	manager.workers.Add(1)
	go manager.run(request)
	return cloneRoutingOperation(operation), nil
}

func (manager *routingOperationManager) Read(operationID string) (routing.ControlOperation, error) {
	if manager == nil {
		return routing.ControlOperation{}, errRoutingOperationNotFound
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	operation, exists := manager.operations[operationID]
	if !exists {
		return routing.ControlOperation{}, errRoutingOperationNotFound
	}
	return cloneRoutingOperation(operation), nil
}

// ReadIdle excludes Start for the complete read, closing the check/use race
// between an idle check and RoutingPlan's identity lock. Active mutations fail
// fast; progress polling remains available through Read(operationID).
func (manager *routingOperationManager) ReadIdle(read func() error) error {
	if manager == nil {
		return errors.New("routing operation manager is unavailable")
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if manager.activeID != "" {
		return errRoutingOperationBusy
	}
	return read()
}

func (manager *routingOperationManager) Wait() {
	if manager != nil {
		manager.workers.Wait()
	}
}

func (manager *routingOperationManager) run(request routing.ControlRequest) {
	defer manager.workers.Done()
	operationCtx, cancel := context.WithTimeout(manager.ctx, routingControlActionTimeout)
	defer cancel()
	observe := func(phase string) {
		manager.mu.Lock()
		operation := manager.operations[request.RequestID]
		operation.Phase = phase
		manager.operations[request.RequestID] = operation
		manager.mu.Unlock()
	}

	var plan routing.Plan
	var err error
	switch request.Action {
	case routing.ControlActionApply:
		plan, err = manager.controller.ApplyRoutingObserved(
			operationCtx, manager.catalog, manager.store,
			request.ExpectedRevision, request.Selections, observe, request.SourceSelections)
	case routing.ControlActionRollback:
		plan, err = manager.controller.RollbackRoutingObserved(
			operationCtx, manager.catalog, manager.store,
			request.ExpectedRevision, observe)
	default:
		err = errors.New("unsupported asynchronous routing action")
	}

	manager.mu.Lock()
	operation := manager.operations[request.RequestID]
	operation.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
	if plan.Revision != 0 {
		cloned := cloneRoutingPlan(plan)
		operation.Plan = &cloned
	}
	if err == nil {
		operation.State = routing.ControlOperationSucceeded
		operation.Phase = "complete"
	} else {
		operation.State = routing.ControlOperationFailed
		if plan.Revision != 0 {
			operation.State = routing.ControlOperationDegraded
		}
		operation.Error = safeRoutingControlError(err)
	}
	manager.operations[request.RequestID] = operation
	if manager.activeID == request.RequestID {
		manager.activeID = ""
	}
	manager.pruneLocked()
	manager.mu.Unlock()

	if err != nil {
		logRoutingControlFailure(request, err)
		log.Printf(
			"[video-broker] event=routing_operation_completed operation_id=%s action=%s state=%s phase=%s code=%s",
			operation.OperationID, operation.Action, operation.State,
			operation.Phase, operation.Error.Code)
	} else {
		log.Printf(
			"[video-broker] event=routing_operation_completed operation_id=%s action=%s state=%s revision=%d",
			operation.OperationID, operation.Action, operation.State, plan.Revision)
	}
}

func (manager *routingOperationManager) pruneLocked() {
	for len(manager.order) > routingControlOperationHistory {
		oldest := manager.order[0]
		if oldest == manager.activeID {
			return
		}
		manager.order = manager.order[1:]
		delete(manager.operations, oldest)
	}
}

func cloneRoutingSelections(input map[string]string) map[string]string {
	if input == nil {
		return nil
	}
	output := make(map[string]string, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}

func cloneRoutingPlan(input routing.Plan) routing.Plan {
	return routing.Plan{
		Schema: input.Schema, CatalogRevision: input.CatalogRevision,
		Revision:                 input.Revision,
		Selections:               cloneRoutingSelections(input.Selections),
		StreamGenerations:        cloneRoutingGenerations(input.StreamGenerations),
		RollbackSelections:       cloneRoutingSelections(input.RollbackSelections),
		SourceSelections:         cloneRoutingSelections(input.SourceSelections),
		RollbackSourceSelections: cloneRoutingSelections(input.RollbackSourceSelections),
	}
}

func cloneRoutingGenerations(input map[string]uint64) map[string]uint64 {
	if input == nil {
		return nil
	}
	output := make(map[string]uint64, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}

func cloneRoutingOperation(input routing.ControlOperation) routing.ControlOperation {
	output := input
	if input.Plan != nil {
		plan := cloneRoutingPlan(*input.Plan)
		output.Plan = &plan
	}
	if input.Error != nil {
		errorCopy := *input.Error
		output.Error = &errorCopy
	}
	return output
}
