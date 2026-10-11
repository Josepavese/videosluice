package broker

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/Josepavese/videosluice/capability"
)

// capabilityAllocator prevents independent capture owners from silently
// oversubscribing a physical accelerator. Hardware stays single-operation by
// default. Discovery may explicitly attest a finite concurrent-operation
// capacity, while non-exclusive implementations such as CPU SIMD can declare
// themselves shareable. A source keeps its claims across child recovery and
// releases them only when its graph stops.
type capabilityAllocator struct {
	mu           sync.Mutex
	claims       map[string]map[string]int
	reservations map[string]map[string]int
	reservedBy   map[string]map[string]bool
}

func newCapabilityAllocator() *capabilityAllocator {
	return &capabilityAllocator{
		claims:       map[string]map[string]int{},
		reservations: map[string]map[string]int{},
		reservedBy:   map[string]map[string]bool{},
	}
}

func (allocator *capabilityAllocator) resolve(
	sourceID string,
	inventory capability.Inventory,
	policy capability.ProviderPolicy,
	requirements capability.Requirements,
) (capability.Plan, error) {
	if allocator == nil {
		return capability.Resolve(inventory, policy, requirements)
	}
	if err := validateConcurrencyContracts(inventory); err != nil {
		return capability.Plan{}, err
	}
	allocator.mu.Lock()
	defer allocator.mu.Unlock()
	if reservationID := allocator.sourceReservation(sourceID); reservationID != "" {
		return capability.Plan{}, fmt.Errorf(
			"source %q has active capability transition %q", sourceID, reservationID)
	}

	available := inventory
	available.Components = append([]capability.Component(nil), inventory.Components...)
	for attempts := 0; attempts <= len(available.Components); attempts++ {
		for index := range available.Components {
			component := &available.Components[index]
			if componentShareable(*component) || !component.Available {
				continue
			}
			resourceID := componentResourceID(*component)
			if allocator.usedByOtherSources(resourceID, sourceID)+allocator.reservedUnits(resourceID) >=
				componentCapacity(*component) {
				rejectResourceGroup(&available, resourceID, allocator.claims[resourceID], sourceID)
			}
		}

		plan, err := capability.Resolve(available, policy, requirements)
		if err != nil {
			return capability.Plan{}, err
		}
		requested := planHardwareClaims(plan, available)
		overflow := ""
		for resourceID, request := range requested {
			component, ok := componentByID(available, request.componentID)
			if !ok || componentShareable(component) {
				continue
			}
			if allocator.usedByOtherSources(resourceID, sourceID)+allocator.reservedUnits(resourceID)+
				request.units > componentCapacity(component) {
				overflow = resourceID
				break
			}
		}
		if overflow == "" {
			allocator.releaseLocked(sourceID)
			for resourceID, request := range requested {
				if allocator.claims[resourceID] == nil {
					allocator.claims[resourceID] = map[string]int{}
				}
				allocator.claims[resourceID][sourceID] = request.units
			}
			return plan, nil
		}
		rejectResourceGroup(&available, overflow, allocator.claims[overflow], sourceID)
	}
	return capability.Plan{}, fmt.Errorf("hardware capacity resolution did not converge for source %q", sourceID)
}

func (allocator *capabilityAllocator) release(sourceID string) {
	if allocator == nil {
		return
	}
	allocator.mu.Lock()
	defer allocator.mu.Unlock()
	allocator.releaseLocked(sourceID)
}

func (allocator *capabilityAllocator) releaseLocked(sourceID string) {
	for componentID, owners := range allocator.claims {
		delete(owners, sourceID)
		if len(owners) == 0 {
			delete(allocator.claims, componentID)
		}
	}
}

func (allocator *capabilityAllocator) usedByOtherSources(componentID, sourceID string) int {
	used := 0
	for owner, units := range allocator.claims[componentID] {
		if owner != sourceID {
			used += units
		}
	}
	return used
}

func (allocator *capabilityAllocator) reservedUnits(resourceID string) int {
	used := 0
	for _, reservation := range allocator.reservations {
		used += reservation[resourceID]
	}
	return used
}

// capabilityTransitionReservation holds only the additional hardware needed
// while candidate output branches warm beside the current branches. Commit
// atomically replaces the complete active claims of every affected source;
// Abort drops only the temporary reservation. This lets a fan-out transaction
// prove peak prepare capacity without releasing the incumbent graph early.
type capabilityTransitionReservation struct {
	mu sync.Mutex

	allocator       *capabilityAllocator
	id              string
	currentClaims   map[string]map[string]hardwareClaim
	candidateClaims map[string]map[string]hardwareClaim
	capacities      map[string]int
	terminal        bool
}

func (allocator *capabilityAllocator) reserveTransition(
	reservationID string,
	inventory capability.Inventory,
	currentPlans map[string]capability.Plan,
	candidatePlans map[string]capability.Plan,
	warmupPlans map[string]capability.Plan,
) (*capabilityTransitionReservation, error) {
	return allocator.prepareTransition(
		reservationID, inventory, currentPlans, candidatePlans, warmupPlans, true)
}

// validateTransition applies the exact reservation admission logic without
// retaining allocator state. Preview can therefore fail on the same active
// claims, concurrent reservations and peak warmup capacity as Prepare, while
// Prepare remains the only operation that acquires the reservation.
func (allocator *capabilityAllocator) validateTransition(
	reservationID string,
	inventory capability.Inventory,
	currentPlans map[string]capability.Plan,
	candidatePlans map[string]capability.Plan,
	warmupPlans map[string]capability.Plan,
) error {
	_, err := allocator.prepareTransition(
		reservationID, inventory, currentPlans, candidatePlans, warmupPlans, false)
	return err
}

func (allocator *capabilityAllocator) prepareTransition(
	reservationID string,
	inventory capability.Inventory,
	currentPlans map[string]capability.Plan,
	candidatePlans map[string]capability.Plan,
	warmupPlans map[string]capability.Plan,
	acquire bool,
) (*capabilityTransitionReservation, error) {
	if allocator == nil {
		return nil, errors.New("capability allocator is required for a graph transition")
	}
	if strings.TrimSpace(reservationID) == "" || strings.TrimSpace(reservationID) != reservationID {
		return nil, errors.New("capability transition reservation id is not canonical")
	}
	if len(currentPlans) == 0 || len(currentPlans) != len(candidatePlans) {
		return nil, errors.New("capability transition plans are incomplete")
	}
	if err := validateConcurrencyContracts(inventory); err != nil {
		return nil, err
	}

	sourceIDs := make([]string, 0, len(currentPlans))
	currentClaims := make(map[string]map[string]hardwareClaim, len(currentPlans))
	candidateClaims := make(map[string]map[string]hardwareClaim, len(candidatePlans))
	capacities := map[string]int{}
	for sourceID, plan := range currentPlans {
		if strings.TrimSpace(sourceID) == "" || strings.TrimSpace(sourceID) != sourceID {
			return nil, errors.New("capability transition source id is not canonical")
		}
		candidate, exists := candidatePlans[sourceID]
		if !exists {
			return nil, fmt.Errorf("capability transition candidate source %q is missing", sourceID)
		}
		var err error
		currentClaims[sourceID], err = strictPlanHardwareClaims(plan, inventory)
		if err != nil {
			return nil, fmt.Errorf("current capability plan for %q: %w", sourceID, err)
		}
		candidateClaims[sourceID], err = strictPlanHardwareClaims(candidate, inventory)
		if err != nil {
			return nil, fmt.Errorf("candidate capability plan for %q: %w", sourceID, err)
		}
		sourceIDs = append(sourceIDs, sourceID)
	}
	for sourceID := range candidatePlans {
		if _, exists := currentPlans[sourceID]; !exists {
			return nil, fmt.Errorf("capability transition current source %q is missing", sourceID)
		}
	}
	sort.Strings(sourceIDs)

	allocator.mu.Lock()
	defer allocator.mu.Unlock()
	if allocator.reservations == nil {
		allocator.reservations = map[string]map[string]int{}
	}
	if allocator.reservedBy == nil {
		allocator.reservedBy = map[string]map[string]bool{}
	}
	if _, exists := allocator.reservations[reservationID]; exists {
		return nil, fmt.Errorf("capability transition reservation %q already exists", reservationID)
	}
	for _, sourceID := range sourceIDs {
		if owner := allocator.sourceReservation(sourceID); owner != "" {
			return nil, fmt.Errorf("capability transition source %q is already reserved by %q", sourceID, owner)
		}
		if active := allocator.activeClaimsForSource(sourceID); !sameClaimUnits(active, currentClaims[sourceID]) {
			return nil, fmt.Errorf("active capability claims for source %q differ from the current plan", sourceID)
		}
	}

	additional := map[string]int{}
	components := map[string]string{}
	warmupClaims := map[string]map[string]hardwareClaim{}
	for sourceID, plan := range warmupPlans {
		candidate, exists := candidateClaims[sourceID]
		if !exists {
			return nil, fmt.Errorf("capability warmup source %q is outside the transition", sourceID)
		}
		warmup, err := strictPlanHardwareClaims(plan, inventory)
		if err != nil {
			return nil, fmt.Errorf("warmup capability plan for %q: %w", sourceID, err)
		}
		warmupClaims[sourceID] = warmup
		for resourceID, claim := range warmup {
			if candidate[resourceID].units < claim.units {
				return nil, fmt.Errorf(
					"capability warmup resource %q is not present in candidate source %q",
					resourceID, sourceID)
			}
			additional[resourceID] += claim.units
			components[resourceID] = claim.componentID
		}
	}
	for _, sourceID := range sourceIDs {
		for resourceID, candidate := range candidateClaims[sourceID] {
			current := currentClaims[sourceID][resourceID]
			minimum := candidate.units - current.units
			if current.componentID != "" && current.componentID != candidate.componentID && minimum < candidate.units {
				minimum = candidate.units
			}
			if minimum > 0 && warmupClaims[sourceID][resourceID].units < minimum {
				return nil, fmt.Errorf(
					"capability warmup source %q under-reserves resource %q",
					sourceID, resourceID)
			}
		}
	}
	for _, claimsBySource := range []map[string]map[string]hardwareClaim{currentClaims, candidateClaims} {
		for _, claims := range claimsBySource {
			for resourceID, claim := range claims {
				component, exists := componentByID(inventory, claim.componentID)
				if !exists || componentShareable(component) {
					return nil, fmt.Errorf("capability transition resource %q has no finite hardware contract", resourceID)
				}
				capacities[resourceID] = componentCapacity(component)
			}
		}
	}
	for resourceID, units := range additional {
		component, exists := componentByID(inventory, components[resourceID])
		if !exists || componentShareable(component) {
			return nil, fmt.Errorf("capability transition resource %q has no finite hardware contract", resourceID)
		}
		if allocator.activeUnits(resourceID)+allocator.reservedUnits(resourceID)+units > componentCapacity(component) {
			return nil, fmt.Errorf(
				"capability transition resource %q lacks prepare capacity", resourceID)
		}
	}
	if !acquire {
		return nil, nil
	}
	allocator.reservations[reservationID] = additional
	allocator.reservedBy[reservationID] = make(map[string]bool, len(sourceIDs))
	for _, sourceID := range sourceIDs {
		allocator.reservedBy[reservationID][sourceID] = true
	}
	return &capabilityTransitionReservation{
		allocator: allocator, id: reservationID,
		currentClaims: currentClaims, candidateClaims: candidateClaims, capacities: capacities,
	}, nil
}

func (reservation *capabilityTransitionReservation) Commit() error {
	if reservation == nil {
		return errors.New("nil capability transition reservation")
	}
	reservation.mu.Lock()
	defer reservation.mu.Unlock()
	if reservation.terminal {
		return nil
	}
	allocator := reservation.allocator
	if allocator == nil {
		return errors.New("capability transition allocator is unavailable")
	}
	allocator.mu.Lock()
	defer allocator.mu.Unlock()
	ownedReservation, exists := allocator.reservations[reservation.id]
	if !exists {
		return errors.New("capability transition reservation is no longer active")
	}
	for sourceID, expected := range reservation.currentClaims {
		if active := allocator.activeClaimsForSource(sourceID); !sameClaimUnits(active, expected) {
			return fmt.Errorf("active capability claims for source %q changed before commit", sourceID)
		}
	}
	for resourceID, capacity := range reservation.capacities {
		finalUnits := allocator.activeUnits(resourceID)
		for sourceID, claims := range reservation.currentClaims {
			finalUnits -= claims[resourceID].units
			finalUnits += reservation.candidateClaims[sourceID][resourceID].units
		}
		otherReserved := allocator.reservedUnits(resourceID) - ownedReservation[resourceID]
		if finalUnits < 0 || finalUnits+otherReserved > capacity {
			return fmt.Errorf("capability transition resource %q lacks commit capacity", resourceID)
		}
	}
	for sourceID := range reservation.currentClaims {
		allocator.releaseLocked(sourceID)
	}
	for sourceID, claims := range reservation.candidateClaims {
		for resourceID, claim := range claims {
			if allocator.claims[resourceID] == nil {
				allocator.claims[resourceID] = map[string]int{}
			}
			allocator.claims[resourceID][sourceID] = claim.units
		}
	}
	delete(allocator.reservations, reservation.id)
	delete(allocator.reservedBy, reservation.id)
	reservation.terminal = true
	return nil
}

func (reservation *capabilityTransitionReservation) Abort() error {
	if reservation == nil {
		return errors.New("nil capability transition reservation")
	}
	reservation.mu.Lock()
	defer reservation.mu.Unlock()
	if reservation.terminal {
		return nil
	}
	if reservation.allocator == nil {
		return errors.New("capability transition allocator is unavailable")
	}
	reservation.allocator.mu.Lock()
	delete(reservation.allocator.reservations, reservation.id)
	delete(reservation.allocator.reservedBy, reservation.id)
	reservation.allocator.mu.Unlock()
	reservation.terminal = true
	return nil
}

func strictPlanHardwareClaims(
	plan capability.Plan,
	inventory capability.Inventory,
) (map[string]hardwareClaim, error) {
	claims := map[string]hardwareClaim{}
	for _, stage := range plan.Stages {
		if stage.Component == "" || !stage.Hardware {
			continue
		}
		component, exists := componentByID(inventory, stage.Component)
		if !exists {
			return nil, fmt.Errorf("selected hardware component %q is absent from inventory", stage.Component)
		}
		if !component.Hardware || !component.Available {
			return nil, fmt.Errorf("selected component %q is not available hardware", stage.Component)
		}
		if !componentSupportsSelection(component, stage) {
			return nil, fmt.Errorf(
				"selected component %q does not support stage=%q format=%q",
				stage.Component, stage.Stage, stage.Format)
		}
		if componentShareable(component) {
			continue
		}
		resourceID := componentResourceID(component)
		claim := claims[resourceID]
		claim.componentID = stage.Component
		claim.units++
		claims[resourceID] = claim
	}
	return claims, nil
}

func componentSupportsSelection(
	component capability.Component,
	selection capability.StageSelection,
) bool {
	stage := strings.ToLower(strings.TrimSpace(selection.Stage))
	format := capability.NormalizeFormat(selection.Format)
	for _, operation := range component.Operations {
		if strings.ToLower(strings.TrimSpace(operation.Stage)) != stage {
			continue
		}
		operationFormat := capability.NormalizeFormat(operation.Format)
		if operationFormat == "" || format == "" || operationFormat == format {
			return true
		}
	}
	return false
}

func (allocator *capabilityAllocator) activeClaimsForSource(sourceID string) map[string]int {
	claims := map[string]int{}
	for resourceID, owners := range allocator.claims {
		if units := owners[sourceID]; units > 0 {
			claims[resourceID] = units
		}
	}
	return claims
}

func sameClaimUnits(active map[string]int, expected map[string]hardwareClaim) bool {
	if len(active) != len(expected) {
		return false
	}
	for resourceID, claim := range expected {
		if active[resourceID] != claim.units {
			return false
		}
	}
	return true
}

func (allocator *capabilityAllocator) activeUnits(resourceID string) int {
	used := 0
	for _, units := range allocator.claims[resourceID] {
		used += units
	}
	return used
}

func (allocator *capabilityAllocator) sourceReservation(sourceID string) string {
	for reservationID, sources := range allocator.reservedBy {
		if sources[sourceID] {
			return reservationID
		}
	}
	return ""
}

func componentCapacity(component capability.Component) int {
	return component.MaxConcurrentOperations
}

func componentShareable(component capability.Component) bool {
	return component.ConcurrencyMode == capability.ConcurrencyShareable
}

func componentResourceID(component capability.Component) string {
	if resourceID := strings.TrimSpace(component.ConcurrencyGroup); resourceID != "" {
		return resourceID
	}
	return component.ID
}

func componentByID(inventory capability.Inventory, componentID string) (capability.Component, bool) {
	for _, component := range inventory.Components {
		if component.ID == componentID {
			return component, true
		}
	}
	return capability.Component{}, false
}

type hardwareClaim struct {
	componentID string
	units       int
}

func planHardwareClaims(plan capability.Plan, inventory capability.Inventory) map[string]hardwareClaim {
	claims := map[string]hardwareClaim{}
	for _, stage := range plan.Stages {
		if stage.Component == "" || !stage.Hardware {
			continue
		}
		component, ok := componentByID(inventory, stage.Component)
		if !ok || componentShareable(component) {
			continue
		}
		resourceID := componentResourceID(component)
		claim := claims[resourceID]
		claim.componentID = stage.Component
		claim.units++
		claims[resourceID] = claim
	}
	return claims
}

func rejectResourceGroup(inventory *capability.Inventory, resourceID string, owners map[string]int, sourceID string) {
	reason := "concurrent_capacity_exhausted"
	for owner := range owners {
		if owner != sourceID {
			reason = "leased_by_source:" + owner
			break
		}
	}
	for index := range inventory.Components {
		component := &inventory.Components[index]
		if componentResourceID(*component) != resourceID || componentShareable(*component) {
			continue
		}
		component.Available = false
		component.State = capability.StateRejected
		component.Reason = reason
	}
}

func validateConcurrencyContracts(inventory capability.Inventory) error {
	type contract struct {
		mode     string
		capacity int
	}
	groups := map[string]contract{}
	for _, component := range inventory.Components {
		group := strings.TrimSpace(component.ConcurrencyGroup)
		if group != component.ConcurrencyGroup {
			return fmt.Errorf("component %q has a non-canonical concurrency group", component.ID)
		}
		switch component.ConcurrencyMode {
		case capability.ConcurrencyExclusive:
			if group == "" || component.MaxConcurrentOperations != 1 {
				return fmt.Errorf("component %q has invalid exclusive concurrency contract", component.ID)
			}
		case capability.ConcurrencyFinite:
			if group == "" || component.MaxConcurrentOperations < 1 {
				return fmt.Errorf("component %q has invalid finite concurrency contract", component.ID)
			}
		case capability.ConcurrencyShareable:
			if group != "" || component.MaxConcurrentOperations != 0 {
				return fmt.Errorf("component %q has invalid shareable concurrency contract", component.ID)
			}
			continue
		default:
			return fmt.Errorf("component %q has unknown concurrency mode %q", component.ID, component.ConcurrencyMode)
		}
		value := contract{mode: component.ConcurrencyMode, capacity: component.MaxConcurrentOperations}
		if previous, exists := groups[group]; exists && previous != value {
			return fmt.Errorf("concurrency group %q has inconsistent component contracts", group)
		}
		groups[group] = value
	}
	return nil
}
