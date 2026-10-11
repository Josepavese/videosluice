package envelope

import "sync/atomic"

var mappingGeneration atomic.Uint64

// NextMappingGeneration issues a never-reused allocation identity within this
// broker process. The authenticated namespace fences process restarts. All
// delivery and native surface allocations use the same issuer.
func NextMappingGeneration() (uint64, error) {
	for {
		previous := mappingGeneration.Load()
		if previous == ^uint64(0) {
			return 0, ErrMapping
		}
		if mappingGeneration.CompareAndSwap(previous, previous+1) {
			return previous + 1, nil
		}
	}
}
