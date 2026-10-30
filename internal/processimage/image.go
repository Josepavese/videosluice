// Package processimage owns bounded residency of the startup process's immutable
// file images. It does not pin heaps, media buffers, models or child processes.
package processimage

import "context"

// Acquire reserves file-backed read-only pages of the currently loaded program
// and shared libraries. A zero budget disables admission. A positive budget is
// mandatory: unsupported platforms, changed files or insufficient memlock fail
// before startup. The caller owns Close until all protected work has stopped.
func Acquire(ctx context.Context, budget uint64) (*Owner, error) {
	return acquire(ctx, budget)
}
