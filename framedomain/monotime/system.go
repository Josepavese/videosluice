// Package monotime supplies a process-local monotonic clock for frame deadlines.
package monotime

import "time"

type System struct{ epoch time.Time }

func New() *System { return &System{epoch: time.Now()} }
func (c *System) NowNS() uint64 {
	if c == nil || c.epoch.IsZero() {
		return 0
	}
	elapsed := time.Since(c.epoch)
	if elapsed < 0 {
		return 0
	}
	return uint64(elapsed) + 1
}
