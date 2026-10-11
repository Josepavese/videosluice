package ingress

import (
	"bytes"
	"context"
	"errors"
	"time"

	peer "github.com/Josepavese/videosluice/framepeer"
)

// Startup readiness is distinct from a frame request or a grant. The sole
// launcher consumes this authenticated record once per broker execution,
// before starting consumers. No capability is carried or authority inferred
// from a pathname, PID string, timeout, or successful socket connection.
var bootstrapReadyRecord = [...]byte{'V', 'B', 'R', 'E', 'A', 'D', 'Y', 1}

func SendBootstrapReady(ctx context.Context, c peer.ControlServerConnection, controller string) error {
	if ctx == nil || c == nil || !controlToken(controller) {
		return peer.ErrInvalid
	}
	current, err := c.CurrentGeneration()
	if err != nil {
		return err
	}
	if current != controller {
		return peer.ErrRetired
	}
	return c.Reply(ctx, bootstrapReadyRecord[:])
}

func AwaitBootstrapReady(ctx context.Context, c peer.ControlConnection, broker string, budget time.Duration) error {
	if ctx == nil || c == nil || !controlToken(broker) || budget <= 0 || budget > 30*time.Second {
		return peer.ErrInvalid
	}
	ctx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	var record [len(bootstrapReadyRecord)]byte
	for {
		if err := ctx.Err(); err != nil {
			return errors.Join(err, peer.ErrNoRecord)
		}
		current, err := c.CurrentGeneration()
		if err != nil {
			return err
		}
		if current != broker {
			return peer.ErrRetired
		}
		n, principal, err := c.Receive(ctx, record[:])
		if errors.Is(err, peer.ErrDeadline) && errors.Is(err, peer.ErrNoRecord) && ctx.Err() == nil && n == 0 {
			continue // Idle startup: no request sent and no uncertain exchange.
		}
		if err != nil {
			return err
		}
		if principal.Generation != broker {
			return peer.ErrUnauthorized
		}
		if n != len(record) || !bytes.Equal(record[:], bootstrapReadyRecord[:]) {
			return peer.ErrRecord
		}
		current, err = c.CurrentGeneration()
		if err != nil {
			return err
		}
		if current != broker {
			return peer.ErrRetired
		}
		return ctx.Err()
	}
}
