//go:build !linux

package processimage

import (
	"context"
	"errors"
)

type Owner struct{}

func (*Owner) Bytes() uint64 { return 0 }
func (*Owner) Close() error  { return nil }
func acquire(ctx context.Context, budget uint64) (*Owner, error) {
	if ctx == nil {
		return nil, errors.New("nil process image context")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if budget != 0 {
		return nil, errors.New("process image residency requires Linux PAL")
	}
	return &Owner{}, nil
}
