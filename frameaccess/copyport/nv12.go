// Package copyport implements an explicit CPU copy provider. Its presence is
// not permission to use software: NewStore separately requires policy and a
// source/epoch/graph-bound runtime admission receipt.
package copyport

import (
	"context"
	fa "github.com/Josepavese/videosluice/frameaccess"
)

type CPU struct{}

func (CPU) Capability() fa.CopyCapability {
	return fa.CopyCapability{Provider: "cpu_nv12_copy", Hardware: false}
}
func (CPU) CopyNV12(ctx context.Context, b fa.CopyBudget, dst, src []byte, g fa.Geometry, r fa.Rect) error {
	if err := b.Check(ctx); err != nil {
		return err
	}
	size, err := g.Bytes()
	if err != nil || len(src) != size || r.X1 < 0 || r.Y1 < 0 || r.X2 > g.Width || r.Y2 > g.Height || r.X2 <= r.X1 || r.Y2 <= r.Y1 || r.X1%2 != 0 || r.Y1%2 != 0 || r.X2%2 != 0 || r.Y2%2 != 0 {
		return fa.InvalidContract
	}
	w, h := r.X2-r.X1, r.Y2-r.Y1
	// Full input cloning preserves stride, including row padding. ROI outputs
	// are packed. The store's full-frame publication destination uses src size.
	if r == (fa.Rect{X1: 0, Y1: 0, X2: g.Width, Y2: g.Height}) && len(dst) == size {
		if size > b.MaxBytes {
			return fa.CapacityRejected
		}
		for offset := 0; offset < size; offset += g.Stride {
			if err := b.Check(ctx); err != nil {
				return err
			}
			copy(dst[offset:offset+g.Stride], src[offset:offset+g.Stride])
		}
		return b.Check(ctx)
	}
	n := w * (h + h/2)
	if len(dst) != n || n > b.MaxBytes {
		return fa.CapacityRejected
	}
	for row := 0; row < h; row++ {
		if err := b.Check(ctx); err != nil {
			return err
		}
		start := (r.Y1+row)*g.Stride + r.X1
		copy(dst[row*w:(row+1)*w], src[start:start+w])
	}
	base := g.Stride * g.Height
	for row := 0; row < h/2; row++ {
		if err := b.Check(ctx); err != nil {
			return err
		}
		start := base + (r.Y1/2+row)*g.Stride + r.X1
		copy(dst[w*h+row*w:w*h+(row+1)*w], src[start:start+w])
	}
	return b.Check(ctx)
}
