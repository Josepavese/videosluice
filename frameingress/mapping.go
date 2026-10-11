package ingress

import (
	"context"
	wire "github.com/Josepavese/videosluice/framewire"
	"github.com/Josepavese/videosluice/framewire/surfaceport"
	fd "github.com/Josepavese/videosluice/framedomain"
)

// OwnMapping composes authenticated source ownership, provider-quoted memory
// and physical release. It does not expose or duplicate an FD, choose a provider
// or pretend that a descriptor is process authentication.
func OwnMapping(ctx context.Context, b *fd.Binding, provider surfaceport.Allocator, m wire.Mapping) (*fd.Resource[surfaceport.Allocation], error) {
	if ctx == nil || b == nil || provider == nil {
		return nil, fail("mapping_admission", fd.ErrInvalid)
	}
	if m.Namespace != b.Fence().Namespace || m.Generation != b.Scope().MediaEpoch {
		return nil, fail("mapping_domain", fd.ErrDomainMismatch)
	}
	n, err := provider.Footprint(m)
	if err != nil {
		return nil, fail("mapping_footprint", err)
	}
	if n == 0 || n > 1<<30 {
		return nil, fail("mapping_footprint", fd.ErrInvalid)
	}
	result, err := fd.OwnResource(ctx, b, fd.ResourcePlan[surfaceport.Allocation]{
		Bytes: int(n),
		Create: func(ctx context.Context) (fd.Created[surfaceport.Allocation], error) {
			if err := ctx.Err(); err != nil {
				return fd.Created[surfaceport.Allocation]{}, err
			}
			a, err := provider.Create(m)
			return fd.Created[surfaceport.Allocation]{Value: a, Owned: a != nil}, err
		},
		Release: func(a surfaceport.Allocation) error { return a.Close() },
	})
	return result, fail("mapping_ownership", err)
}
