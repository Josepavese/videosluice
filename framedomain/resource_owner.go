package framedomain

import "context"

// A runtime owner serves the broker-wide control plane; a binding owner
// additionally enforces source quotas and retirement. Both use one budget.
type resourceOwner struct {
	runtime *Runtime
	binding *Binding
}

func (o resourceOwner) source() string {
	if o.binding != nil {
		return o.binding.scope.Source
	}
	return ""
}
func (o resourceOwner) closedLocked() bool {
	return o.runtime.closed || o.binding != nil && o.binding.closed
}
func (o resourceOwner) current() error {
	if o.binding != nil {
		return o.binding.current()
	}
	return o.runtime.checkProcess()
}
func (o resourceOwner) begin(ctx context.Context) (context.Context, func(), error) {
	if ctx == nil {
		return nil, nil, ErrInvalid
	}
	if o.binding != nil {
		_, operation, finish, err := o.binding.begin(ctx)
		return operation, finish, err
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if err := o.current(); err != nil {
		return nil, nil, err
	}
	operation, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(o.runtime.ctx, cancel)
	return operation, func() { stop(); cancel() }, nil
}
func (o resourceOwner) addLocked(s *resourceState) {
	if o.binding != nil {
		o.binding.resources[s] = struct{}{}
		o.runtime.sourceBytes[o.binding.scope.Source] += s.bytes
	} else {
		o.runtime.ownedResources[s] = struct{}{}
	}
}
func (o resourceOwner) removeLocked(s *resourceState) {
	if o.binding != nil {
		delete(o.binding.resources, s)
		o.runtime.sourceBytes[o.binding.scope.Source] -= s.bytes
	} else {
		delete(o.runtime.ownedResources, s)
	}
}
