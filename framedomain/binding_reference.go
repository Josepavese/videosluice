package framedomain

// MaxBindingReferences is the bounded source cardinality of one discovery
// snapshot. Streams and frames have their own independent limits.
const MaxBindingReferences = 64

// BindingRef identifies one immutable source layout, not the current layout
// by name. Media epochs are allocated by Runtime.Open and never reused within
// its process-fenced namespace, including failed admissions and rollbacks.
type BindingRef struct {
	Fence  Fence
	Source string
	Epoch  uint64
}

func (b *Binding) Reference() BindingRef {
	return BindingRef{Fence: b.runtime.fence, Source: b.scope.Source, Epoch: b.scope.MediaEpoch}
}

// ResolveBinding is a trusted control-plane lookup, not an authorization API.
// The caller must authenticate first and validate the returned binding again
// during admission: concurrent close/switch can retire it after this lookup.
func (r *Runtime) ResolveBinding(ref BindingRef) (*Binding, error) {
	if !validToken(ref.Source) || ref.Epoch == 0 {
		return nil, ErrInvalid
	}
	if err := r.checkFence(ref.Fence); err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, ErrProcessChanged
	}
	b := r.active[ref.Source]
	if b == nil || b.closed || b.core == nil || b.scope.MediaEpoch != ref.Epoch {
		return nil, ErrRetired
	}
	return b, nil
}

// SnapshotBindingsInto is a trusted control-plane view, not authorization or
// evidence that a frame is available. It copies only fully admitted bindings
// at one lock-protected instant, sorted by source, into caller-admitted storage.
// A later switch can retire a returned reference: grant/use MUST still resolve
// that exact epoch, never substitute the current source by name.
// At most MaxSources slots are touched; no reference-storage allocation or
// unbounded output. This does not quote the authority provider's footprint.
func (r *Runtime) SnapshotBindingsInto(buffer []BindingRef) (int, error) {
	if r == nil || len(buffer) < r.limits.MaxSources {
		return 0, ErrInvalid
	}
	buffer = buffer[:r.limits.MaxSources]
	clear(buffer)
	if err := r.checkProcess(); err != nil {
		return 0, err
	}
	r.mu.Lock()
	n := 0
	if !r.closed {
		for _, b := range r.active {
			if b.closed || b.core == nil {
				continue
			}
			if n == len(buffer) {
				r.mu.Unlock()
				clear(buffer)
				return 0, ErrInvalid
			}
			buffer[n] = b.Reference()
			n++
		}
	}
	r.mu.Unlock()
	// Bounded insertion sort avoids auxiliary allocation inside discovery.
	for i := 1; i < n; i++ {
		value := buffer[i]
		j := i
		for j > 0 && value.Source < buffer[j-1].Source {
			buffer[j] = buffer[j-1]
			j--
		}
		buffer[j] = value
	}
	if err := r.checkProcess(); err != nil {
		clear(buffer)
		return 0, err
	}
	return n, nil
}
