//go:build linux

package linux

import (
	peer "github.com/Josepavese/videosluice/framepeer"
	"os"
	"sync"
)

type pendingCapability struct {
	mu       sync.Mutex
	file     *os.File
	closed   bool
	closeErr error
}

// OwnReceivedCapability takes exclusive ownership of one already received,
// admitted file. It does not allocate another FD or authenticate its sender.
func OwnReceivedCapability(file *os.File) (peer.PendingCapability, error) {
	if file == nil {
		return nil, peer.ErrInvalid
	}
	return &pendingCapability{file: file}, nil
}

func (p *pendingCapability) Import(kind string, metadata []byte) (peer.ProcessCapability, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || p.file == nil {
		return nil, failure("capability_pending_retired", peer.ErrRetired)
	}
	if kind != ProcessCapabilityType {
		return nil, failure("capability_provider_unavailable", peer.ErrUnavailable)
	}
	identity, err := DecodeProcessIdentity(metadata)
	if err != nil {
		return nil, err
	}
	capability, err := ImportProcessCapability(p.file, identity)
	if err != nil {
		return nil, err
	}
	p.file = nil // validated owned capability now holds the exact received file
	p.closed = true
	return capability, nil
}

func (p *pendingCapability) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return p.closeErr
	}
	p.closed = true
	p.closeErr = p.file.Close()
	p.file = nil // never retry close against a potentially reused descriptor
	return p.closeErr
}
