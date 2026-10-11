//go:build linux && cgo

package linux

import (
	"bytes"
	"context"
	"errors"
	frame "github.com/Josepavese/videosluice/framewire"
	"github.com/Josepavese/videosluice/framewire/surfaceport"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
)

func mapping() frame.Mapping {
	return frame.Mapping{Namespace: [16]byte{1}, Generation: 1, Capacity: 1024}
}

func TestCreateDiagnostics(t *testing.T) {
	for code, want := range map[int]string{0: "unknown", 1: "validation", 2: "allocation", 3: "descriptor", 4: "size", 5: "mapping", 6: "atomic-admission", 7: "seals", 999: "unknown"} {
		if got := createStage(code); got != want {
			t.Fatal(code, got, want)
		}
	}
}

func TestDescriptorDenialReleasesQuota(t *testing.T) {
	if os.Getenv("VBSC_TEST_DESCRIPTOR_DENIAL") == "1" {
		m := manager(t, 1, 1)
		var original syscall.Rlimit
		if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &original); err != nil {
			os.Exit(90)
		}
		limited := original
		limited.Cur = 0
		if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &limited); err != nil {
			os.Exit(91)
		}
		_, err := m.Create(mapping())
		// Restore before process exit so LeakSanitizer can open /proc. The
		// denial still occurs at the real memfd syscall, without weakening ASan.
		if restore := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &original); restore != nil {
			os.Exit(93)
		}
		if !errors.Is(err, surfaceport.ErrUnavailable) || !strings.Contains(err.Error(), "stage=descriptor") || m.Usage() != (Usage{}) {
			os.Exit(92)
		}
		os.Exit(0)
	}
	path, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5_000_000_000)
	defer cancel()
	cmd := exec.CommandContext(ctx, path, "-test.run=^TestDescriptorDenialReleasesQuota$")
	cmd.Env = append(os.Environ(), "VBSC_TEST_DESCRIPTOR_DENIAL=1")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatal(err, string(out))
	}
}
func packet(t *testing.T, id uint64) []byte {
	t.Helper()
	h := frame.Header{Kind: frame.Delivery, Codec: frame.NV12, Surface: frame.Delivered, Correlation: 1,
		Width: 4, Height: 2, StrideY: 8, StrideUV: 8, SourceEpoch: 1, OriginSequence: id, PublicationEpoch: 1, DeliveryID: id,
		Namespace: [16]byte{1}, Graph: [32]byte{1}, Transform: [32]byte{1}, Source: "front", Stream: "analysis", Process: "broker-1"}
	p := make([]byte, 24)
	for i := range p {
		p[i] = byte((id + uint64(i)) % 251)
	}
	b, err := frame.Marshal(h, p)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func manager(t *testing.T, n, ops int) *Manager {
	t.Helper()
	m, err := New(Limits{Bytes: uint64(n) * pageCharge(1088), Allocations: n, Operations: ops})
	if err != nil {
		t.Fatal(err)
	}
	return m
}
func create(t *testing.T, m *Manager) *allocation {
	t.Helper()
	a, err := m.Create(mapping())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := a.Close(); err != nil {
			t.Error(err)
		}
	})
	return a.(*allocation)
}
func TestOwnedRoundTrip(t *testing.T) {
	m := manager(t, 1, 2)
	a := create(t, m)
	out := make([]byte, 1024)
	if _, err := a.SnapshotInto(context.Background(), out); !errors.Is(err, frame.ErrBusy) {
		t.Fatal(err)
	}
	p := packet(t, 7)
	if err := a.Publish(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	n, err := a.SnapshotInto(context.Background(), out)
	if err != nil || !bytes.Equal(out[:n], p) {
		t.Fatal(n, err)
	}
	if _, err = a.SnapshotInto(context.Background(), out[:471]); !errors.Is(err, frame.ErrInvalid) {
		t.Fatal(err)
	}
	p[450] ^= 1
	if err = a.Publish(context.Background(), p); !errors.Is(err, frame.ErrInvalid) {
		t.Fatal(err)
	}
	n, err = a.SnapshotInto(context.Background(), out)
	if err != nil || !bytes.Equal(out[:n], packet(t, 7)) {
		t.Fatal("invalid publication altered mapping", err)
	}
	a.Close()
	a.Close()
	if m.Usage() != (Usage{}) {
		t.Fatal(m.Usage())
	}
	if _, err = a.SnapshotInto(context.Background(), out); !errors.Is(err, surfaceport.ErrClosed) {
		t.Fatal(err)
	}
	if err = a.Publish(context.Background(), packet(t, 8)); !errors.Is(err, surfaceport.ErrClosed) {
		t.Fatal(err)
	}
}
func TestRetiredPinnedQuota(t *testing.T) {
	m := manager(t, 1, 1)
	a := create(t, m)
	if _, err := a.pin(); err != nil {
		t.Fatal(err)
	}
	if _, err := a.pin(); !errors.Is(err, surfaceport.ErrQuota) {
		t.Fatal(err)
	}
	a.Close()
	if got := m.Usage(); got.Allocations != 1 || got.Bytes != pageCharge(1088) {
		t.Fatal(got)
	}
	if _, err := m.Create(mapping()); !errors.Is(err, surfaceport.ErrQuota) {
		t.Fatal(err)
	}
	a.unpin()
	if m.Usage() != (Usage{}) {
		t.Fatal(m.Usage())
	}
	create(t, m)
}
func TestAdmissionAndCancellation(t *testing.T) {
	for _, l := range []Limits{{}, {Bytes: 1, Allocations: 1}, {Bytes: 1, Operations: 1}, {Allocations: 1, Operations: 1}} {
		if _, err := New(l); !errors.Is(err, frame.ErrInvalid) {
			t.Fatal(l, err)
		}
	}
	m := manager(t, 2, 1)
	for _, bad := range []frame.Mapping{{}, {Namespace: [16]byte{1}, Generation: 1, Capacity: 447}, {Namespace: [16]byte{1}, Generation: 1, Capacity: frame.HeaderSize + frame.MaxPayload + 1}} {
		if _, err := m.Create(bad); !errors.Is(err, frame.ErrInvalid) {
			t.Fatal(err)
		}
	}
	if m.Usage() != (Usage{}) {
		t.Fatal(m.Usage())
	}
	a := create(t, m)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := a.Publish(ctx, packet(t, 1)); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := a.SnapshotInto(ctx, make([]byte, 1024)); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := a.SnapshotInto(context.Background(), make([]byte, 1024)); !errors.Is(err, frame.ErrBusy) {
		t.Fatal(err)
	}
	if err := a.Publish(context.Background(), nil); !errors.Is(err, frame.ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := a.SnapshotInto(context.Background(), nil); !errors.Is(err, frame.ErrInvalid) {
		t.Fatal(err)
	}
	a.writer.Lock()
	if err := a.Publish(context.Background(), packet(t, 1)); !errors.Is(err, frame.ErrBusy) {
		t.Fatal(err)
	}
	a.writer.Unlock()
	other := mapping()
	other.Generation = 2
	other.Capacity = 4096
	// One small mapping has consumed half the byte budget. A larger allocation
	// must be rejected even though the handle count would allow it.
	if _, err := m.Create(other); !errors.Is(err, surfaceport.ErrQuota) {
		t.Fatal(err)
	}
}
func TestConcurrentPublicationAndRetirement(t *testing.T) {
	m := manager(t, 1, 5)
	a := create(t, m)
	p := packet(t, 1)
	if err := a.Publish(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	var readers sync.WaitGroup
	ready := make(chan struct{}, 4)
	for range 4 {
		readers.Add(1)
		go func() {
			defer readers.Done()
			out := make([]byte, 1024)
			ready <- struct{}{}
			for {
				n, err := a.SnapshotInto(context.Background(), out)
				if errors.Is(err, surfaceport.ErrClosed) {
					return
				}
				if errors.Is(err, frame.ErrBusy) || errors.Is(err, surfaceport.ErrQuota) {
					runtime.Gosched()
					continue
				}
				if err != nil {
					t.Error(err)
					return
				}
				h, payload, err := frame.Parse(out[:n])
				if err != nil {
					t.Error(err)
					return
				}
				for i, v := range payload {
					if v != byte((h.OriginSequence+uint64(i))%251) {
						t.Error("torn payload")
						return
					}
				}
			}
		}()
	}
	for range 4 {
		<-ready
	}
	for id := uint64(2); id <= 1000; id++ {
		if err := a.Publish(context.Background(), packet(t, id)); err != nil {
			t.Fatal(err)
		}
	}
	a.Close()
	readers.Wait()
	if m.Usage() != (Usage{}) {
		t.Fatal(m.Usage())
	}
}

func TestConcurrentCloseAndLastUnpin(t *testing.T) {
	m := manager(t, 1, 1)
	for range 100 {
		a := create(t, m)
		if _, err := a.pin(); err != nil {
			t.Fatal(err)
		}
		var group sync.WaitGroup
		group.Add(3)
		go func() { defer group.Done(); a.Close() }()
		go func() { defer group.Done(); a.Close() }()
		go func() { defer group.Done(); a.unpin() }()
		group.Wait()
		if m.Usage() != (Usage{}) {
			t.Fatal(m.Usage())
		}
	}
}
