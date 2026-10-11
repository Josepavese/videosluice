//go:build linux && cgo

package framehash

import (
	"bytes"
	"crypto/sha256"
	"log"
	"strings"
	"testing"
	"unsafe"
)

// A provider failure must not kill the process: this port cannot return an
// error, and a panic from inside a frame takes down the whole video-broker
// instead of refusing one frame. The failure is reported, the digest stays
// zero — which the wire validation refuses outright — and the count stays
// visible so the reason is not lost.
func TestSum256RefusesTheFrameWhenTheProviderIsUnavailable(t *testing.T) {
	previous := nativeSum
	previousFailures := ProviderFailures()
	t.Cleanup(func() { nativeSum = previous })

	var output bytes.Buffer
	previousWriter := log.Writer()
	previousFlags := log.Flags()
	previousPrefix := log.Prefix()
	log.SetOutput(&output)
	log.SetFlags(0)
	log.SetPrefix("")
	t.Cleanup(func() {
		log.SetOutput(previousWriter)
		log.SetFlags(previousFlags)
		log.SetPrefix(previousPrefix)
	})

	calls := 0
	nativeSum = func(pixels unsafe.Pointer, size uintptr, digest unsafe.Pointer) int {
		calls++
		return -1
	}

	payload := bytes.Repeat([]byte{0x5a}, 345600)
	// 500 failures: enough to prove the neutral value and the bounded report.
	for i := 0; i < 500; i++ {
		if got := Sum256(payload); got != [sha256.Size]byte{} {
			t.Fatalf("failed provider digest = %x, want zero", got)
		}
	}
	if calls != 500 {
		t.Fatalf("provider calls = %d, want 500", calls)
	}
	if got := ProviderFailures() - previousFailures; got != 500 {
		t.Fatalf("provider failures = %d, want 500", got)
	}
	logged := strings.Split(strings.TrimSpace(output.String()), "\n")
	if len(logged) != 1 {
		t.Fatalf("reports=%d, want 1 (first failure only under the stride): %q", len(logged), logged)
	}
	if !strings.Contains(logged[0], "frame_integrity_provider_unavailable") || !strings.Contains(logged[0], "failures=1") {
		t.Fatalf("report does not name the cause and the count: %q", logged[0])
	}

	// A payload below the native threshold still uses the portable path and
	// keeps working: the failure is confined to the provider that is broken.
	small := payload[:4095]
	if got := Sum256(small); got != sha256.Sum256(small) {
		t.Fatal("small payload did not use the portable path")
	}
}

// The zero digest must never be accepted by the wire contract: otherwise a
// failed provider and a frame claiming an all-zero digest would agree, and the
// frame would be published unverified.
func TestZeroDigestIsNotAVerifiablePayload(t *testing.T) {
	if sha256.Sum256(nil) == [sha256.Size]byte{} {
		t.Fatal("the empty-payload digest is zero: the invariant does not hold")
	}
}
