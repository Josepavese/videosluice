//go:build linux && cgo

// Package framehash computes the unchanged SHA-256 frame-integrity contract.
// Linux selects the qualified AF_ALG driver when available; otherwise it uses
// the libcrypto implementation already required by the native frame provider.
package framehash

/*
#cgo CFLAGS: -D_GNU_SOURCE
#cgo LDFLAGS: -lcrypto -lpthread
#include "frame_sha256_linux.h"
*/
import "C"

import (
	"crypto/fips140"
	"crypto/sha256"
	"log"
	"sync/atomic"
	"unsafe"
)

// Small headers stay in Go rather than crossing cgo. This is an implementation
// boundary, not a different algorithm, protocol, trust policy or media fallback.
const nativeMinimumBytes = 4096

// providerFailureLogStride bounds the reporting: the first failure is logged in
// full, then every stride-th one, so a permanently broken provider stays visible
// in the journal without writing a line per frame.
const providerFailureLogStride = 1000

// providerFailures counts the payloads whose digest could not be computed
// because the native provider was unavailable.
var providerFailures atomic.Uint64

// nativeSum is the provider call, indirected so a test can drive the failure
// path that otherwise only a broken kernel/driver provider reaches. It takes
// plain Go types so tests do not need cgo.
var nativeSum = func(pixels unsafe.Pointer, size uintptr, digest unsafe.Pointer) int {
	return int(C.vb_frame_sha256_sum((*C.uint8_t)(pixels), C.size_t(size), (*C.uint8_t)(digest)))
}

// ProviderFailures reports how many payloads were refused because the native
// provider was unavailable. It never decreases: the provider's own policy is
// that a failure is terminal for the process.
func ProviderFailures() uint64 { return providerFailures.Load() }

func Sum256(payload []byte) [sha256.Size]byte {
	// An explicitly selected Go FIPS module must not be bypassed by libcrypto.
	if len(payload) < nativeMinimumBytes || fips140.Enabled() {
		return sha256.Sum256(payload)
	}
	var digest [sha256.Size]byte
	// The selected provider is synchronous, borrows bytes only for this call,
	// and retains neither Go pointer. Failure never accepts an unchecked frame.
	if nativeSum(unsafe.Pointer(&payload[0]), uintptr(len(payload)), unsafe.Pointer(&digest[0])) != 0 {
		// This port cannot return an error, and the provider declares its own
		// failure terminal: it must never silently fall back to software. Killing
		// the process from inside a frame is not an acceptable way to say that,
		// so the failure is reported and the digest stays zero. A zero digest
		// cannot match a published payload digest — the header validation rejects
		// an all-zero PayloadSHA outright — so every affected frame is refused as
		// source_integrity, the journal names the reason, and the pipeline fails
		// closed instead of the process dying.
		failures := providerFailures.Add(1)
		if failures == 1 || failures%providerFailureLogStride == 0 {
			log.Printf("event=frame_integrity_provider_unavailable backend=unknown failures=%d action=refuse_frame; restart the task to re-select the provider", failures)
		}
		return [sha256.Size]byte{}
	}
	return digest
}

// Backend names the large-payload implementation, not hardware acceleration.
func Backend() string {
	if fips140.Enabled() {
		return "go"
	}
	return C.GoString(C.vb_frame_sha256_backend())
}

// WorkspaceBytes is reserved by the owner for every concurrent invocation.
// It includes capped kernel hash workspace, not just Go heap allocations.
func WorkspaceBytes(payloadBytes int) int {
	if payloadBytes < nativeMinimumBytes || fips140.Enabled() {
		return 0
	}
	return int(C.vb_frame_sha256_workspace_bytes(C.size_t(payloadBytes)))
}

// MaxWorkspaceBytes covers every permitted size, not just the largest payload:
// a ceiling above the kernel limit can still produce a smaller native ROI.
func MaxWorkspaceBytes(maxPayloadBytes int) int {
	return WorkspaceBytes(min(maxPayloadBytes, int(C.VB_FRAME_SHA256_KERNEL_MAX_BYTES)))
}

// NativeWorkspaceLimit is the provider ceiling also checked by the native
// mapping adapter. Importing this owning package binds Go's build cache to the
// shared C header; an out-of-package C include alone is not cache invalidation.
func NativeWorkspaceLimit() int { return int(C.VB_FRAME_SHA256_KERNEL_WORKSPACE) }
