//go:build !linux || !cgo

package framehash

import "crypto/sha256"

// The portable build has the same algorithm and wire contract; it does not
// claim the native Linux provider or admit an unsupported media accelerator.
func Sum256(payload []byte) [sha256.Size]byte { return sha256.Sum256(payload) }

// ProviderFailures is always zero here: the portable build has no external
// provider that can be unavailable.
func ProviderFailures() uint64 { return 0 }

func Backend() string { return "go" }

func WorkspaceBytes(int) int { return 0 }

func MaxWorkspaceBytes(int) int { return 0 }

func NativeWorkspaceLimit() int { return 0 }
