//go:build linux && cgo

package framehash

import (
	"context"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestNativeKernelHashFaultsAndOwnership(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	binary := filepath.Join(t.TempDir(), "frame-sha256-test")
	source := "testdata/kernel_provider_test.c"
	cmd := exec.CommandContext(ctx, "cc", "-std=c11", "-Wall", "-Wextra", "-Werror", "-Wconversion", source, "-o", binary, "-lcrypto", "-pthread")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("native test build: %v\n%s", err, output)
	}
	for _, mode := range []string{"present", "absent", "invalid-selftest", "cold",
		"missing-software", "invalid-software", "slow-startup"} {
		t.Run(mode, func(t *testing.T) {
			args := []string{}
			if mode != "present" {
				args = append(args, mode)
			}
			if output, err := exec.CommandContext(ctx, binary, args...).CombinedOutput(); err != nil {
				t.Fatalf("native test: %v\n%s", err, output)
			}
		})
	}
}
