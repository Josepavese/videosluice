//go:build linux && cgo

package main

import (
	"bytes"
	"context"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestNativeTransformCacheExactBoundedInvalidation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	binary := filepath.Join(t.TempDir(), "transform-cache")
	compile := exec.CommandContext(ctx, "cc", "-std=c11", "-O1", "-g", "-Wall", "-Wextra", "-Werror",
		"-fsanitize=address,undefined", "-fno-omit-frame-pointer", "-fno-pie", "-no-pie",
		"-I../../native", "-I../../framewire/native", "../../native/tests/nv12_transform_cache_test.c", "-lcrypto", "-o", binary)
	if out, err := compile.CombinedOutput(); err != nil {
		t.Fatalf("compile transform cache: %v\n%s", err, out)
	}
	if out, err := exec.CommandContext(ctx, binary).CombinedOutput(); err != nil || !bytes.Contains(out, []byte("OK_NV12_TRANSFORM_CACHE_EXACT_BOUNDED_INVALIDATION")) {
		t.Fatalf("transform cache: %v\n%s", err, out)
	}
}
