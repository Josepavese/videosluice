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

func TestNativeSourcePublisherOwnershipAndSaturation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	binary := filepath.Join(t.TempDir(), "source-publisher")
	compile := exec.CommandContext(ctx, "cc", "-std=c11", "-O1", "-g", "-Wall", "-Wextra", "-Werror", "-Wconversion", "-pthread",
		"-fsanitize=address,undefined", "-fno-omit-frame-pointer", "-fno-pie", "-no-pie",
		"-I../../native", "-I../../framewire/native", "../../native/source_publisher.c", "../../native/tests/source_publisher_test.c", "-o", binary)
	if out, err := compile.CombinedOutput(); err != nil {
		t.Fatalf("compile source publisher: %v\n%s", err, out)
	}
	if out, err := exec.CommandContext(ctx, binary).CombinedOutput(); err != nil || !bytes.Contains(out, []byte("SOURCE_PUBLISHER_PASS")) {
		t.Fatalf("source publisher: %v\n%s", err, out)
	}
}
