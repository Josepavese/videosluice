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

func TestNativeOutputSubmissionWaitAttribution(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	binary := filepath.Join(t.TempDir(), "output-submission")
	compile := exec.CommandContext(ctx, "cc", "-std=c11", "-O1", "-g", "-Wall", "-Wextra", "-Werror", "-pthread",
		"-fsanitize=address,undefined", "-fno-omit-frame-pointer", "-fno-pie", "-no-pie",
		"-I../../native", "-I../../framewire/native", "-I../../framepeer/native", "-I../../framepeer/provider/linux/native",
		"../../native/tests/frame_source_failure_test.c", "../../native/frame_source_linux.c", "../../native/source_publisher.c",
		"../../framewire/native/causal_envelope.c", "../../framewire/native/causal_chunks.c",
		"../../framewire/native/causal_snapshot.c", "../../framewire/native/atomic_snapshot.c",
		"../../framewire/provider/linux/native/source_mailbox.c", "../../framepeer/provider/linux/native/launch.c",
		"-Wl,--wrap=framepeer_linux_current", "-lcrypto", "-o", binary)
	if out, err := compile.CombinedOutput(); err != nil {
		t.Fatalf("compile output submission: %v\n%s", err, out)
	}
	if out, err := exec.CommandContext(ctx, binary).CombinedOutput(); err != nil || !bytes.Contains(out, []byte("OK_NATIVE_OUTPUT_MAILBOX_NO_READER_NO_CAPTURE_FAILURE")) {
		t.Fatalf("output submission: %v\n%s", err, out)
	}
}
