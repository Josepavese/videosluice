package envelope

import (
	"bytes"
	"context"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestNativeAtomicWordCopyBounds(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	binary := filepath.Join(t.TempDir(), "atomic-word-copy")
	command := exec.CommandContext(ctx, "cc", "-std=c11", "-O1", "-g", "-Wall", "-Wextra", "-Werror",
		"-fsanitize=address,undefined", "-fno-omit-frame-pointer", "-fno-pie", "-no-pie",
		"-Inative", "native/tests/atomic_word_copy_test.c", "native/causal_snapshot.c",
		"native/causal_envelope.c", "-o", binary)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("compile atomic word boundaries: %v\n%s", err, output)
	}
	if output, err := exec.CommandContext(ctx, binary).CombinedOutput(); err != nil ||
		!bytes.Contains(output, []byte("ATOMIC_WORD_BOUNDARIES_PASS")) {
		t.Fatalf("atomic word boundaries: %v\n%s", err, output)
	}
}
