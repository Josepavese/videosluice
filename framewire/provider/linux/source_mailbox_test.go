//go:build linux && cgo

package linux

import (
	"os/exec"
	"path/filepath"
	"testing"
)

func TestSourceMailboxNative(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "source-mailbox")
	command := exec.Command("cc", "-std=c11", "-O1", "-g", "-Wall", "-Wextra", "-Werror", "-fsanitize=address,undefined", "-fno-omit-frame-pointer", "-fno-pie", "-no-pie", "-I../../native", "native/source_mailbox_test.c", "../../native/atomic_snapshot.c", "../../native/causal_snapshot.c", "../../native/causal_envelope.c", "-lcrypto", "-o", binary)
	if out, err := command.CombinedOutput(); err != nil {
		t.Fatalf("compile: %v\n%s", err, out)
	}
	if out, err := exec.Command(binary).CombinedOutput(); err != nil {
		t.Fatalf("mailbox: %v\n%s", err, out)
	} else {
		t.Log(string(out))
	}
}
