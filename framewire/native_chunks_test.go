package envelope

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestNativeChunkSenderFaults(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	binary := filepath.Join(t.TempDir(), "causal-chunks")
	compile := exec.CommandContext(ctx, "cc", "-std=c11", "-Wall", "-Wextra", "-Wpedantic", "-Werror", "-O1", "-g", "-fsanitize=address,undefined", "-fno-omit-frame-pointer", "-fno-pie", "-no-pie", "-Inative", "native/tests/chunks_test.c", "native/causal_chunks.c", "native/causal_envelope.c", "-lcrypto", "-o", binary)
	if out, e := compile.CombinedOutput(); e != nil {
		t.Fatalf("compile sender: %v\n%s", e, out)
	}
	if out, e := exec.CommandContext(ctx, binary).CombinedOutput(); e != nil || !bytes.Contains(out, []byte("NATIVE_CHUNKS_PASS")) {
		t.Fatalf("native sender: %v\n%s", e, out)
	}
}

func TestNativeChunkProtocolHasNoPlatformDependencies(t *testing.T) {
	allowed := map[string]bool{"#include \"causal_chunks.h\"": true, "#include \"causal_envelope.h\"": true, "#include <string.h>": true}
	for _, path := range []string{"native/causal_chunks.c", "native/causal_chunks.h"} {
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		for _, line := range strings.Split(string(b), "\n") {
			if strings.HasPrefix(line, "#include") && !allowed[line] {
				t.Fatalf("portable framing has platform dependency: %s", line)
			}
		}
	}
}
