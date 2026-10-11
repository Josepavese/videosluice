package capability

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestGStreamerProbeScansInOwnedProcessAndReapsTimeout(t *testing.T) {
	directory := t.TempDir()
	record := filepath.Join(directory, "probe.record")
	// Only shell builtins before exec: the timeout fixture remains the exact
	// child owned and reaped by CommandContext, never an orphaned grandchild.
	script := "#!/bin/sh\n[ \"$1\" = --gst-disable-registry-fork ] || exit 42\nprintf '%s\\n%s\\n' \"$$\" \"$GST_PLUGIN_PATH\" > \"$VB_PROBE_RECORD\"\n[ \"$2\" != hold ] || exec /bin/sleep 10\n"
	if err := os.WriteFile(filepath.Join(directory, "gst-inspect-1.0"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", directory+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("VB_PROBE_RECORD", record)
	t.Setenv("GST_PLUGIN_PATH", "/existing/plugins")
	probe := OSProbe{}
	if !probe.InspectGStreamer(context.Background(), "videobrokertone", "/product/plugins") {
		t.Fatal("probe did not disable registry forking")
	}
	data, err := os.ReadFile(record)
	if err != nil || !strings.Contains(string(data), "/product/plugins:/existing/plugins") {
		t.Fatal("product plugin path was not preserved", err, string(data))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	started := time.Now()
	if probe.InspectGStreamer(ctx, "hold", "") || time.Since(started) > 2*time.Second {
		t.Fatal("cancelled discovery was admitted or unbounded")
	}
	data, err = os.ReadFile(record)
	if err != nil {
		t.Fatal(err)
	}
	pid, err := strconv.Atoi(strings.SplitN(string(data), "\n", 2)[0])
	if err != nil || pid <= 1 {
		t.Fatal("missing owned probe identity", err)
	}
	if err = syscall.Kill(pid, 0); !errors.Is(err, syscall.ESRCH) {
		t.Fatal("timed-out owned probe was not reaped", pid, err)
	}
}
