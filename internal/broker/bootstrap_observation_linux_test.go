//go:build linux

package broker

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

func TestBootstrapObservationSanitizesAndDistinguishesMissingSignals(t *testing.T) {
	for _, tc := range []struct{ name, status, wait, want string }{
		{"caught", "State:\tS (sleeping)\nThreads:\t9\nSigCgt:\t0000000100005002\nSigBlk:\t0", "do_select\n", "startup_observation=proc state=S threads=9 term_caught=true term_blocked=false wait_channel=do_select"},
		{"before_handler", "State:\tD (disk sleep)\nThreads:\t1\nSigCgt:\t0\nSigBlk:\t4000", "folio_wait_bit_common", "startup_observation=proc state=D threads=1 term_caught=false term_blocked=true wait_channel=folio_wait_bit_common"},
		{"missing", "Name:\tprivate-camera-name\n", "", "startup_observation=proc state=unknown threads=unknown term_caught=unknown term_blocked=unknown wait_channel=unavailable"},
		{"hostile", "State:\tRinjected\nThreads:\t1 bad=yes\nSigCgt:\txyz\nSigBlk:\t-1", "wait\nforged=yes", "startup_observation=proc state=unknown threads=unknown term_caught=unknown term_blocked=unknown wait_channel=unavailable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := formatBootstrapObservation(tc.status, tc.wait); got != tc.want {
				t.Fatalf("got %q want %q", got, tc.want)
			}
		})
	}
}

func TestBootstrapObservationOwnedUnwaitedProcess(t *testing.T) {
	if got := brokerBootstrapObservation(nil); got != "startup_observation=unavailable" {
		t.Fatal(got)
	}
	// Observe this test's live process without starting camera or SDK resources.
	cmd := &exec.Cmd{Process: &os.Process{Pid: os.Getpid()}}
	got := brokerBootstrapObservation(cmd)
	if !strings.Contains(got, "startup_observation=proc") || strings.Contains(got, "threads=unknown") || len(got) > 256 {
		t.Fatal(got)
	}
	path := t.TempDir() + "/oversize"
	if err := os.WriteFile(path, []byte("12345"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := readBootstrapProc(path, 4); err == nil {
		t.Fatal("accepted oversized observation")
	}
}
