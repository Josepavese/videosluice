package broker

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestFrameCleanupGuardSurvivesRuntimeRestart(t *testing.T) {
	state := t.TempDir()
	if err := loadFrameCleanupGuard(state); err != nil {
		t.Fatal(err)
	}
	persistFrameCleanupGuard(state, "incident-7", errors.New("provider release failed"))
	if err := loadFrameCleanupGuard(state); !errors.Is(err, errFrameCleanupGuardActive) {
		t.Fatalf("release failure did not survive restart: %v", err)
	}
	restarted := &videoBrokerRuntime{frameCleanupErr: loadFrameCleanupGuard(state)}
	if err := restarted.ensureChildLocked(context.Background()); !errors.Is(err, errFrameCleanupGuardActive) {
		t.Fatalf("restart admitted hardware behind persisted guard: %v", err)
	}
	path := frameCleanupGuardPath(state)
	if info, err := os.Stat(path); err != nil || info.Mode().Perm() != 0o640 {
		t.Fatalf("guard not durably written with restricted mode: %v %v", info, err)
	}
	if err := os.WriteFile(path, []byte("invalid"), 0o640); err != nil {
		t.Fatal(err)
	}
	if err := loadFrameCleanupGuard(state); err == nil {
		t.Fatal("corrupt guard was treated as a clean release")
	}
}

func TestFrameCleanupTimeoutHoldsUntilOriginalRetirementConfirms(t *testing.T) {
	state := t.TempDir()
	persistFrameCleanupGuard(state, "incident-8", context.DeadlineExceeded)
	path := filepath.Join(state, "camera", "frame-cleanup-guard.json")
	if err := loadFrameCleanupGuard(state); !errors.Is(err, errFrameCleanupGuardActive) {
		t.Fatalf("pending release was lost across restart: %v", err)
	}
	if err := clearPendingFrameCleanupGuard(state, "incident-8"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("confirmed release did not remove pending guard: %v", err)
	}
}

func TestCleanupCompletionCannotClearDifferentOrTerminalGuard(t *testing.T) {
	for _, scenario := range []string{"other_incident", "terminal_failure", "invalid"} {
		t.Run(scenario, func(t *testing.T) {
			state := t.TempDir()
			failure := error(context.DeadlineExceeded)
			if scenario == "terminal_failure" {
				failure = errors.New("SDK release not confirmed")
			}
			persistFrameCleanupGuard(state, "incident-9", failure)
			path := frameCleanupGuardPath(state)
			incident := "incident-9"
			if scenario == "other_incident" {
				incident = "incident-8"
			}
			if scenario == "invalid" {
				if err := os.WriteFile(path, []byte("invalid"), 0o640); err != nil {
					t.Fatal(err)
				}
			}
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if err := clearPendingFrameCleanupGuard(state, incident); !errors.Is(err, errFrameCleanupGuardActive) {
				t.Fatalf("unsafe completion allowed: %v", err)
			}
			after, err := os.ReadFile(path)
			if err != nil || string(before) != string(after) {
				t.Fatalf("unrelated guard modified: %v", err)
			}
		})
	}
}
