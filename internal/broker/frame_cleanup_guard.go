package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const frameCleanupGuardSchema = "video-broker.frame-cleanup-guard.v1"

type frameCleanupGuard struct {
	Schema     string    `json:"schema"`
	IncidentID string    `json:"incident_id"`
	Reason     string    `json:"reason"`
	CreatedAt  time.Time `json:"created_at"`
	BootID     string    `json:"boot_id,omitempty"`
}

func frameCleanupGuardPath(stateDirectory string) string {
	root := strings.TrimSpace(stateDirectory)
	if root == "" || !filepath.IsAbs(root) {
		return ""
	}
	return filepath.Join(root, cameraStateDirectoryName, "frame-cleanup-guard.json")
}

// An unconfirmed release, including a pending join, survives a service restart.
// A terminal provider failure is not auto-cleared by process restart, boot or
// mode change; only an independently verified hardware reset may remove it.
func loadFrameCleanupGuard(stateDirectory string) error {
	path := frameCleanupGuardPath(stateDirectory)
	if path == "" {
		return nil
	}
	body, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("frame cleanup guard cannot be read: %w", err)
	}
	var guard frameCleanupGuard
	if json.Unmarshal(body, &guard) != nil || guard.Schema != frameCleanupGuardSchema ||
		guard.IncidentID == "" || guard.CreatedAt.IsZero() {
		return errors.New("frame cleanup guard is invalid and requires operator review")
	}
	return fmt.Errorf("frame cleanup guard active incident=%s reason=%s: %w", guard.IncidentID, guard.Reason, errFrameCleanupGuardActive)
}

var errFrameCleanupGuardActive = errors.New("hardware release not verified")

func persistFrameCleanupGuard(stateDirectory, incidentID string, failure error) {
	path := frameCleanupGuardPath(stateDirectory)
	if path == "" || failure == nil {
		return
	}
	reason := "provider_release_unconfirmed"
	if errors.Is(failure, context.DeadlineExceeded) || errors.Is(failure, context.Canceled) {
		reason = "cleanup_pending"
	}
	guard := frameCleanupGuard{Schema: frameCleanupGuardSchema, IncidentID: incidentID,
		Reason: reason, CreatedAt: time.Now().UTC()}
	if boot, err := os.ReadFile("/proc/sys/kernel/random/boot_id"); err == nil {
		guard.BootID = strings.TrimSpace(string(boot))
	}
	body, err := json.Marshal(guard)
	if err == nil {
		err = writeCameraStateDocument(path, body)
	}
	if err != nil {
		log.Printf("[video-broker] event=frame_cleanup_guard_persist_failed incident_id=%s path=%s err=%v action=hold_admission", incidentID, path, err)
	} else {
		log.Printf("[video-broker] event=frame_cleanup_guard_persisted incident_id=%s path=%s action=hold_admission", incidentID, path)
	}
}

// Only the original retirement operation's successful result can remove a
// pending guard. A terminal SDK failure has no such path in the broker.
func clearPendingFrameCleanupGuard(stateDirectory, incidentID string) error {
	path := frameCleanupGuardPath(stateDirectory)
	if path == "" {
		return nil
	}
	body, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var guard frameCleanupGuard
	if json.Unmarshal(body, &guard) != nil || guard.Schema != frameCleanupGuardSchema ||
		guard.CreatedAt.IsZero() || incidentID == "" || guard.IncidentID != incidentID ||
		guard.Reason != "cleanup_pending" {
		return fmt.Errorf("cleanup completion does not match pending guard: %w", errFrameCleanupGuardActive)
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	directory, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}
