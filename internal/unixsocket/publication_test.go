package unixsocket

import (
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestSingleOwnerPublicationIsPrivateAndHasOneLink(t *testing.T) {
	logicalPath := filepath.Join(t.TempDir(), "routing-control.sock")
	publication, err := ListenSingleOwner(logicalPath)
	if err != nil {
		t.Fatal(err)
	}
	if publication.privatePath != "" {
		publication.Cleanup()
		t.Fatalf("single-owner publication retained a private alias: %s", publication.privatePath)
	}
	info, err := os.Lstat(logicalPath)
	if err != nil {
		publication.Cleanup()
		t.Fatal(err)
	}
	metadata, ok := info.Sys().(*syscall.Stat_t)
	if info.Mode()&os.ModeSocket == 0 || info.Mode().Perm() != 0o600 ||
		!ok || metadata.Nlink != 1 {
		publication.Cleanup()
		t.Fatalf("single-owner metadata mode=%v metadata=%+v", info.Mode(), metadata)
	}
	assertPublicationAcceptsPath(t, publication, logicalPath)
	publication.Cleanup()
	if _, err := os.Lstat(logicalPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("single-owner cleanup retained endpoint: %v", err)
	}
}

func TestSingleOwnerCleanupCannotRemoveReplacement(t *testing.T) {
	logicalPath := filepath.Join(t.TempDir(), "routing-control.sock")
	publication, err := ListenSingleOwner(logicalPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(logicalPath); err != nil {
		publication.Cleanup()
		t.Fatal(err)
	}
	replacement, err := net.Listen("unix", logicalPath)
	if err != nil {
		publication.Cleanup()
		t.Fatal(err)
	}
	defer replacement.Close()
	publication.Cleanup()
	info, err := os.Lstat(logicalPath)
	if err != nil || info.Mode()&os.ModeSocket == 0 {
		t.Fatalf("old single-owner cleanup removed replacement: info=%v err=%v", info, err)
	}
}

func TestSingleOwnerPublicationReclaimsStaleSocket(t *testing.T) {
	logicalPath := filepath.Join(t.TempDir(), "routing-control.sock")
	stale, err := net.Listen("unix", logicalPath)
	if err != nil {
		t.Fatal(err)
	}
	stale.(*net.UnixListener).SetUnlinkOnClose(false)
	if err := stale.Close(); err != nil {
		t.Fatal(err)
	}
	publication, err := ListenSingleOwner(logicalPath)
	if err != nil {
		t.Fatalf("single-owner publication did not reclaim stale socket: %v", err)
	}
	publication.Cleanup()
}

func TestPublicationUsesStableHardLinkedEndpoint(t *testing.T) {
	logicalPath := filepath.Join(t.TempDir(), "live.sock")
	publication, err := Listen(logicalPath)
	if err != nil {
		t.Fatal(err)
	}
	privateInfo, err := os.Lstat(publication.privatePath)
	if err != nil {
		publication.Cleanup()
		t.Fatal(err)
	}
	logicalInfo, err := os.Lstat(logicalPath)
	if err != nil {
		publication.Cleanup()
		t.Fatal(err)
	}
	if privateInfo.Mode()&os.ModeSocket == 0 || logicalInfo.Mode()&os.ModeSocket == 0 ||
		!os.SameFile(privateInfo, logicalInfo) {
		publication.Cleanup()
		t.Fatalf("logical endpoint is not the private listener inode: private=%v logical=%v", privateInfo.Mode(), logicalInfo.Mode())
	}

	received := make(chan string, 1)
	go func() {
		connection, acceptErr := publication.Accept()
		if acceptErr != nil {
			received <- "accept: " + acceptErr.Error()
			return
		}
		defer connection.Close()
		_ = connection.SetReadDeadline(time.Now().Add(time.Second))
		buffer := make([]byte, 4)
		_, readErr := io.ReadFull(connection, buffer)
		if readErr != nil {
			received <- "read: " + readErr.Error()
			return
		}
		received <- string(buffer)
	}()
	client, err := net.DialTimeout("unix", logicalPath, time.Second)
	if err != nil {
		publication.Cleanup()
		t.Fatal(err)
	}
	if _, err := client.Write([]byte("live")); err != nil {
		_ = client.Close()
		publication.Cleanup()
		t.Fatal(err)
	}
	_ = client.Close()
	if got := <-received; got != "live" {
		publication.Cleanup()
		t.Fatalf("hard-linked endpoint delivery=%q", got)
	}
	publication.Cleanup()
	for _, path := range []string{logicalPath, publication.privatePath} {
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("publication cleanup retained %s: %v", path, err)
		}
	}
}

func TestCleanupCannotRemoveReplacementOwner(t *testing.T) {
	logicalPath := filepath.Join(t.TempDir(), "live.sock")
	publication, err := Listen(logicalPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(logicalPath); err != nil {
		publication.Cleanup()
		t.Fatal(err)
	}
	replacement, err := net.Listen("unix", logicalPath)
	if err != nil {
		publication.Cleanup()
		t.Fatal(err)
	}
	defer replacement.Close()
	publication.Cleanup()
	info, err := os.Lstat(logicalPath)
	if err != nil || info.Mode()&os.ModeSocket == 0 {
		t.Fatalf("old owner removed replacement endpoint: info=%v err=%v", info, err)
	}
	if _, err := os.Lstat(publication.privatePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("old private endpoint survived cleanup: %v", err)
	}
}

func TestPublicationRejectsLiveOrUnsafeOwnersAndReclaimsStaleSocket(t *testing.T) {
	t.Run("live owner", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "live.sock")
		listener, err := net.Listen("unix", path)
		if err != nil {
			t.Fatal(err)
		}
		defer listener.Close()
		if _, err := Listen(path); err == nil || !strings.Contains(err.Error(), "live listener") {
			t.Fatalf("live endpoint owner was replaced: %v", err)
		}
	})
	t.Run("non socket", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "live.sock")
		if err := os.WriteFile(path, []byte("foreign"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := Listen(path); err == nil || !strings.Contains(err.Error(), "non-socket") {
			t.Fatalf("non-socket endpoint owner was replaced: %v", err)
		}
	})
	t.Run("stale socket", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "live.sock")
		listener, err := net.Listen("unix", path)
		if err != nil {
			t.Fatal(err)
		}
		unixListener := listener.(*net.UnixListener)
		unixListener.SetUnlinkOnClose(false)
		if err := listener.Close(); err != nil {
			t.Fatal(err)
		}
		publication, err := Listen(path)
		if err != nil {
			t.Fatalf("stale socket was not reclaimed: %v", err)
		}
		publication.Cleanup()
	})
	t.Run("crashed publication", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "live.sock")
		crashed, err := Listen(path)
		if err != nil {
			t.Fatal(err)
		}
		crashed.listener.(*net.UnixListener).SetUnlinkOnClose(false)
		if err := crashed.listener.Close(); err != nil {
			t.Fatal(err)
		}
		oldPrivatePath := crashed.privatePath
		replacement, err := Listen(path)
		if err != nil {
			t.Fatalf("crashed publication was not reclaimed: %v", err)
		}
		defer replacement.Cleanup()
		if _, err := os.Lstat(oldPrivatePath); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("crashed private socket survived recovery: %v", err)
		}
	})
}

func TestPublicationRecoversAfterProcessCrashDuringPrepareOrCutover(t *testing.T) {
	for _, phase := range []string{"prepare", "cutover"} {
		t.Run(phase, func(t *testing.T) {
			directory := t.TempDir()
			logicalPath := filepath.Join(directory, "analysis.sock")
			stagePath := filepath.Join(directory, ".vb-fanout-crash-stage")
			command := exec.Command(os.Args[0], "-test.run=^TestPublicationProcessCrashHelper$")
			command.Env = append(os.Environ(),
				"VIDEO_BROKER_PUBLICATION_CRASH_HELPER=1",
				"VIDEO_BROKER_PUBLICATION_CRASH_PHASE="+phase,
				"VIDEO_BROKER_PUBLICATION_CRASH_LOGICAL="+logicalPath,
				"VIDEO_BROKER_PUBLICATION_CRASH_STAGE="+stagePath,
			)
			output, runErr := command.CombinedOutput()
			var exitErr *exec.ExitError
			if !errors.As(runErr, &exitErr) || exitErr.ExitCode() != 87 {
				t.Fatalf("%s crash helper exit=%v output=%s", phase, runErr, output)
			}

			// A restarted broker materializes the still-persisted current graph.
			// Its listener must replace whichever dead inode the crashed process
			// left at the stable endpoint, including a candidate after cutover.
			recovered, err := Listen(logicalPath)
			if err != nil {
				t.Fatalf("recover logical endpoint after %s crash: %v", phase, err)
			}
			assertPublicationAcceptsPath(t, recovered, logicalPath)
			recovered.Cleanup()

			if phase == "prepare" {
				staged, err := Listen(stagePath)
				if err != nil {
					t.Fatalf("recover private candidate after prepare crash: %v", err)
				}
				staged.Cleanup()
			} else if _, err := os.Lstat(stagePath); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("cutover retained the public staging alias: %v", err)
			}
		})
	}
}

func TestPublicationProcessCrashHelper(t *testing.T) {
	if os.Getenv("VIDEO_BROKER_PUBLICATION_CRASH_HELPER") != "1" {
		return
	}
	phase := os.Getenv("VIDEO_BROKER_PUBLICATION_CRASH_PHASE")
	logicalPath := os.Getenv("VIDEO_BROKER_PUBLICATION_CRASH_LOGICAL")
	stagePath := os.Getenv("VIDEO_BROKER_PUBLICATION_CRASH_STAGE")
	incumbent, err := Listen(logicalPath)
	if err != nil {
		t.Fatal(err)
	}
	successor, err := Listen(stagePath)
	if err != nil {
		t.Fatal(err)
	}
	if phase == "cutover" {
		if err := successor.Replace(incumbent); err != nil {
			t.Fatal(err)
		}
	} else if phase != "prepare" {
		t.Fatalf("unknown crash phase %q", phase)
	}
	// Deliberately bypass Cleanup to model SIGKILL/power loss ownership.
	os.Exit(87)
}

func TestPublicationReplaceAndRollbackAreIdentityBound(t *testing.T) {
	directory := t.TempDir()
	logicalPath := filepath.Join(directory, "live.sock")
	stagingPath := filepath.Join(directory, "candidate.sock")
	incumbent, err := Listen(logicalPath)
	if err != nil {
		t.Fatal(err)
	}
	defer incumbent.Cleanup()
	successor, err := Listen(stagingPath)
	if err != nil {
		t.Fatal(err)
	}
	defer successor.Cleanup()

	if err := successor.Replace(incumbent); err != nil {
		t.Fatal(err)
	}
	if !successor.Owns(logicalPath) || incumbent.Owns(logicalPath) {
		t.Fatal("successor did not acquire the logical endpoint")
	}
	assertPublicationAcceptsPath(t, successor, logicalPath)
	if _, err := os.Lstat(stagingPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("successor staging alias survived cutover: %v", err)
	}
	if err := incumbent.Replace(successor); err != nil {
		t.Fatal(err)
	}
	if !incumbent.Owns(logicalPath) || successor.Owns(logicalPath) {
		t.Fatal("incumbent did not reacquire the logical endpoint")
	}
	assertPublicationAcceptsPath(t, incumbent, logicalPath)
}

func assertPublicationAcceptsPath(t *testing.T, publication *Publication, path string) {
	t.Helper()
	accepted := make(chan error, 1)
	go func() {
		connection, err := publication.Accept()
		if err == nil {
			err = connection.Close()
		}
		accepted <- err
	}()
	connection, err := net.DialTimeout("unix", path, time.Second)
	if err != nil {
		t.Fatalf("dial replaced publication %s: %v", path, err)
	}
	_ = connection.Close()
	select {
	case err := <-accepted:
		if err != nil {
			t.Fatalf("accept replaced publication %s: %v", path, err)
		}
	case <-time.After(time.Second):
		t.Fatalf("replaced publication did not accept %s", path)
	}
}

func TestPublicationReplaceRejectsChangedIncumbentOwner(t *testing.T) {
	directory := t.TempDir()
	logicalPath := filepath.Join(directory, "live.sock")
	incumbent, err := Listen(logicalPath)
	if err != nil {
		t.Fatal(err)
	}
	defer incumbent.Cleanup()
	successor, err := Listen(filepath.Join(directory, "candidate.sock"))
	if err != nil {
		t.Fatal(err)
	}
	defer successor.Cleanup()
	if err := os.Remove(logicalPath); err != nil {
		t.Fatal(err)
	}
	foreign, err := net.Listen("unix", logicalPath)
	if err != nil {
		t.Fatal(err)
	}
	defer foreign.Close()
	if err := successor.Replace(incumbent); err == nil ||
		!strings.Contains(err.Error(), "changed owner") {
		t.Fatalf("foreign endpoint was replaced: %v", err)
	}
}
