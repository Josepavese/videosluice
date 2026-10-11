//go:build linux

package main

import (
	"bufio"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	fd "github.com/Josepavese/videosluice/framedomain"
	in "github.com/Josepavese/videosluice/frameingress"
	linux "github.com/Josepavese/videosluice/framepeer/provider/linux"
	"golang.org/x/sys/unix"
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func TestNativeDaemonBootstrapChild(t *testing.T) {
	if os.Getenv("FAP_NATIVE_DAEMON_CHILD") != "1" {
		return
	}
	// A synthetic fixture admission, NEVER the default product resolver.
	open := nativeFrameOpener(nativeFrameAdmission{limits: fd.Limits{MaxSources: 1, MaxStreams: 1, MaxSourceGenerations: 2, MaxMemoryBytes: 2 << 20, MaxSourceBytes: 4096, MaxBuffers: 4, MaxBufferBytes: 4096, MaxResources: 8, MaxResourceOperations: 4}, bytes: 1 << 20, controlBytes: 65536, workspaceBytes: 32768, transportBytes: 4096, maxGrants: 4})
	mode := os.Getenv("FAP_NATIVE_DAEMON_MODE")
	if mode == "control_loop" || mode == "active_close" {
		metadata, err := hex.DecodeString(os.Getenv("FAP_NATIVE_DAEMON_METADATA"))
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		owner, err := open(ctx, frameLaunchInput{SocketFD: 3, ControllerFD: 4, ControllerMetadata: metadata})
		if err != nil {
			t.Fatal(err)
		}
		owned := owner.(*nativeFrameBootstrap)
		defer owned.Close()
		done := make(chan error, 1)
		go func() { done <- owned.Run(ctx) }()
		input := bufio.NewReader(os.Stdin)
		fmt.Println("control-loop-ready")
		if _, err = input.ReadByte(); err != nil {
			t.Fatal(err)
		}
		if mode == "active_close" {
			if err = owned.Close(); err != nil {
				t.Fatal("active bootstrap close failed", err)
			}
		} else {
			cancel()
		}
		select {
		case err = <-done:
			if err == nil {
				t.Fatal("stopped loop returned success")
			}
		case <-time.After(time.Second):
			t.Fatal("control loop failed to join")
		}
		if mode == "control_loop" {
			if !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			if stats := owned.runtime.Stats(); stats.LiveResources != 1 || stats.ReservedResourceBytes != 1<<20 {
				t.Fatal("dependent resources outlived run", stats)
			}
			if err = owned.resource.Borrow(context.Background(), func(_ context.Context, launch *linux.AcceptedLaunch) error {
				_, err := launch.Controller.CurrentGeneration()
				return err
			}); err != nil {
				t.Fatal("controller authority retired early", err)
			}
		}
		if err = owned.Close(); err != nil {
			t.Fatal("duplicate close or cleanup failure", err)
		}
		if stats := owned.runtime.Stats(); stats.LiveResources != 0 || stats.ReservedMemoryBytes != 0 {
			t.Fatal(stats)
		}
		fmt.Println("authenticated-and-revoked")
		if _, err = input.ReadByte(); err != nil {
			t.Fatal(err)
		}
		return
	}
	var owned *nativeFrameBootstrap
	args := []string{"--frame-launch-socket-fd", "3", "--frame-launch-controller-fd", "4", "--frame-launch-controller-metadata", os.Getenv("FAP_NATIVE_DAEMON_METADATA")}
	err := runDaemon(context.Background(), args, io.Discard, func(ctx context.Context, input frameLaunchInput) (frameBootstrapOwner, error) {
		owner, err := open(ctx, input)
		if err != nil {
			return owner, err
		}
		owned = owner.(*nativeFrameBootstrap)
		if owned.runtime.Fence().ProcessGeneration != "native-cli-child" || owned.runtime.Stats().ReservedResourceBytes != 1<<20 {
			t.Fatal("CLI lost authenticated admission", owned.runtime.Fence(), owned.runtime.Stats())
		}
		if err = owned.resource.Borrow(ctx, func(_ context.Context, launch *linux.AcceptedLaunch) error {
			generation, err := launch.Self.CurrentGeneration()
			if err != nil || generation != "native-cli-child" {
				t.Fatal(generation, err)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		return owner, nil
	})
	if err == nil || !strings.Contains(err.Error(), "load configuration") || owned == nil {
		t.Fatal("CLI did not authenticate before config failure", err)
	}
	if stats := owned.runtime.Stats(); !stats.Revoked || stats.LiveResources != 0 || stats.ReservedMemoryBytes != 0 {
		t.Fatal("CLI left bootstrap resources after failure", stats)
	}
	if err = owned.Close(); err != nil {
		t.Fatal("CLI bootstrap close not idempotent", err)
	}
	fmt.Println("authenticated-and-revoked")
	// Keep this exact execution alive until the parent has completed Deliver's
	// final liveness check; no status delay or arbitrary sleep is used.
	if _, err = bufio.NewReader(os.Stdin).ReadByte(); err != nil {
		t.Fatal(err)
	}
}

func TestNativeDaemonCLIAuthenticatesAndReleasesBeforeExit(t *testing.T) {
	for _, mode := range []string{"config_failure", "control_loop", "active_close"} {
		t.Run(mode, func(t *testing.T) {
			pidfd, err := unix.PidfdOpen(os.Getpid(), 0)
			if err != nil {
				t.Fatal(err)
			}
			file := os.NewFile(uintptr(pidfd), "fixture-controller-self")
			controller, err := linux.ImportProcessCapability(file, linux.ProcessIdentity{PID: os.Getpid(), UID: uint32(os.Getuid()), GID: uint32(os.Getgid()), Generation: "native-cli-controller"})
			if err != nil {
				file.Close()
				t.Fatal(err)
			}
			defer controller.Close()
			launch, err := linux.PrepareLaunch(controller)
			if launch != nil {
				defer launch.Close()
			}
			if err != nil {
				t.Fatal(err)
			}
			files, metadata, err := launch.Inherited()
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestNativeDaemonBootstrapChild$")
			command.Env = append(os.Environ(), "FAP_NATIVE_DAEMON_CHILD=1", "FAP_NATIVE_DAEMON_METADATA="+hex.EncodeToString(metadata), "FAP_NATIVE_DAEMON_MODE="+mode)
			command.ExtraFiles = files[:]
			command.Stderr = os.Stderr
			input, err := command.StdinPipe()
			if err != nil {
				t.Fatal(err)
			}
			defer input.Close()
			output, err := command.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			if err = command.Start(); err != nil {
				t.Fatal(err)
			}
			waited := false
			defer func() {
				if !waited {
					cancel()
					command.Wait()
				}
			}()
			if err = launch.CloseInherited(); err != nil {
				t.Fatal(err)
			}
			connection, err := launch.Deliver(ctx, command.Process, "native-cli-child", uint32(os.Getuid()), uint32(os.Getgid()), time.Second)
			if err != nil {
				t.Fatal(err)
			}
			scanner := bufio.NewScanner(output)
			if mode != "config_failure" {
				if err = in.AwaitBootstrapReady(ctx, connection, "native-cli-child", time.Second); err != nil {
					t.Fatal(err)
				}
				if !scanner.Scan() || scanner.Text() != "control-loop-ready" {
					t.Fatal(scanner.Text(), scanner.Err())
				}
				var query [in.DiscoveryQueryBytes]byte
				var response [in.MaxControlReplyBytes]byte
				var refs [fd.MaxBindingReferences]fd.BindingRef
				challenge := [16]byte{8}
				n, err := in.EncodeDiscoveryQueryInto(query[:], challenge)
				if err != nil {
					t.Fatal(err)
				}
				if err = connection.WriteCommand(ctx, query[:n], nil); err != nil {
					t.Fatal(err)
				}
				n, sender, err := connection.Receive(ctx, response[:])
				if err != nil {
					t.Fatal(err)
				}
				catalog, err := in.DecodeDiscoveryCatalogInto(response[:n], refs[:])
				if err != nil {
					t.Fatal(err)
				}
				if err = in.ValidateDiscoveryCatalog(catalog, challenge, "native-cli-child", sender); err != nil {
					t.Fatal(err)
				}
				if len(catalog.References) != 0 {
					t.Fatal("bootstrap fabricated camera binding")
				}
				request := in.ControlCommand{Kind: in.ControlRevoke, Header: in.ControlHeader{Target: catalog.Broker, ID: [16]byte{9}, Sequence: 1, Grant: "reader"}}
				packet, err := in.EncodeControl(request)
				if err != nil {
					t.Fatal(err)
				}
				if err = connection.WriteCommand(ctx, packet, nil); err != nil {
					t.Fatal(err)
				}
				n, sender, err = connection.Receive(ctx, response[:])
				if err != nil || sender.Generation != "native-cli-child" {
					t.Fatal(sender, err)
				}
				reply, err := in.DecodeControlReply(response[:n])
				if err != nil {
					t.Fatal(err)
				}
				if err = in.ValidateControlReply(reply, request); err != nil {
					t.Fatal(err)
				}
				if reply.Code != in.ReplyOK || reply.Receipt.Outcome != "absent" {
					t.Fatal(reply)
				}
				if _, err = input.Write([]byte{'\n'}); err != nil {
					t.Fatal(err)
				}
			}
			if !scanner.Scan() || scanner.Text() != "authenticated-and-revoked" {
				t.Fatal("CLI lifecycle completion missing", scanner.Text(), scanner.Err())
			}
			if _, err = input.Write([]byte{'\n'}); err != nil {
				t.Fatal(err)
			}
			err = command.Wait()
			waited = true
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}
