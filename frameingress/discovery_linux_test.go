//go:build linux

package ingress_test

import (
	"bufio"
	"context"
	"errors"
	in "github.com/Josepavese/videosluice/frameingress"
	"fmt"
	fd "github.com/Josepavese/videosluice/framedomain"
	peer "github.com/Josepavese/videosluice/framepeer"
	linux "github.com/Josepavese/videosluice/framepeer/provider/linux"
	"golang.org/x/sys/unix"
	"net"
	"os"
	"strings"
	"testing"
	"time"
)

func TestDiscoveryControllerChild(t *testing.T) {
	if os.Getenv("FAP_DISCOVERY_CHILD") != "1" {
		return
	}
	input := bufio.NewReader(os.Stdin)
	mode, err := input.ReadString('\n')
	must(t, err)
	mode = strings.TrimSpace(mode)
	file := os.NewFile(3, "discovery-channel")
	conn, err := net.FileConn(file)
	file.Close()
	must(t, err)
	defer conn.Close()
	capability := os.NewFile(4, "trusted-broker-capability")
	broker, err := linux.ImportProcessCapability(capability, linux.ProcessIdentity{PID: os.Getppid(), UID: uint32(os.Getuid()), GID: uint32(os.Getgid()), Generation: "authenticated-process-1"})
	if err != nil {
		capability.Close()
		t.Fatal(err)
	}
	defer broker.Close()
	client, err := linux.NewControlConnection(conn.(*net.UnixConn), broker, time.Second)
	must(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	if mode == "client_retire" || mode == "client_replay" || mode == "session_retire" {
		f := setup(t)
		defer f.r.Revoke()
		sdk, workspace := discoveryAdmittedFixture(t, f, client, "authenticated-process-1")
		for i := 1; i <= 2; i++ {
			visits := 0
			out, err := sdk.Discover(ctx, workspace, func(catalog in.DiscoveryCatalog) error {
				visits++
				if i == 1 && (len(catalog.References) != 1 || catalog.References[0].Source != "front" || catalog.References[0].Epoch != 1) {
					t.Fatal(catalog)
				}
				if i == 2 && len(catalog.References) != 0 {
					t.Fatal("retired native binding exported")
				}
				return nil
			})
			if i == 2 && mode == "client_replay" {
				if !errors.Is(err, peer.ErrUnauthorized) || visits != 0 || out.ResponseReceived || !sdk.Stats().Closed {
					t.Fatal("native replay accepted", out, err)
				}
				continue
			}
			must(t, err)
			if _, ok := sdk.BoundTarget(); !ok || !out.ResponseReceived || visits != 1 {
				t.Fatal("native discovery not committed")
			}
		}
		if mode == "session_retire" {
			target, ok := sdk.BoundTarget()
			if !ok {
				t.Fatal("missing authenticated target")
			}
			q := in.ControlCommand{Kind: in.ControlRevoke, Header: in.ControlHeader{Target: target, Sequence: 1, ID: [16]byte{3}, Grant: "reader"}}
			out, err := sdk.Exchange(ctx, q, nil)
			must(t, err)
			if !out.ResponseReceived || out.Reply.Code != in.ReplyOK || out.Reply.Receipt.Outcome != "absent" {
				t.Fatal(out)
			}
			q = in.ControlCommand{Kind: in.ControlStatus, Header: in.ControlHeader{Target: target, ID: [16]byte{4}}}
			out, err = sdk.Exchange(ctx, q, nil)
			must(t, err)
			if !out.ResponseReceived || out.Reply.Code != in.ReplyOK || out.Reply.Stats == nil || out.Reply.Stats.Receipt.Sequence != 1 {
				t.Fatal(out)
			}
		}
		sdk.Close()
		must(t, sdk.WaitClosed(ctx))
		fmt.Println("discovery-verified")
		_, _ = input.ReadByte()
		return
	}
	var request [in.DiscoveryQueryBytes]byte
	var response [in.MaxControlReplyBytes]byte
	var refs [fd.MaxBindingReferences]fd.BindingRef
	for i := byte(1); i <= 2; i++ {
		challenge := [16]byte{i}
		n, err := in.EncodeDiscoveryQueryInto(request[:], challenge)
		must(t, err)
		var capability peer.ProcessCapability
		if mode == "capability" && i == 2 {
			capability = broker
		}
		must(t, client.WriteCommand(ctx, request[:n], capability))
		n, sender, err := client.Receive(ctx, response[:])
		if mode == "capability" && i == 2 {
			if err == nil {
				t.Fatal("unsolicited capability obtained discovery reply")
			}
			_, err = broker.CurrentGeneration()
			must(t, err)
			continue
		}
		must(t, err)
		catalog, err := in.DecodeDiscoveryCatalogInto(response[:n], refs[:])
		must(t, err)
		err = in.ValidateDiscoveryCatalog(catalog, challenge, "authenticated-process-1", sender)
		if mode == "replay" && i == 2 {
			if !errors.Is(err, peer.ErrUnauthorized) {
				t.Fatal("stale authenticated catalog accepted", err)
			}
			continue
		}
		must(t, err)
		if i == 1 && (len(catalog.References) != 1 || catalog.References[0].Source != "front" || catalog.References[0].Epoch != 1) {
			t.Fatal(catalog)
		}
		if i == 2 && len(catalog.References) != 0 {
			t.Fatal("retired source still advertised")
		}
	}
	fmt.Println("discovery-verified")
	// The fixture parent owns termination/reaping. Stay alive through its final
	// send liveness check; no timing assumptions or sleeps.
	_, _ = input.ReadByte()
}

func TestDiscoveryAcrossAuthenticatedProcessesAndSourceRetirement(t *testing.T) {
	for _, mode := range []string{"retire", "replay", "capability", "client_retire", "client_replay", "session_retire"} {
		t.Run(mode, func(t *testing.T) {
			f := setup(t)
			defer f.r.Revoke()
			conn, childSocket := integrationPair(t)
			pidfd, err := unix.PidfdOpen(os.Getpid(), 0)
			must(t, err)
			brokerFile := os.NewFile(uintptr(pidfd), "fixture-broker-self")
			defer brokerFile.Close()
			child, input, output := integrationChild(t, "TestDiscoveryControllerChild", "FAP_DISCOVERY_CHILD", []*os.File{childSocket, brokerFile})
			_, err = fmt.Fprintln(input, mode)
			must(t, err)
			controller, err := linux.BindStartedProcess(child.Process, "controller-start-1", uint32(os.Getuid()), uint32(os.Getgid()))
			must(t, err)
			defer controller.Close()
			if mode == "session_retire" {
				// Full client/server lifecycle over native transport. Quotes remain
				// synthetic fixture values, not a Linux footprint qualification.
				owner, err := in.OwnControlDispatcher(context.Background(), f.r, controller, "controller-start-1", 2, 65536)
				must(t, err)
				defer owner.Close()
				receiver, err := linux.NewControlReceiver(conn, controller, 4096, time.Second)
				must(t, err)
				session, workspace := serverSessionFixture(t, f, owner, receiver)
				ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
				defer cancel()
				for i := 0; i < 4; i++ {
					completed, err := session.Step(ctx)
					must(t, err)
					if !completed {
						t.Fatal("native server did not complete discovery")
					}
					if i == 0 {
						f.b.Close()
					}
				}
				line, err := output.ReadString('\n')
				must(t, err)
				if line != "discovery-verified\n" {
					t.Fatal(line)
				}
				session.Close()
				must(t, session.WaitClosed(ctx))
				workspace.Close()
				must(t, workspace.WaitClosed(ctx))
				owner.Close()
				must(t, owner.WaitClosed(ctx))
				if f.r.Stats().ReservedResourceBytes != 0 {
					t.Fatal("native server resource residue", f.r.Stats())
				}
				return
			}
			control, err := in.NewGrantControl(f.r, controller, "controller-start-1", 2)
			must(t, err)
			defer control.Close()
			dispatcher, err := in.NewControlDispatcher(control)
			must(t, err)
			defer dispatcher.Close()
			receiver, err := linux.NewControlReceiver(conn, controller, 4096, time.Second)
			must(t, err)
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
			defer cancel()
			var query [in.MaxControlBytes]byte
			var packet [in.MaxDiscoveryBytes]byte
			var refs [fd.MaxBindingReferences]fd.BindingRef
			var first []byte
			for i := 1; i <= 2; i++ {
				n, principal, raw, err := receiver.Receive(ctx, query[:])
				must(t, err)
				if mode == "capability" && i == 2 {
					if raw == nil {
						t.Fatal("native capability fixture missing")
					}
					pending, err := linux.OwnReceivedCapability(raw)
					must(t, err)
					_, accepted, err := dispatcher.Discover(ctx, principal, query[:n], refs[:], pending)
					if !accepted || !errors.Is(err, peer.ErrInvalid) {
						t.Fatal("unsolicited native capability accepted", accepted, err)
					}
					if _, err = raw.Stat(); err == nil {
						t.Fatal("rejected native descriptor leaked")
					}
					must(t, conn.Close())
					continue
				}
				if raw != nil {
					raw.Close()
					t.Fatal("discovery unexpectedly carried capability")
				}
				catalog, accepted, err := dispatcher.Discover(ctx, principal, query[:n], refs[:], nil)
				must(t, err)
				if !accepted {
					t.Fatal("discovery not admitted")
				}
				n, err = in.EncodeDiscoveryCatalogInto(packet[:], catalog)
				must(t, err)
				if i == 1 {
					first = append([]byte(nil), packet[:n]...)
				}
				if i == 2 && (mode == "replay" || mode == "client_replay") {
					must(t, receiver.Reply(ctx, first))
				} else {
					must(t, receiver.Reply(ctx, packet[:n]))
				}
				if i == 1 {
					f.b.Close()
				}
			}
			line, err := output.ReadString('\n')
			must(t, err)
			if line != "discovery-verified\n" {
				t.Fatal(line)
			}
		})
	}
}
