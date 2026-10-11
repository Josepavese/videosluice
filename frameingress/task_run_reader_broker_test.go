//go:build linux && nemesi_integration

package ingress_test

import (
	"context"
	"encoding/hex"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/Josepavese/videosluice/frameaccess/copyport"
	fd "github.com/Josepavese/videosluice/framedomain"
	in "github.com/Josepavese/videosluice/frameingress"
	peer "github.com/Josepavese/videosluice/framepeer"
	pl "github.com/Josepavese/videosluice/framepeer/provider/linux"
	"golang.org/x/sys/unix"
)

// The actual supervisor Task.Run launches this broker fixture. It reuses the
// native SDK/source fixtures, not a second protocol implementation. Only camera
// pixels and admission quotes are synthetic; process and grant control are real.
func TestTaskRunReaderBrokerProcess(t *testing.T) {
	if os.Getenv("FAP_TASK_BROKER") != "1" {
		return
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	fixtureBudget := 30 * time.Second
	if os.Getenv("FAP_SLOW_HARDWARE_START") == "timeout" {
		fixtureBudget = 60 * time.Second // outlive the real launcher's 30s wait
	}
	ctx, cancel := context.WithTimeout(ctx, fixtureBudget)
	defer cancel()
	flags := flag.NewFlagSet("task-broker", flag.ContinueOnError)
	flags.String("socket", "", "Task IPC")
	socket := flags.Int("frame-launch-socket-fd", -1, "launch")
	controller := flags.Int("frame-launch-controller-fd", -1, "controller")
	encoded := flags.String("frame-launch-controller-metadata", "", "identity")
	must(t, flags.Parse(flag.Args()))
	metadata, err := hex.DecodeString(*encoded)
	must(t, err)
	launch, err := pl.AcceptLaunch(ctx, os.NewFile(uintptr(*socket), "launch"), os.NewFile(uintptr(*controller), "controller"), metadata, time.Second)
	must(t, err)
	defer launch.Close()
	if slow := os.Getenv("FAP_SLOW_HARDWARE_START"); slow != "" {
		// Reproduce discovery slower than one normal control-exchange budget.
		// Parent must wait before execing its consumer, not poison this channel.
		select {
		case <-time.After(func() time.Duration {
			if slow == "timeout" {
				return 31500 * time.Millisecond
			}
			return 1500 * time.Millisecond
		}()):
		case <-ctx.Done():
			return
		}
	}
	f := setupGeometryAuthorityCopyBudget(t, copyport.CPU{}, launch.Self, 16, 16, uint64(time.Second))
	ticket := f.ticket(t)
	must(t, f.i.AcceptSurface(ctx, f.surface(t, 1), 100, 200))
	must(t, f.i.AcceptDelivery(ctx, ticket, f.delivery(t, ticket, 1)))
	p := deliveryPublication(t, f)
	h, pixels := publicationFrame(t, f, 1)
	h.DeliveryID = ticket.Ref().Ref.FrameID
	must(t, p.Publish(ctx, h, pixels))
	generation, err := launch.Controller.CurrentGeneration()
	must(t, err)
	control, err := in.OwnControlDispatcher(ctx, f.r, launch.Controller, generation, 4, 16384)
	must(t, err)
	defer func() { control.Close(); must(t, control.WaitClosed(context.Background())) }()
	connection := &taskBrokerTransport{ControlServerConnection: launch.Control, owner: launch, drop: os.Getenv("FAP_DROP_GRANT_ACK") == "1"}
	session, workspace := serverSessionFixture(t, f, control, connection)
	defer func() {
		session.Close()
		must(t, session.WaitClosed(context.Background()))
		workspace.Close()
		must(t, workspace.WaitClosed(context.Background()))
	}()
	var readers sync.WaitGroup
	defer readers.Wait()
	defer cancel()
	for index, name := range []string{"frame-delivery.sock", "frame-roi.sock"} {
		listener, err := net.ListenUnix("unixpacket", &net.UnixAddr{Name: filepath.Join(os.Getenv("FAP_ENDPOINT_DIR"), name), Net: "unixpacket"})
		must(t, err)
		defer listener.Close()
		raw, err := listener.SyscallConn()
		must(t, err)
		var setupErr error
		must(t, raw.Control(func(handle uintptr) {
			setupErr = unix.SetsockoptInt(int(handle), unix.SOL_SOCKET, unix.SO_PASSCRED, 1)
			if setupErr == nil {
				setupErr = unix.Listen(int(handle), 1)
			}
		}))
		must(t, setupErr)
		var publication *in.DeliveryPublication
		if index == 0 {
			publication = p
		}
		var selectDelivery func(fd.BindingRef) (*in.DeliveryPublication, error)
		if publication != nil {
			selectDelivery = func(fd.BindingRef) (*in.DeliveryPublication, error) { return publication, nil }
		}
		readers.Add(1)
		go func() {
			defer readers.Done()
			for ctx.Err() == nil {
				var reader *in.FrameSession
				var partial *fd.Resource[peer.Connection]
				err := control.Borrow(ctx, func(ctx context.Context, dispatch *in.ControlDispatcher) error {
					var err error
					reader, partial, err = dispatch.AcceptReaderSession(ctx, "tracker", selectDelivery, in.ReaderAcceptPlan{Bytes: 4096,
						Accept: func(context.Context) (*net.UnixConn, error) {
							listener.SetDeadline(time.Now().Add(250 * time.Millisecond))
							return listener.AcceptUnix()
						}}, 1024, uint64(time.Second))
					return err
				})
				if partial != nil {
					partial.Close()
					if closeErr := partial.WaitClosed(context.Background()); closeErr != nil {
						t.Error(closeErr)
						return
					}
				}
				if err != nil {
					if ctx.Err() == nil {
						time.Sleep(10 * time.Millisecond)
					}
					continue
				}
				for ctx.Err() == nil {
					if _, err = reader.Step(ctx); err != nil {
						break
					}
				}
				reader.Close()
				if err := reader.WaitClosed(context.Background()); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	must(t, os.WriteFile(os.Getenv("FAP_BROKER_READY"), []byte(f.r.Fence().ProcessGeneration), 0600))
	must(t, in.SendBootstrapReady(ctx, launch.Control, generation))
	for ctx.Err() == nil {
		if _, err = session.Step(ctx); err != nil {
			if !connection.drop && ctx.Err() == nil {
				t.Error(err)
			}
			if connection.drop {
				<-ctx.Done()
			} // keep process alive; parent must retire its own channel
			return
		}
	}
}

type taskBrokerTransport struct {
	peer.ControlServerConnection
	owner *pl.AcceptedLaunch
	drop  bool
}

func (c *taskBrokerTransport) Close() error { return c.owner.CloseControl() }
func (c *taskBrokerTransport) Reply(ctx context.Context, packet []byte) error {
	if reply, err := in.DecodeControlReply(packet); err == nil && reply.Kind == in.ControlGrant && reply.Receipt.Outcome == "installed" {
		if err := os.WriteFile(os.Getenv("FAP_GRANT_MARKER"), []byte(strconv.FormatUint(reply.Receipt.Sequence, 10)), 0600); err != nil {
			return err
		}
		if c.drop {
			// Commit succeeds but ACK arrives beyond the client's one-second
			// deadline. The retired physical socket must reject this late reply.
			// Deliberately ignore the deadline ONLY in this fault injection:
			// emulate a provider that finishes an already committed reply late.
			time.Sleep(1250 * time.Millisecond)
			err := c.ControlServerConnection.Reply(context.Background(), packet)
			if markerErr := os.WriteFile(os.Getenv("FAP_GRANT_MARKER")+".late", []byte(fmt.Sprint(err)), 0600); markerErr != nil {
				return markerErr
			}
			return err
		}
	}
	return c.ControlServerConnection.Reply(ctx, packet)
}
