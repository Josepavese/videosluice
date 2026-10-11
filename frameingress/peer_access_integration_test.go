//go:build linux

package ingress_test

import (
	"bufio"
	"context"
	"encoding/hex"
	in "github.com/Josepavese/videosluice/frameingress"
	"fmt"
	fd "github.com/Josepavese/videosluice/framedomain"
	peer "github.com/Josepavese/videosluice/framepeer"
	linux "github.com/Josepavese/videosluice/framepeer/provider/linux"
	"golang.org/x/sys/unix"
	"net"
	"os"
	"os/exec"
	"testing"
	"time"
)

func TestAccessPeerChild(t *testing.T) {
	if os.Getenv("FAP_ACCESS_CHILD") != "1" {
		return
	}
	file := os.NewFile(3, "fixture-request")
	defer file.Close()
	scan := bufio.NewScanner(os.Stdin)
	for scan.Scan() {
		packet, err := hex.DecodeString(scan.Text())
		if err != nil {
			os.Exit(4)
		}
		if _, err := file.Write(packet); err != nil {
			os.Exit(3)
		}
	}
}

// Stdin hex is fixture coordination only. The real authenticated socket carries
// exactly the bounded binary frame-request protocol, not JSON/PID assertions.
func TestRealPeerPrincipalFeedsACLAndNativeExtraction(t *testing.T) {
	f, _, _, q := accessFixture(t)
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
	must(t, err)
	left, right := os.NewFile(uintptr(fds[0]), "receiver"), os.NewFile(uintptr(fds[1]), "sender")
	defer right.Close()
	connection, err := net.FileConn(left)
	_ = left.Close()
	must(t, err)
	defer connection.Close()
	cmd := exec.Command(os.Args[0], "-test.run=^TestAccessPeerChild$")
	cmd.Env = append(os.Environ(), "FAP_ACCESS_CHILD=1")
	cmd.Stderr = os.Stderr
	cmd.ExtraFiles = []*os.File{right}
	input, err := cmd.StdinPipe()
	must(t, err)
	must(t, cmd.Start())
	defer func() { _ = input.Close(); _ = cmd.Process.Kill(); _ = cmd.Wait() }()
	lease, err := linux.BindStartedProcess(cmd.Process, "real-consumer-start", uint32(os.Getuid()), uint32(os.Getgid()))
	must(t, err)
	receiver, err := linux.NewReceiver(connection.(*net.UnixConn), lease, 4096, time.Second)
	must(t, err)
	a, err := in.NewAccess(f.r, f.b, lease, in.AccessPolicy{Generation: "real-consumer-start", Streams: []string{"analysis"}, MaxRegions: 1, MaxRequests: 1, MaxResults: 1})
	must(t, err)
	defer a.Revoke()
	for _, allowed := range []bool{true, false} {
		request := in.FrameRequest{ID: q.ID, Frame: q.Frame, Source: q.Source, Surface: q.Surface, Regions: q.Regions, BudgetNS: 100}
		if !allowed {
			request.Source = "rear"
		}
		packet, err := in.EncodeFrameRequest(request)
		must(t, err)
		_, err = fmt.Fprintln(input, hex.EncodeToString(packet))
		must(t, err)
		buffer := make([]byte, 4096)
		n, principal, err := receiver.Receive(context.Background(), buffer)
		must(t, err)
		result, err := a.ExtractPacket(context.Background(), principal, buffer[:n])
		if !allowed {
			want(t, err, peer.ErrUnauthorized)
			if result != nil {
				result.Close()
				t.Fatal("foreign source result")
			}
			continue
		}
		must(t, err)
		must(t, result.Visit(0, func(v fd.Artifact, p []byte) error {
			if len(p) != 24 {
				t.Fatal("wrong native ROI")
			}
			return fd.VerifyDomainProof(v.Proof, f.b.Fence())
		}))
		result.Close()
	}
}
