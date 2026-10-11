package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Josepavese/videosluice/internal/source"
	"github.com/Josepavese/videosluice/routing"
)

type discoveryFrameOwner struct{ running, joined, closed chan struct{} }

func (o *discoveryFrameOwner) Run(ctx context.Context) error {
	close(o.running)
	<-ctx.Done()
	close(o.joined)
	return ctx.Err()
}
func (o *discoveryFrameOwner) Close() error {
	select {
	case <-o.joined:
	default:
		return errors.New("bootstrap closed before control loop joined")
	}
	close(o.closed)
	return nil
}
func (*discoveryFrameOwner) FrameCaptures() source.FrameCaptureFactory {
	return func(context.Context, source.BrokerMuxPlan) (source.FrameCapture, error) {
		return nil, source.ErrFrameCaptureNotRequested // fixture has only live
	}
}

func TestDaemonServesFrameControlDuringSlowDiscovery(t *testing.T) {
	dir := t.TempDir()
	probeStarted := filepath.Join(dir, "probe.started")
	script := "#!/bin/sh\nprintf ready > \"$VB_STARTUP_PROBE\"\nexec /bin/sleep 10\n"
	for name, data := range map[string]string{"gst-inspect-1.0": script, "gst-launch-1.0": "#!/bin/sh\nexit 0\n"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(data), 0700); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("VB_STARTUP_PROBE", probeStarted)
	catalog := routingControlCatalog(t)
	catalog.Runtime.RuntimeDirectory, catalog.Runtime.StateDirectory = dir, dir
	catalog.Runtime.EngineDirectory = dir
	catalog.Runtime.Status.Socket = filepath.Join(dir, "status.sock")
	live := catalog.Streams["live"]
	live.Socket = filepath.Join(dir, "live.sock")
	catalog.Streams["live"] = live
	plan, err := routing.DefaultPlan(catalog)
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := routing.Materialize(catalog, plan)
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "config.json")
	if err = os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	owner := &discoveryFrameOwner{make(chan struct{}), make(chan struct{}), make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- runDaemon(ctx, append(launchArgs(), "--config", path), io.Discard,
			func(context.Context, frameLaunchInput) (frameBootstrapOwner, error) { return owner, nil })
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Error(err)
			}
		case <-time.After(5 * time.Second):
			t.Error("startup cancellation failed to join daemon")
		}
		select {
		case <-owner.closed:
		default:
			t.Error("startup did not close its joined bootstrap owner")
		}
	})
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, err := os.Stat(probeStarted); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("hardware discovery fixture never started")
		}
		time.Sleep(5 * time.Millisecond)
	}
	select {
	case <-owner.running:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("frame control is blocked behind hardware discovery")
	}
}
