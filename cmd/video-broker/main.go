package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	videobroker "github.com/Josepavese/videosluice"
	"github.com/Josepavese/videosluice/capability"
	"github.com/Josepavese/videosluice/config"
	in "github.com/Josepavese/videosluice/frameingress"
	"github.com/Josepavese/videosluice/internal/diagnosticlog"
	"github.com/Josepavese/videosluice/internal/unixsocket"
	"github.com/Josepavese/videosluice/routing"
)

func main() {
	// Log output is not a media consumer. In particular metrics emitted while
	// holding a runtime lock must not stop capture/fan-out on a slow supervisor
	// or journal sink. One bounded writer serves every standard logger call.
	diagnostics, err := diagnosticlog.New(log.Writer(), 256)
	if err != nil {
		panic(err)
	} // fixed, internal composition contract
	log.SetOutput(diagnostics)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	err = runDaemon(ctx, os.Args[1:], os.Stdout, resolveFrameBootstrap)
	stop()
	if err != nil && !errors.Is(err, flag.ErrHelp) {
		log.Print(err)
	}
	flush, cancel := context.WithTimeout(context.Background(), time.Second)
	_ = diagnostics.Close(flush) // bounded best effort, never a hardware gate
	cancel()
	if err != nil && !errors.Is(err, flag.ErrHelp) {
		os.Exit(1)
	}
}

// runDaemon owns startup and cleanup. Provider composition is injected here,
// never selected by a frame request or an operator-provided resource quote.
func runDaemon(parent context.Context, args []string, output io.Writer, openFrame frameBootstrapOpener) (result error) {
	if parent == nil || output == nil {
		return errors.New("invalid daemon startup context")
	}
	flags := flag.NewFlagSet("video-broker", flag.ContinueOnError)
	flags.SetOutput(output)
	configPath := flags.String("config", "", "absolute path to the video-broker JSON configuration")
	routingCatalogPath := flags.String("routing-catalog", "", "absolute path to the central runtime routing catalog")
	routingPlanPath := flags.String("routing-plan", "", "absolute path to the persisted runtime routing plan")
	routingControlSocket := flags.String("routing-control-socket", "", "absolute path to the standalone runtime routing control socket")
	checkConfig := flags.Bool("check-config", false, "validate the configuration and exit")
	discoverCapabilities := flags.Bool("discover-capabilities", false, "print standalone local component discovery as JSON and exit")
	engineDirectory := flags.String("engine-directory", "", "product worker/plugin directory for capability discovery")
	probeTimeout := flags.Duration("probe-timeout", 8*time.Second, "bounded capability discovery timeout")
	frameOptions := registerFrameBootstrapFlags(flags)
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("unexpected positional daemon arguments")
	}
	launch, requested, err := frameOptions.validate()
	if err != nil {
		return err
	}
	if requested && (*checkConfig || *discoverCapabilities) {
		return errors.New("frame bootstrap requires daemon mode")
	}
	if err := parent.Err(); err != nil {
		return err
	}
	ctx, stop := context.WithCancel(parent)
	defer stop()
	var bootstrap frameBootstrapOwner
	if requested {
		if openFrame == nil {
			return errFrameProviderUnavailable
		}
		var err error
		bootstrap, err = openFrame(ctx, launch)
		// A partial failure can still transfer cleanup obligations.
		if bootstrap != nil {
			defer func() { stop(); result = errors.Join(result, bootstrap.Close()) }()
		}
		if err != nil {
			return fmt.Errorf("frame bootstrap: %w", err)
		}
		if bootstrap == nil {
			return errors.New("frame bootstrap returned no lifecycle owner")
		}
	}
	if *discoverCapabilities {
		directory := *engineDirectory
		if directory == "" {
			directory = defaultEngineDirectory()
		}
		inventory := capability.Discover(ctx, capability.DiscoverOptions{
			EngineDirectory: directory,
			ProbeTimeout:    *probeTimeout,
		})
		encoder := json.NewEncoder(output)
		encoder.SetIndent("", "  ")
		if err := encoder.Encode(inventory); err != nil {
			return fmt.Errorf("encode capabilities: %w", err)
		}
		return nil
	}
	var input runtimeInput
	if *checkConfig {
		input, err = loadRuntimeInput(*configPath, *routingCatalogPath, *routingPlanPath)
	} else {
		input, err = loadRuntimeInputForStart(*configPath, *routingCatalogPath, *routingPlanPath)
	}
	if err != nil {
		return fmt.Errorf("load configuration: %w", err)
	}
	if input.CatalogReconciled {
		log.Printf(
			"[video-broker] reconciled operator plan to central catalog revision=%s plan_revision=%d",
			input.Plan.CatalogRevision, input.Plan.Revision,
		)
	}
	cfg := input.Config
	controlPath := strings.TrimSpace(*routingControlSocket)
	var routingStore *routing.Store
	if controlPath != "" {
		if input.Catalog == nil || input.Plan == nil {
			return errors.New("routing control requires --routing-catalog and --routing-plan")
		}
		if err := validateRoutingControlPath(
			controlPath, cfg, *routingCatalogPath, *routingPlanPath,
		); err != nil {
			return fmt.Errorf("routing control path: %w", err)
		}
		routingStore, err = routing.NewStoreForPlan(*routingPlanPath)
		if err != nil {
			return fmt.Errorf("routing control store: %w", err)
		}
	}
	if *checkConfig {
		_, err := fmt.Fprintln(output, "video-broker configuration valid")
		return err
	}
	if cfg.Runtime.FrameAccess != nil && bootstrap == nil {
		return errors.New("frame access requires authenticated launcher bootstrap")
	}
	if configured, ok := bootstrap.(interface{ Configure(config.Config) error }); ok {
		if err := configured.Configure(cfg); err != nil {
			return fmt.Errorf("frame product configuration: %w", err)
		}
	}
	var service *videobroker.Service
	captureOptions, err := frameCaptureOptions(bootstrap)
	if err != nil {
		return fmt.Errorf("frame camera provider: %w", err)
	}
	// The authenticated control plane does not depend on a camera already
	// producing frames. Serve it while hardware discovery/startup is pending,
	// so a slow provider cannot strand the launcher's readiness exchange.
	var frameDone chan error
	if bootstrap != nil {
		frameDone = make(chan error, 1)
		done := frameDone
		go func() {
			err := bootstrap.Run(ctx)
			if ctx.Err() == nil && (err == nil || errors.Is(err, context.Canceled)) {
				err = fmt.Errorf("frame control stopped without daemon cancellation: %v", err)
			}
			done <- err
			stop()
		}()
		defer func() {
			// Startup failure still owns this join. After successful startup,
			// waitDaemon owns it instead; never close bootstrap under its loop.
			if frameDone != nil {
				stop()
				if err := <-frameDone; err != nil && !errors.Is(err, context.Canceled) {
					result = errors.Join(result, fmt.Errorf("frame control startup: %w", err))
				}
			}
		}()
	}
	if input.Catalog != nil && input.Plan != nil {
		service, err = videobroker.StartRouted(ctx, *input.Catalog, *input.Plan, captureOptions...)
	} else {
		service, err = videobroker.Start(ctx, cfg, captureOptions...)
	}
	if err != nil {
		return fmt.Errorf("start: %w", err)
	}
	var statusDone chan error
	if cfg.Runtime.Status.Socket != "" {
		statusDone = make(chan error, 1)
		frames, _ := bootstrap.(frameMetadataProvider)
		go func() {
			statusDone <- serveStatus(ctx, cfg.Runtime.Status.Socket, service, frames)
		}()
	}
	var routingControlDone chan error
	if controlPath != "" {
		routingControlDone = make(chan error, 1)
		go func() {
			routingControlDone <- serveRoutingControl(
				ctx, controlPath, *input.Catalog, routingStore, service)
		}()
	}
	log.Printf("[video-broker] daemon started sources=%d streams=%d", len(cfg.Sources), len(cfg.Streams))
	waitDone := make(chan error, 1)
	go func() { waitDone <- service.Wait(ctx) }()
	joinedFrame := frameDone
	frameDone = nil
	return waitDaemon(stop, waitDone, statusDone, routingControlDone, joinedFrame)
}

func waitDaemon(stop context.CancelFunc, waitDone, statusDone, routingControlDone, frameDone <-chan error) (result error) {
	// Every error path cancels and joins the service before bootstrap ownership
	// is retired. No log.Fatal may skip the lifecycle defers in this function.
	defer func() {
		stop()
		for _, pending := range []struct {
			name string
			done <-chan error
		}{
			{"service", waitDone}, {"status", statusDone}, {"routing_control", routingControlDone},
			{"frame_control", frameDone},
		} {
			if pending.done == nil {
				continue
			}
			log.Printf("[video-broker] shutdown stage=join component=%s", pending.name)
			if err := <-pending.done; err != nil && !errors.Is(err, context.Canceled) {
				result = errors.Join(result, fmt.Errorf("%s shutdown: %w", pending.name, err))
			}
		}
	}()
	select {
	case err := <-frameDone:
		frameDone = nil
		if err != nil && !errors.Is(err, context.Canceled) {
			return fmt.Errorf("frame control API: %w", err)
		}
	case err := <-waitDone:
		waitDone = nil
		if err != nil {
			return fmt.Errorf("runtime: %w", err)
		}
	case err := <-statusDone:
		statusDone = nil
		if err != nil && !errors.Is(err, context.Canceled) {
			return fmt.Errorf("status API: %w", err)
		}
	case err := <-routingControlDone:
		routingControlDone = nil
		if err != nil && !errors.Is(err, context.Canceled) {
			return fmt.Errorf("routing control API: %w", err)
		}
	}
	return nil
}

func loadRuntimeConfig(configPath, routingCatalogPath, routingPlanPath string) (config.Config, error) {
	input, err := loadRuntimeInput(configPath, routingCatalogPath, routingPlanPath)
	return input.Config, err
}

type runtimeInput struct {
	Config            config.Config
	Catalog           *routing.Catalog
	Plan              *routing.Plan
	CatalogReconciled bool
}

func loadRuntimeInput(configPath, routingCatalogPath, routingPlanPath string) (runtimeInput, error) {
	return loadRuntimeInputMode(configPath, routingCatalogPath, routingPlanPath, false)
}

func loadRuntimeInputForStart(configPath, routingCatalogPath, routingPlanPath string) (runtimeInput, error) {
	return loadRuntimeInputMode(configPath, routingCatalogPath, routingPlanPath, true)
}

func loadRuntimeInputMode(
	configPath, routingCatalogPath, routingPlanPath string,
	reconcileCatalog bool,
) (runtimeInput, error) {
	configPath = strings.TrimSpace(configPath)
	routingCatalogPath = strings.TrimSpace(routingCatalogPath)
	routingPlanPath = strings.TrimSpace(routingPlanPath)
	if configPath != "" {
		if routingCatalogPath != "" || routingPlanPath != "" {
			return runtimeInput{}, errors.New("--config cannot be combined with runtime routing inputs")
		}
		cfg, err := config.Load(configPath)
		return runtimeInput{Config: cfg}, err
	}
	if routingCatalogPath == "" || routingPlanPath == "" {
		return runtimeInput{}, errors.New("use --config or both --routing-catalog and --routing-plan")
	}
	catalog, err := routing.LoadCatalog(routingCatalogPath)
	if err != nil {
		return runtimeInput{}, err
	}
	store, err := routing.NewStoreForPlan(routingPlanPath)
	if err != nil {
		return runtimeInput{}, err
	}
	var plan routing.Plan
	reconciled := false
	if reconcileCatalog {
		plan, reconciled, err = store.ReadAndReconcileCatalog(catalog)
	} else {
		plan, err = store.Read(catalog)
	}
	if err != nil {
		return runtimeInput{}, fmt.Errorf("load routing plan: %w", err)
	}
	cfg, err := routing.Materialize(catalog, plan)
	if err != nil {
		return runtimeInput{}, err
	}
	return runtimeInput{
		Config: cfg, Catalog: &catalog, Plan: &plan,
		CatalogReconciled: reconciled,
	}, nil
}

func defaultEngineDirectory() string {
	executable, err := os.Executable()
	if err != nil {
		return ""
	}
	return filepath.Join(filepath.Dir(executable), "bin")
}

type frameMetadataProvider interface {
	FrameMetadata(context.Context) map[string]map[string]in.DeliveryMetadata
}

func serveStatus(ctx context.Context, path string, service *videobroker.Service, frames frameMetadataProvider) error {
	path = filepath.Clean(path)
	if !filepath.IsAbs(path) || path == "/" {
		return fmt.Errorf("status socket must be an absolute non-root path")
	}
	// Status is a daemon-lifetime control endpoint, not a replaceable media
	// publication. Keep exactly one owner-only pathname so health consumers can
	// authenticate and fully account for its inode.
	listener, err := unixsocket.ListenSingleOwner(path)
	if err != nil {
		return err
	}
	defer listener.Cleanup()
	go func() {
		<-ctx.Done()
		_ = listener.Close()
	}()
	for {
		connection, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		_ = connection.SetWriteDeadline(time.Now().Add(time.Second))
		status := service.Snapshot(time.Now())
		if frames != nil {
			status.FrameAccess = &in.MetadataStatus{Schema: "video-broker.frame-metadata.v1", Sources: frames.FrameMetadata(ctx)}
		}
		_ = json.NewEncoder(connection).Encode(status)
		_ = connection.Close()
	}
}
