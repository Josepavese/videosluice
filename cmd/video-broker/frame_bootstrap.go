package main

import (
	"context"
	"encoding/hex"
	"errors"
	"flag"
	videobroker "github.com/Josepavese/videosluice"
	"github.com/Josepavese/videosluice/internal/source"
	"strconv"
)

var errFrameProviderUnavailable = errors.New("frame bootstrap provider admission unavailable")

// Public launch metadata and inherited positions are a bootstrap contract,
// not authentication. Only the selected native provider can verify handles.
type frameLaunchInput struct {
	SocketFD, ControllerFD int
	ControllerMetadata     []byte
}

type frameBootstrapOwner interface {
	Run(context.Context) error
	Close() error
}
type frameBootstrapOpener func(context.Context, frameLaunchInput) (frameBootstrapOwner, error)

func frameCaptureOptions(owner frameBootstrapOwner) ([]videobroker.StartOptions, error) {
	if owner == nil {
		return nil, nil
	}
	provider, ok := owner.(interface {
		FrameCaptures() source.FrameCaptureFactory
	})
	if !ok {
		return nil, errFrameProviderUnavailable
	}
	factory := provider.FrameCaptures()
	if factory == nil {
		return nil, errFrameProviderUnavailable
	}
	return []videobroker.StartOptions{{FrameCapture: factory}}, nil
}

func resolveFrameBootstrap(ctx context.Context, input frameLaunchInput) (frameBootstrapOwner, error) {
	return openProductFrameBootstrap(ctx, input)
}

type bootstrapArgument struct {
	value string
	seen  bool
}

func (a *bootstrapArgument) String() string { return a.value }
func (a *bootstrapArgument) Set(value string) error {
	if a.seen {
		return errors.New("duplicate bootstrap argument")
	}
	a.seen, a.value = true, value
	return nil
}

type frameBootstrapFlags struct{ socket, controller, metadata bootstrapArgument }

func registerFrameBootstrapFlags(flags *flag.FlagSet) *frameBootstrapFlags {
	f := &frameBootstrapFlags{}
	flags.Var(&f.socket, "frame-launch-socket-fd", "inherited private frame launch socket")
	flags.Var(&f.controller, "frame-launch-controller-fd", "inherited launcher process capability")
	flags.Var(&f.metadata, "frame-launch-controller-metadata", "public launcher capability metadata")
	return f
}

func (f *frameBootstrapFlags) validate() (frameLaunchInput, bool, error) {
	requested := f.socket.seen || f.controller.seen || f.metadata.seen
	if !requested {
		return frameLaunchInput{}, false, nil
	}
	invalid := errors.New("invalid frame bootstrap argument group")
	if !f.socket.seen || !f.controller.seen || !f.metadata.seen {
		return frameLaunchInput{}, true, invalid
	}
	parseFD := func(raw string) (int, bool) {
		if len(raw) < 1 || len(raw) > 6 || raw[0] == '0' {
			return 0, false
		}
		for _, c := range raw {
			if c < '0' || c > '9' {
				return 0, false
			}
		}
		fd, err := strconv.Atoi(raw)
		return fd, err == nil && fd >= 3
	}
	socket, ok := parseFD(f.socket.value)
	controller, otherOK := parseFD(f.controller.value)
	if !ok || !otherOK || socket == controller {
		return frameLaunchInput{}, true, invalid
	}
	raw := f.metadata.value
	if len(raw) < 2 || len(raw) > 512 || len(raw)%2 != 0 {
		return frameLaunchInput{}, true, invalid
	}
	for _, c := range raw {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return frameLaunchInput{}, true, invalid
		}
	}
	metadata, err := hex.DecodeString(raw)
	if err != nil {
		return frameLaunchInput{}, true, invalid
	}
	return frameLaunchInput{socket, controller, metadata}, true, nil
}
