//go:build !linux || !cgo

package main

import "context"

func openProductFrameBootstrap(context.Context, frameLaunchInput) (frameBootstrapOwner, error) {
	return nil, errFrameProviderUnavailable
}
