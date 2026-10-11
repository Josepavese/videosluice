//go:build !linux

package broker

import "os/exec"

func brokerBootstrapObservation(_ *exec.Cmd) string {
	return "startup_observation=unsupported"
}
