//go:build !linux

package broker

import "io"

func tuneBrokerPipeCapacity(_ io.ReadCloser, _ int) (int, error) {
	return 0, nil
}
