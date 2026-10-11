//go:build linux

package broker

import (
	"fmt"
	"io"
	"os"
	"syscall"
)

func tuneBrokerPipeCapacity(reader io.ReadCloser, targetBytes int) (int, error) {
	file, ok := reader.(*os.File)
	if !ok || file == nil {
		return 0, fmt.Errorf("broker transport is not an OS pipe")
	}
	if targetBytes <= 0 {
		return 0, fmt.Errorf("invalid broker pipe capacity %d", targetBytes)
	}
	actual, _, errno := syscall.Syscall(
		syscall.SYS_FCNTL,
		file.Fd(),
		uintptr(syscall.F_SETPIPE_SZ),
		uintptr(targetBytes),
	)
	if errno != 0 {
		return brokerPipeCapacity(file), errno
	}
	return int(actual), nil
}

func brokerPipeCapacity(file *os.File) int {
	if file == nil {
		return 0
	}
	actual, _, errno := syscall.Syscall(
		syscall.SYS_FCNTL,
		file.Fd(),
		uintptr(syscall.F_GETPIPE_SZ),
		0,
	)
	if errno != 0 {
		return 0
	}
	return int(actual)
}
