//go:build linux && cgo

package linux

import "os"

func pageCharge(n uint64) uint64 {
	page := uint64(os.Getpagesize())
	return (n + page - 1) / page * page
}
