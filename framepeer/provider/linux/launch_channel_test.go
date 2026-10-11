//go:build linux

package linux

import (
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
)

type countedLaunchConnection struct {
	net.Conn
	closes atomic.Int32
	cause  error
}

func (c *countedLaunchConnection) Close() error {
	c.closes.Add(1)
	return errors.Join(c.Conn.Close(), c.cause)
}

func TestAcceptedLaunchChannelHasOnePhysicalCloseOwner(t *testing.T) {
	for _, failed := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "failure"}[failed], func(t *testing.T) {
			left, right := net.Pipe()
			defer right.Close()
			conn := &countedLaunchConnection{Conn: left}
			if failed {
				conn.cause = errors.New("fixture channel close failed")
			}
			launch := &AcceptedLaunch{connection: conn}
			var group sync.WaitGroup
			for i := 0; i < 20; i++ {
				group.Add(1)
				go func() {
					defer group.Done()
					var err error
					if i%2 == 0 {
						err = launch.CloseControl()
					} else {
						err = launch.Close()
					}
					if failed && !errors.Is(err, conn.cause) || !failed && err != nil {
						t.Error(err)
					}
				}()
			}
			group.Wait()
			if conn.closes.Load() != 1 {
				t.Fatal("channel closed twice", conn.closes.Load())
			}
		})
	}
}
