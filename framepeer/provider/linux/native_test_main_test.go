//go:build linux

package linux

import (
	"fmt"
	"os"
	"sync"
	"testing"
)

// Both native test families execute the same immutable C fixture. Compile it
// once per package invocation, not again after the board has warmed up. Each
// case still starts an independent sanitized process with its own deadlines.
var nativeLaunchFixture struct {
	sync.Mutex
	directory string
	binary    string
}

func TestMain(m *testing.M) {
	code := m.Run()
	if nativeLaunchFixture.directory != "" {
		if err := os.RemoveAll(nativeLaunchFixture.directory); err != nil {
			fmt.Fprintln(os.Stderr, "native fixture cleanup:", err)
			code = 1
		}
	}
	os.Exit(code)
}
