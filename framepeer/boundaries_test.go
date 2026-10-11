package framepeer_test

import (
	"go/parser"
	"go/token"
	"os"
	"strconv"
	"strings"
	"testing"
)

func TestPlatformBoundary(t *testing.T) {
	for _, path := range []string{"port.go", "provider/linux/peer.go", "provider/linux/capability.go", "provider/linux/control.go", "provider/linux/identity_codec.go", "provider/linux/pending.go", "provider/linux/reply.go", "provider/linux/socket_limits.go", "provider/linux/frame_writer.go", "provider/linux/frame_connection.go", "provider/linux/control_connection.go", "provider/linux/launch.go"} {
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatal(err)
		}
		for _, item := range file.Imports {
			name, err := strconv.Unquote(item.Path.Value)
			if err != nil {
				t.Fatal(err)
			}
			if path == "port.go" && name != "context" && name != "errors" {
				t.Fatalf("platform dependency in port: %s", name)
			}
			if path != "port.go" && (strings.Contains(name, "framedomain") || strings.Contains(name, "frameaccess")) {
				t.Fatalf("domain dependency in provider: %s", name)
			}
		}
	}
}

func TestNativePlatformBoundary(t *testing.T) {
	for _, path := range []string{"native/framepeer.h", "provider/linux/native/launch.h", "provider/linux/native/launch.c"} {
		content, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		text := string(content)
		for _, forbidden := range []string{"framedomain", "frameaccess", "rdkx5"} {
			if strings.Contains(strings.ToLower(text), forbidden) {
				t.Fatalf("native peer crosses business/hardware boundary: %s %s", path, forbidden)
			}
		}
		if path == "native/framepeer.h" && strings.Contains(text, "#include") {
			t.Fatal("semantic native contract acquired platform imports")
		}
		for _, line := range strings.Split(text, "\n") {
			line = strings.TrimSpace(line)
			if strings.HasPrefix(line, "#include \"") && line != "#include \"framepeer.h\"" && line != "#include \"launch.h\"" {
				t.Fatalf("unreviewed native provider dependency: %s", line)
			}
		}
	}
}
