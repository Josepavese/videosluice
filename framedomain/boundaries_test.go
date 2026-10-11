package framedomain_test

import (
	"go/parser"
	"go/token"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestDomainPlatformBoundary(t *testing.T) {
	paths, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	checked := 0
	for _, path := range paths {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		checked++
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatal(err)
		}
		for _, item := range file.Imports {
			name, err := strconv.Unquote(item.Path.Value)
			if err != nil {
				t.Fatal(err)
			}
			for _, prefix := range []string{"os", "syscall", "net", "runtime", "golang.org/x/sys", "github.com/Josepavese/videosluice/framedomain/entropy"} {
				if name == prefix || strings.HasPrefix(name, prefix+"/") {
					t.Fatalf("platform import in domain %s: %s", path, name)
				}
			}
			if strings.Contains(name, "/provider/") {
				t.Fatalf("provider/product coupling in domain %s: %s", path, name)
			}
		}
	}
	if checked == 0 {
		t.Fatal("no domain source inspected")
	}
}
