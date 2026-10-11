package surfaceport

import (
	"go/parser"
	"go/token"
	"strconv"
	"testing"
)

func TestContractPlatformBoundary(t *testing.T) {
	f, err := parser.ParseFile(token.NewFileSet(), "port.go", nil, parser.ImportsOnly)
	if err != nil {
		t.Fatal(err)
	}
	allowed := map[string]bool{"context": true, "errors": true, "github.com/Josepavese/videosluice/framewire": true}
	for _, entry := range f.Imports {
		name, err := strconv.Unquote(entry.Path.Value)
		if err != nil || !allowed[name] {
			t.Fatal("provider dependency escaped contract", name, err)
		}
	}
}
