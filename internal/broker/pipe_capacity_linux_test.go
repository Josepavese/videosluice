//go:build linux

package broker

import (
	"bytes"
	"io"
	"os"
	"testing"
)

func TestTuneBrokerPipeCapacity(t *testing.T) {
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Close() }()
	defer func() { _ = writer.Close() }()
	current := brokerPipeCapacity(reader)
	if current <= 0 {
		t.Skip("kernel does not expose pipe capacity")
	}
	actual, err := tuneBrokerPipeCapacity(reader, current)
	if err != nil {
		t.Fatalf("retain current capacity: %v", err)
	}
	if actual != current {
		t.Fatalf("capacity=%d want=%d", actual, current)
	}
	if _, err := tuneBrokerPipeCapacity(io.NopCloser(bytes.NewReader(nil)), current); err == nil {
		t.Fatal("non-pipe reader was accepted")
	}
	if _, err := tuneBrokerPipeCapacity(reader, 0); err == nil {
		t.Fatal("zero capacity was accepted")
	}
}
