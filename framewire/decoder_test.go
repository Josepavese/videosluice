package envelope

import (
	"bytes"
	"errors"
	"go/ast"
	"go/parser"
	gotoken "go/token"
	"io"
	"strconv"
	"testing"
)

type timeoutError struct{}

func (timeoutError) Error() string { return "test timeout" }
func (timeoutError) Timeout() bool { return true }

type interruptedReader struct {
	data       []byte
	at, cut    int
	fired      bool
	endWithEOF bool
	calls      int
}

func TestCodecPlatformBoundary(t *testing.T) {
	allowed := map[string]bool{"crypto/sha256": true, "encoding/binary": true, "errors": true, "io": true, "bytes": true}
	for _, name := range []string{"envelope.go", "decoder.go", "snapshot.go", "chunk.go", "surface_assembly.go"} {
		file, err := parser.ParseFile(gotoken.NewFileSet(), name, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range file.Imports {
			name, err := strconv.Unquote(entry.Path.Value)
			if err != nil || !allowed[name] {
				t.Fatal("platform dependency escaped codec", name, err)
			}
		}
	}
}

func FuzzDecoder(f *testing.F) {
	packet, _ := Marshal(fixture(), make([]byte, 24))
	f.Add(packet)
	f.Add(append(bytes.Clone(packet), packet...))
	f.Add([]byte("invalid"))
	f.Fuzz(func(t *testing.T, b []byte) {
		reader := bytes.NewReader(b)
		d, _ := NewDecoder(make([]byte, HeaderSize+1024))
		for i := 0; i < 3; i++ {
			before := reader.Len()
			h, p, err := d.Read(reader)
			if err != nil {
				after := reader.Len()
				if _, _, next := d.Read(reader); next == nil || reader.Len() != after {
					t.Fatal("fatal decoder resumed")
				}
				return
			}
			encoded, err := Marshal(h, p)
			if err != nil || len(encoded) != before-reader.Len() {
				t.Fatal("invalid decoded packet")
			}
			offset := len(b) - before
			if !bytes.Equal(encoded, b[offset:offset+len(encoded)]) {
				t.Fatal("bytes changed")
			}
		}
	})
}

func (r *interruptedReader) Read(p []byte) (int, error) {
	r.calls++
	if !r.fired && r.at == r.cut {
		r.fired = true
		return 0, timeoutError{}
	}
	end := len(r.data)
	if !r.fired && r.cut < end {
		end = r.cut
	}
	n := copy(p, r.data[r.at:end])
	r.at += n
	if !r.fired && r.at == r.cut {
		r.fired = true
		return n, timeoutError{}
	}
	if r.at == len(r.data) && (n == 0 || r.endWithEOF) {
		return n, io.EOF
	}
	return n, nil
}

func TestDecoderTimeoutAtEveryBoundary(t *testing.T) {
	for _, packet := range vectors(t) {
		for cut := 0; cut <= len(packet); cut++ {
			reader := &interruptedReader{data: append(bytes.Clone(packet), packet...), cut: cut}
			d, _ := NewDecoder(make([]byte, HeaderSize+1024))
			for frame := 0; frame < 2; frame++ {
				var h Header
				var p []byte
				var e error
				for attempts := 0; attempts < 3; attempts++ {
					h, p, e = d.Read(reader)
					if e == nil {
						break
					}
					if !transient(e) {
						t.Fatalf("cut%d frame%d: %v", cut, frame, e)
					}
				}
				if e != nil {
					t.Fatal("did not resume", cut, e)
				}
				b, e := Marshal(h, p)
				if e != nil || !bytes.Equal(b, packet) {
					t.Fatal("framing lost", cut, frame)
				}
			}
			if _, _, e := d.Read(reader); e != io.EOF {
				t.Fatal("EOF", e)
			}
			calls := reader.calls
			if _, _, e := d.Read(reader); e != io.EOF || reader.calls != calls {
				t.Fatal("read after terminal")
			}
		}
	}
}
func TestDecoderCompleteFrameWithEOF(t *testing.T) {
	for _, p := range vectors(t) {
		r := &interruptedReader{data: p, cut: -1, fired: true, endWithEOF: true}
		d, _ := NewDecoder(make([]byte, HeaderSize+1024))
		h, b, e := d.Read(r)
		if e != nil {
			t.Fatal(e)
		}
		wire, _ := Marshal(h, b)
		if !bytes.Equal(wire, p) {
			t.Fatal("bytes+EOF lost frame")
		}
		if _, _, e = d.Read(r); e != io.EOF {
			t.Fatal("terminal EOF missing", e)
		}
	}
}
func TestDecoderFatalPartialAndCorruptionPoison(t *testing.T) {
	base := vectors(t)[0]
	for _, p := range [][]byte{base[:1], base[:HeaderSize], base[:len(base)-1], append([]byte("NMB2"), base[4:]...), append(bytes.Clone(base[:len(base)-1]), 255)} {
		d, _ := NewDecoder(make([]byte, HeaderSize+1024))
		_, _, e := d.Read(bytes.NewReader(p))
		if e == nil {
			t.Fatal("accepted malformed")
		}
		_, _, next := d.Read(bytes.NewReader(base))
		if next == nil || !errors.Is(next, e) {
			t.Fatal("resynced after failure", e, next)
		}
		if d.payload != nil {
			t.Fatal("retained failed payload")
		}
	}
}
func TestDecoderQuotaBeforePayloadAndAbort(t *testing.T) {
	packet := vectors(t)[2]
	r := bytes.NewReader(packet)
	d, _ := NewDecoder(make([]byte, HeaderSize+5))
	if _, _, e := d.Read(r); e != ErrInvalid {
		t.Fatal(e)
	}
	if r.Len() != len(packet)-HeaderSize || d.payload != nil {
		t.Fatal("quota checked after payload allocation/read")
	}
	for _, size := range []int{0, HeaderSize - 1, HeaderSize, HeaderSize + MaxPayload + 1} {
		if _, e := NewDecoder(make([]byte, size)); e == nil {
			t.Fatal("bad quota")
		}
	}
	d, _ = NewDecoder(make([]byte, HeaderSize+1024))
	r2 := &interruptedReader{data: vectors(t)[0], cut: HeaderSize + 1}
	if _, _, e := d.Read(r2); !transient(e) {
		t.Fatal(e)
	}
	sentinel := errors.New("canceled")
	d.Abort(sentinel)
	if d.payload != nil {
		t.Fatal("abort retained payload")
	}
	if _, _, e := d.Read(r2); e != sentinel {
		t.Fatal(e)
	}
}

type badReader struct {
	n     int
	calls int
}

func (r *badReader) Read(p []byte) (int, error) {
	r.calls++
	if r.n == 1 {
		return len(p) + 1, nil
	}
	return r.n, nil
}
func TestDecoderInvalidReaderBounded(t *testing.T) {
	for _, n := range []int{-1, 0, 1} {
		r := &badReader{n: n}
		d, _ := NewDecoder(make([]byte, HeaderSize+1024))
		if _, _, e := d.Read(r); e == nil {
			t.Fatal("bad reader accepted")
		}
		if r.calls != 1 {
			t.Fatal("busy loop")
		}
		d.Read(r)
		if r.calls != 1 {
			t.Fatal("terminal not retained")
		}
	}
}

func TestDecoderBorrowedPacketWorkspace(t *testing.T) {
	first := vectors(t)[0]
	h, payload, err := Parse(first)
	if err != nil {
		t.Fatal(err)
	}
	h.OriginSequence++
	changed := bytes.Repeat([]byte{71}, len(payload))
	second, err := Marshal(h, changed)
	if err != nil {
		t.Fatal(err)
	}
	// The backing array is deliberately larger than the admitted slice. Its
	// tail must never be read into or exposed in the returned packet capacity.
	backing := bytes.Repeat([]byte{0xee}, len(first)+97)
	workspace := backing[:len(first)]
	d, err := NewDecoder(workspace)
	if err != nil {
		t.Fatal(err)
	}
	reader := bytes.NewReader(append(bytes.Clone(first), second...))
	packet, err := d.ReadPacket(reader)
	if err != nil || !bytes.Equal(packet, first) || &packet[0] != &workspace[0] || cap(packet) != len(first) {
		t.Fatal("frame did not borrow the exact admitted packet", err)
	}
	saved := bytes.Clone(packet) // Explicit caller-owned retention, not decoder memory.
	packet, err = d.ReadPacket(reader)
	if err != nil || !bytes.Equal(packet, second) || &packet[0] != &workspace[0] {
		t.Fatal("second frame allocated or changed", err)
	}
	if !bytes.Equal(saved, first) || !bytes.Equal(backing[len(first):], bytes.Repeat([]byte{0xee}, 97)) {
		t.Fatal("unadmitted storage overwritten")
	}
	d.Abort(io.EOF)
	if d.storage != nil || d.payload != nil {
		t.Fatal("aborted decoder retained borrowed workspace")
	}
	if packet, err := d.ReadPacket(reader); err != io.EOF || packet != nil {
		t.Fatal("aborted decoder exposed stale output", err)
	}
}

func TestDecoderNoPayloadAllocationPath(t *testing.T) {
	// Keep the invariant structural as well as pointer-tested above: adding a
	// make/append payload path would evade a test using only small frames.
	file, err := parser.ParseFile(gotoken.NewFileSet(), "decoder.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		id, ok := call.Fun.(*ast.Ident)
		if ok && (id.Name == "make" || id.Name == "append") {
			t.Error("decoder allocates packet storage outside caller admission", id.Name)
		}
		return true
	})
}
