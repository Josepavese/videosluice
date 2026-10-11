package envelope

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"testing"
)

func sourceVector(t *testing.T, hd bool) []byte {
	h := fixture()
	h.Kind, h.Surface, h.Stream, h.PublicationEpoch, h.DeliveryID = Surface, ProcessedFull, "", 0, 0
	if hd {
		h.Width, h.Height, h.StrideY, h.StrideUV = 1280, 720, 1280, 1280
	}
	p := make([]byte, int(h.StrideY)*int(h.Height)*3/2)
	for i := range p {
		p[i] = byte(i * 7)
	}
	b, e := Marshal(h, p)
	if e != nil {
		t.Fatal(e)
	}
	return b
}
func dataRecords(t *testing.T, p []byte, size int) [][]byte {
	t.Helper()
	id := sha256.Sum256(p[:HeaderSize])
	var result [][]byte
	for off := 0; off < len(p)-HeaderSize; {
		n := min(size, len(p)-HeaderSize-off)
		storage := make([]byte, MaxDataPacketBytes)
		written, e := EncodeDataChunk(storage, id, off, p[HeaderSize+off:HeaderSize+off+n])
		if e != nil {
			t.Fatal(e)
		}
		result = append(result, storage[:written:written])
		off += n
	}
	return result
}
func TestDataChunkContract(t *testing.T) {
	id := [32]byte{9}
	storage := make([]byte, MaxDataPacketBytes)
	n, e := EncodeDataChunk(storage, id, MaxDataExtent-3, []byte{1, 2, 3})
	if e != nil {
		t.Fatal(e)
	}
	p := storage[:n]
	d, e := ParseDataChunk(p)
	if e != nil || d.Manifest != id || d.Offset != MaxDataExtent-3 || !bytes.Equal(d.Payload, []byte{1, 2, 3}) {
		t.Fatal("roundtrip", e)
	}
	if &d.Payload[0] != &p[DataHeaderSize] || cap(d.Payload) != len(d.Payload) || &p[0] != &storage[0] {
		t.Fatal("data workspace not borrowed/capped")
	}
	for _, offset := range []int{-1, MaxDataExtent, MaxDataExtent + 1} {
		if b, e := EncodeDataChunk(storage, id, offset, []byte{1}); e == nil || b != 0 {
			t.Fatal("invalid extent accepted")
		}
	}
	if b, e := EncodeDataChunk(storage, [32]byte{}, 0, []byte{1}); e == nil || b != 0 {
		t.Fatal("zero manifest")
	}
	if b, e := EncodeDataChunk(storage[:DataHeaderSize], id, 0, []byte{1}); !errors.Is(e, ErrBuffer) || b != 0 {
		t.Fatal("capacity", e)
	}
	for _, size := range []int{0, MaxDataPacketBytes - DataHeaderSize + 1} {
		if b, e := EncodeDataChunk(storage, id, 0, make([]byte, size)); e == nil || b != 0 {
			t.Fatal("invalid payload")
		}
	}
	for _, mutate := range []func([]byte){func(p []byte) { p[0] = 'X' }, func(p []byte) { p[4] = 2 }, func(p []byte) { p[5] = 1 }, func(p []byte) { p[7]++ }, func(p []byte) { clear(p[8:40]) }, func(p []byte) { binary.BigEndian.PutUint32(p[40:44], MaxDataExtent) }} {
		b := bytes.Clone(p)
		mutate(b)
		if _, e := ParseDataChunk(b); e == nil {
			t.Fatal("invalid data packet accepted")
		}
	}
	for _, n := range []int{0, 4, DataHeaderSize, len(p) - 1} {
		if _, e := ParseDataChunk(p[:n]); e == nil {
			t.Fatal("truncated packet")
		}
	}
}
func TestSourceAssemblyRequiresCompletePayloadSHA256(t *testing.T) {
	p := sourceVector(t, true)
	if a, err := NewSourceAssembly(p[:HeaderSize], make([]byte, len(p)), nil); a != nil || !errors.Is(err, ErrInvalid) {
		t.Fatal("missing integrity provider was accepted", err)
	}
	for _, corrupt := range []bool{false, true} {
		storage := make([]byte, len(p))
		calls := 0
		a, err := NewSourceAssembly(p[:HeaderSize], storage, func(payload []byte) [sha256.Size]byte {
			calls++
			if len(payload) != len(p)-HeaderSize {
				t.Fatal("digest did not cover the whole admitted payload")
			}
			return sha256.Sum256(payload)
		})
		if err != nil {
			t.Fatal(err)
		}
		for _, record := range dataRecords(t, p, MaxDataPacketBytes-DataHeaderSize) {
			if err := a.Append(record); err != nil {
				t.Fatal(err)
			}
			if calls != 0 {
				t.Fatal("hashed partial payload during chunk assembly")
			}
		}
		if corrupt {
			storage[len(storage)-1] ^= 1
		}
		_, out, err := a.Finish()
		if calls != 1 {
			t.Fatal("complete payload must be verified exactly once", calls)
		}
		if corrupt && (!errors.Is(err, ErrInvalid) || out != nil) {
			t.Fatal("corrupt payload escaped verification", err)
		}
		if !corrupt && (err != nil || !bytes.Equal(out, p)) {
			t.Fatal("valid payload changed", err)
		}
	}
}

func TestSurfaceAssemblyHDUsesExactAdmittedWorkspace(t *testing.T) {
	for _, hd := range []bool{false, true} {
		p := sourceVector(t, hd)
		storage := make([]byte, len(p), len(p)+1)
		a, e := NewSourceAssembly(p[:HeaderSize], storage, sha256.Sum256)
		if e != nil {
			t.Fatal(e)
		}
		records := dataRecords(t, p, MaxDataPacketBytes-DataHeaderSize)
		if hd && len(records) != 85 {
			t.Fatal("HD must span85 bounded chunks", len(records))
		}
		for _, r := range records {
			if e := a.Append(r); e != nil {
				t.Fatal(e)
			}
		}
		h, out, e := a.Finish()
		if e != nil || !bytes.Equal(out, p) || h.OriginSequence != fixture().OriginSequence {
			t.Fatal("surface reconstruction", e)
		}
		if &out[0] != &storage[0] || cap(out) != len(out) {
			t.Fatal("surface arena copied/escaped")
		}
		if _, b, e := a.Finish(); e == nil || b != nil || a.Complete() {
			t.Fatal("finished assembly reusable")
		}
		if e := a.Append(records[0]); e == nil {
			t.Fatal("append after Finish")
		}
		for _, n := range []int{len(p) - 1, len(p) + 1} {
			if a, e := NewSourceAssembly(p[:HeaderSize], make([]byte, n), sha256.Sum256); !errors.Is(e, ErrBuffer) || a != nil {
				t.Fatal("nonexact arena accepted")
			}
		}
	}
}
func TestSurfaceAssemblyFaultsAreTerminal(t *testing.T) {
	for _, name := range []string{"missing", "reorder", "duplicate", "manifest", "tamper", "truncated", "overrun", "foreign_header", "extra"} {
		t.Run(name, func(t *testing.T) {
			p := sourceVector(t, false)
			rs := dataRecords(t, p, 8)
			switch name {
			case "missing":
				rs = rs[:len(rs)-1]
			case "reorder":
				rs[0], rs[1] = rs[1], rs[0]
			case "duplicate":
				rs[1] = rs[0]
			case "manifest":
				rs[0][8] ^= 1
			case "tamper":
				rs[len(rs)-1][DataHeaderSize] ^= 1
			case "truncated":
				rs[0] = rs[0][:len(rs[0])-1]
			case "overrun":
				storage := make([]byte, MaxDataPacketBytes)
				n, e := EncodeDataChunk(storage, sha256.Sum256(p[:HeaderSize]), 16, make([]byte, 9))
				if e != nil {
					t.Fatal(e)
				}
				rs[2] = storage[:n:n]
			case "foreign_header":
				rs[0] = p[:HeaderSize]
			case "extra":
				rs = append(rs, rs[0])
			}
			a, e := NewSourceAssembly(p[:HeaderSize], make([]byte, len(p)), sha256.Sum256)
			if e != nil {
				t.Fatal(e)
			}
			var failed bool
			for _, r := range rs {
				if e := a.Append(r); e != nil {
					failed = true
					break
				}
			}
			if _, b, e := a.Finish(); e == nil || b != nil {
				t.Fatal("partial/untrusted surface escaped", failed)
			}
			if a.Complete() || a.Append(rs[0]) == nil {
				t.Fatal("failed assembly can resume")
			}
		})
	}
}

func FuzzDataChunk(f *testing.F) {
	storage := make([]byte, MaxDataPacketBytes)
	n, _ := EncodeDataChunk(storage, [32]byte{1}, 0, []byte{1, 2, 3})
	f.Add(storage[:n])
	f.Add([]byte("invalid"))
	f.Fuzz(func(t *testing.T, p []byte) {
		d, err := ParseDataChunk(p)
		if err != nil {
			return
		}
		out := make([]byte, MaxDataPacketBytes)
		n, err := EncodeDataChunk(out, d.Manifest, int(d.Offset), d.Payload)
		if err != nil || !bytes.Equal(p, out[:n]) {
			t.Fatal("noncanonical data framing", err)
		}
		if cap(d.Payload) != len(d.Payload) {
			t.Fatal("borrow escaped packet")
		}
	})
}
