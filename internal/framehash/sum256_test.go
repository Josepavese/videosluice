package framehash

import (
	"bytes"
	"crypto/fips140"
	"crypto/sha256"
	"fmt"
	"sync"
	"testing"
)

func TestSum256MatchesStandardLibrary(t *testing.T) {
	for _, size := range []int{0, 1, 55, 56, 63, 64, 65, 127, 128, 129, 4095, 4096, 4097, 345600, 3110400, 16 << 20} {
		for _, offset := range []int{0, 1, 7} {
			t.Run(fmt.Sprintf("size=%d/offset=%d", size, offset), func(t *testing.T) {
				storage := make([]byte, size+offset+1)
				for i := range storage {
					storage[i] = byte(i*131 + i/7)
				}
				original := bytes.Clone(storage)
				payload := storage[offset : offset+size]
				if got, want := Sum256(payload), sha256.Sum256(payload); got != want {
					t.Fatalf("digest mismatch: got %x, want %x", got, want)
				}
				if !bytes.Equal(storage, original) {
					t.Fatal("hash changed borrowed payload or surrounding bytes")
				}
			})
		}
	}
	if Sum256(nil) != sha256.Sum256(nil) {
		t.Fatal("nil payload digest")
	}
	if fips140.Enabled() && Backend() != "go" {
		t.Fatal("native backend bypassed the explicit Go FIPS module")
	}
}

func TestConcurrentPayloadIntegrity(t *testing.T) {
	var group sync.WaitGroup
	for worker := range 16 {
		group.Add(1)
		go func(seed int) {
			defer group.Done()
			payload := bytes.Repeat([]byte{byte(seed)}, 345600)
			want := sha256.Sum256(payload)
			for range 4 {
				if got := Sum256(payload); got != want {
					t.Errorf("concurrent digest mismatch for worker %d", seed)
				}
			}
			payload[len(payload)/2] ^= 1
			if got := Sum256(payload); got == want || got != sha256.Sum256(payload) {
				t.Errorf("payload mutation was not verified for worker %d", seed)
			}
		}(worker)
	}
	group.Wait()
}

var digestResult [sha256.Size]byte

func TestMaximumWorkspaceCoversEveryAdmittedPayloadSize(t *testing.T) {
	for _, maximum := range []int{1, 4096, 65535, 65536, 3110400, 32 << 20, 64 << 20} {
		quote := MaxWorkspaceBytes(maximum)
		if quote < 0 || quote > NativeWorkspaceLimit() {
			t.Fatalf("workspace quote=%d", quote)
		}
		for _, size := range []int{0, 1, 4096, 65536, 3110400, 32 << 20, maximum} {
			if size <= maximum && WorkspaceBytes(size) > quote {
				t.Fatalf("size=%d exceeds quote=%d for ceiling=%d", size, quote, maximum)
			}
		}
	}
}

func BenchmarkPayloadSHA256(b *testing.B) {
	for _, size := range []int{4096, 345600, 3110400} {
		payload := make([]byte, size)
		for i := range payload {
			payload[i] = byte(i*131 + i/7)
		}
		for _, implementation := range []struct {
			name string
			sum  func([]byte) [sha256.Size]byte
		}{{"standard", sha256.Sum256}, {"selected", Sum256}} {
			b.Run(fmt.Sprintf("%d/%s", size, implementation.name), func(b *testing.B) {
				b.SetBytes(int64(size))
				b.ReportAllocs()
				for b.Loop() {
					digestResult = implementation.sum(payload)
				}
			})
		}
	}
}
