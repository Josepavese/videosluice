package envelope

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"os/exec"
	"reflect"
	"testing"
)

func controlFixture(t *testing.T, p []byte) (Control, []byte, Mapping) {
	t.Helper()
	h, _, err := Parse(p)
	if err != nil {
		t.Fatal(err)
	}
	c := Control{Begin: math.MaxUint64 - 1, End: math.MaxUint64 - 1, Capacity: 1024, Used: uint64(len(p)), Generation: math.MaxUint64, Namespace: h.Namespace}
	b, err := MarshalControl(c)
	if err != nil {
		t.Fatal(err)
	}
	return c, b[:], Mapping{c.Namespace, c.Generation, c.Capacity}
}

func TestSnapshotRoundTripAndIndependentParity(t *testing.T) {
	var records [][3]string
	var want []bool
	var controls []string
	add := func(b, p, a []byte, m Mapping) {
		_, _, err := ValidateSnapshot(b, p, a, m)
		records = append(records, [3]string{hex.EncodeToString(b), hex.EncodeToString(p), hex.EncodeToString(a)})
		want = append(want, err == nil)
	}
	for _, p := range vectors(t) {
		c, b, m := controlFixture(t, p)
		parsed, err := ParseControl(b)
		if err != nil || parsed != c {
			t.Fatal("control roundtrip", err)
		}
		controls = append(controls, hex.EncodeToString(b))
		h, body, err := ValidateSnapshot(b, p, b, m)
		if err != nil {
			t.Fatal(err)
		}
		rebuilt, err := Marshal(h, body)
		if err != nil || !bytes.Equal(rebuilt, p) {
			t.Fatal("snapshot packet differs")
		}
		add(b, p, b, m)
		for i := range b {
			x := bytes.Clone(b)
			x[i] ^= 0x80
			add(x, p, x, m)
			add(b, p, x, m)
		}
		for i := 0; i < len(b); i++ {
			add(b[:i], p, b, m)
			add(b, p, b[:i], m)
		}
		for i := range p {
			x := bytes.Clone(p)
			x[i] ^= 0x80
			add(b, x, b, m)
		}
		add(append(bytes.Clone(b), 0), p, b, m)
		add(b, p[:len(p)-1], b, m)
		add(b, append(bytes.Clone(p), 0), b, m)
	}
	input, _ := json.Marshal(records)
	cmd := exec.Command("python3", "-B", "snapshot_crosscheck.py")
	cmd.Stdin = bytes.NewReader(input)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Python: %v %s", err, output)
	}
	var result struct {
		Controls []string `json:"controls"`
		Accepted []bool   `json:"accepted"`
	}
	if err := json.Unmarshal(output, &result); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(controls, result.Controls) || !reflect.DeepEqual(want, result.Accepted) {
		t.Fatal("cross-language snapshot mismatch")
	}
	t.Logf("%d independent snapshot acceptance cases, %d identical control vectors", len(records), len(controls))
}

func TestSnapshotFailureCategoriesAndBounds(t *testing.T) {
	p := vectors(t)[0]
	c, b, m := controlFixture(t, p)
	for _, change := range []func(*Mapping){func(m *Mapping) { m.Namespace = [16]byte{} }, func(m *Mapping) { m.Generation = 0 }, func(m *Mapping) { m.Generation-- }, func(m *Mapping) { m.Capacity = HeaderSize - 1 }, func(m *Mapping) { m.Capacity = HeaderSize + MaxPayload + 1 }, func(m *Mapping) { m.Capacity++ }} {
		bad := m
		change(&bad)
		if _, _, err := ValidateSnapshot(b, p, b, bad); !errors.Is(err, ErrMapping) {
			t.Fatal("expected allocation rejection", err)
		}
	}
	for _, change := range []func(*Control){func(c *Control) { c.Begin = 0 }, func(c *Control) { c.End = 0 }, func(c *Control) { c.Used = HeaderSize - 1 }, func(c *Control) { c.Used = 1025 }, func(c *Control) { c.Generation = 0 }, func(c *Control) { c.Capacity = math.MaxUint64 }} {
		bad := c
		change(&bad)
		if _, err := MarshalControl(bad); err == nil {
			t.Fatal("invalid control marshalled")
		}
	}
	odd := bytes.Clone(b)
	binary.BigEndian.PutUint64(odd[8:16], 3)
	if _, _, err := ValidateSnapshot(odd, p, b, m); !errors.Is(err, ErrBusy) {
		t.Fatal(err)
	}
	changed := c
	changed.Begin = 2
	changed.End = 2
	other, _ := MarshalControl(changed)
	if _, _, err := ValidateSnapshot(b, p, other[:], m); !errors.Is(err, ErrBusy) {
		t.Fatal(err)
	}
	// Internal envelope mismatch must not be authenticated by the control alone.
	h, body, _ := Parse(p)
	h.Namespace[1] = 1
	wrong, _ := Marshal(h, body)
	if _, _, err := ValidateSnapshot(b, wrong, b, m); !errors.Is(err, ErrMapping) {
		t.Fatal(err)
	}
}

func FuzzSnapshot(f *testing.F) {
	p, _ := Marshal(fixture(), make([]byte, 24))
	c := Control{Begin: 2, End: 2, Capacity: 1024, Used: uint64(len(p)), Generation: 1, Namespace: fixture().Namespace}
	b, _ := MarshalControl(c)
	f.Add(b[:], p, b[:])
	f.Add([]byte{}, []byte{}, []byte{})
	f.Fuzz(func(t *testing.T, b, p, a []byte) {
		h, body, err := ValidateSnapshot(b, p, a, Mapping{fixture().Namespace, 1, 1024})
		if err == nil {
			r, e := Marshal(h, body)
			if e != nil || !bytes.Equal(r, p) || !bytes.Equal(b, a) {
				t.Fatal("invalid success")
			}
		}
	})
}
