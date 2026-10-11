package frameaccess

import "testing"

func TestSequenceWindowBoundaries(t *testing.T) {
	var w sequenceWindow
	if w.accepts(0) {
		t.Fatal("zero accepted")
	}
	w.commit(100)
	for _, n := range []uint64{100, 36, 1} {
		if w.accepts(n) {
			t.Fatal("replay or outside window", n)
		}
	}
	for _, n := range []uint64{37, 99, 101} {
		if !w.accepts(n) {
			t.Fatal("valid reorder", n)
		}
	}
	w.commit(37)
	if w.accepts(37) {
		t.Fatal("replay at boundary")
	}
	w.commit(99)
	w.commit(101)
	if w.accepts(99) || w.accepts(100) || w.accepts(101) || w.accepts(37) {
		t.Fatal("lost replay state")
	}
	w.commit(200)
	if w.accepts(136) || !w.accepts(137) {
		t.Fatal("large jump")
	}
	w.commit(^uint64(0))
	if w.accepts(0) || w.accepts(^uint64(0)) || !w.accepts(^uint64(0)-63) || w.accepts(^uint64(0)-64) {
		t.Fatal("uint64 boundary")
	}
}
