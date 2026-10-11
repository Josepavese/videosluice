package monotime

import "testing"

func TestProcessClockInitializedAndNondecreasing(t *testing.T) {
	if (*System)(nil).NowNS() != 0 || (&System{}).NowNS() != 0 {
		t.Fatal("invalid clock accepted")
	}
	clock := New()
	previous := clock.NowNS()
	if previous == 0 {
		t.Fatal("clock did not initialize")
	}
	for i := 0; i < 1000; i++ {
		current := clock.NowNS()
		if current < previous {
			t.Fatal("clock regressed")
		}
		previous = current
	}
}
