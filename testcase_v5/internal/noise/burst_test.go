package noise

import (
	"testing"
	"time"
)

func TestBurstScheduleDisabled(t *testing.T) {
	s := BurstSchedule{}
	if got := s.MultiplierAt(5 * time.Second); got != 1.0 {
		t.Fatalf("disabled schedule must return 1.0, got %v", got)
	}
}

func TestBurstScheduleAlternates(t *testing.T) {
	s := BurstSchedule{
		Enabled: true, Multiplier: 4.0,
		BurstSeconds: 20, IntervalSeconds: 50,
	}
	cases := []struct {
		t    time.Duration
		want float64
	}{
		{0 * time.Second, 4.0},  // burst phase
		{19 * time.Second, 4.0}, // last burst second
		{20 * time.Second, 1.0}, // interval starts
		{69 * time.Second, 1.0}, // last interval second
		{70 * time.Second, 4.0}, // next burst starts (cycle=70)
		{89 * time.Second, 4.0}, // burst still on
		{90 * time.Second, 1.0}, // next interval
	}
	for _, c := range cases {
		if got := s.MultiplierAt(c.t); got != c.want {
			t.Errorf("t=%s want %v got %v", c.t, c.want, got)
		}
	}
}
