package noise

import "time"

// BurstSchedule decides the rate multiplier to apply at time t since start.
// Off-state returns 1.0 always. On-state alternates burst_seconds at
// Multiplier × baseline with interval_seconds at 1.0 × baseline.
//
// The cycle is deterministic and stateless: given t, the multiplier is a
// pure function of (t, BurstSeconds, IntervalSeconds, Multiplier). This is
// the single-pass single-algorithm choice the harness requires.
type BurstSchedule struct {
	Enabled         bool
	Multiplier      float64
	BurstSeconds    int
	IntervalSeconds int
}

// Multiplier returns the rate multiplier at time t since the start of the
// schedule. Off-state and zero-cycle return 1.0.
func (s BurstSchedule) MultiplierAt(t time.Duration) float64 {
	if !s.Enabled || s.Multiplier <= 0 || s.BurstSeconds <= 0 {
		return 1.0
	}
	cycle := s.BurstSeconds + s.IntervalSeconds
	if cycle <= 0 {
		return 1.0
	}
	phase := int(t.Seconds()) % cycle
	if phase < s.BurstSeconds {
		return s.Multiplier
	}
	return 1.0
}
