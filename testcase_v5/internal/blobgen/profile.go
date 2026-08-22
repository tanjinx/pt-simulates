package blobgen

import "fmt"

// SizeClass labels one of v4's three blob size bands. The class
// determines the size ranges and the metadata text length per row.
type SizeClass int

const (
	Small SizeClass = iota
	Medium
	Large
)

// classRanges mirrors v4's profile size bands. The bytes themselves are
// Go-PRNG-native (statistical parity, not byte parity).
var classRanges = map[SizeClass]struct {
	contents     [2]int
	highlight    [2]int
	metadataText int
	externalPtr  [2]int
}{
	Small: {
		contents:     [2]int{2 * 1024, 8 * 1024},
		highlight:    [2]int{1 * 1024, 4 * 1024},
		metadataText: 2048,
		externalPtr:  [2]int{512, 2 * 1024},
	},
	Medium: {
		contents:     [2]int{64 * 1024, 256 * 1024},
		highlight:    [2]int{32 * 1024, 128 * 1024},
		metadataText: 4096,
		externalPtr:  [2]int{4 * 1024, 16 * 1024},
	},
	Large: {
		contents:     [2]int{1 * 1024 * 1024, 2 * 1024 * 1024},
		highlight:    [2]int{512 * 1024, 1536 * 1024},
		metadataText: 8192,
		// external_ptr is BLOB (64 KiB ceiling); v4 caps at 60 KiB.
		externalPtr: [2]int{32 * 1024, 60 * 1024},
	},
}

// Profile reflects tc_config.json init.blob_profile after validation. The
// three percentages MUST sum to 100; this is enforced by config.Validate so
// blobgen can assume it here.
type Profile struct {
	SmallPct  int
	MediumPct int
	LargePct  int
}

// Cycle is the per-row size-class ordering: a slice of length 100 where
// SmallPct entries are Small, MediumPct entries are Medium, LargePct
// entries are Large. Row N picks Cycle[N % 100]; this preserves v4's
// repeatable distribution shape.
type Cycle []SizeClass

// NewCycle builds the cycle slice once per init phase. Validate the inputs
// at the call site (config.Validate already does this).
func NewCycle(p Profile) Cycle {
	if total := p.SmallPct + p.MediumPct + p.LargePct; total != 100 {
		// We do not silently coerce — config.Validate is the boundary, and
		// reaching here with bad input is a programmer error (panic only
		// on programmer error).
		panic(fmt.Sprintf("blobgen: profile sums to %d, want 100", total))
	}
	out := make(Cycle, 0, 100)
	for i := 0; i < p.SmallPct; i++ {
		out = append(out, Small)
	}
	for i := 0; i < p.MediumPct; i++ {
		out = append(out, Medium)
	}
	for i := 0; i < p.LargePct; i++ {
		out = append(out, Large)
	}
	return out
}

// Pick returns the SizeClass for row N.
func (c Cycle) Pick(rowN uint64) SizeClass {
	return c[int(rowN%uint64(len(c)))]
}
