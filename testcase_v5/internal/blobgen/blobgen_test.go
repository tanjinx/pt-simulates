package blobgen

import (
	"bytes"
	"math"
	"testing"
)

func TestDeterminism(t *testing.T) {
	in := Inputs{
		TenantID: 123,
		StartID:  1000,
		DateBase: 1_700_000_000,
		DateStep: 60,
		Seed:     42,
		Class:    Medium,
	}
	a := Generate(7, in)
	b := Generate(7, in)
	if a.ID != b.ID || a.DateCreate != b.DateCreate {
		t.Fatal("scalar fields diverge across calls")
	}
	if !bytes.Equal(a.Contents, b.Contents) {
		t.Fatalf("contents diverge: %d vs %d bytes", len(a.Contents), len(b.Contents))
	}
	if !bytes.Equal(a.ContentsHighlight, b.ContentsHighlight) {
		t.Fatal("highlight diverges")
	}
	if a.Metadata != b.Metadata {
		t.Fatal("metadata diverges")
	}
	if a.MD5 != b.MD5 {
		t.Fatal("md5 diverges")
	}
}

func TestSeedSeparation(t *testing.T) {
	in1 := Inputs{TenantID: 1, StartID: 1, DateBase: 1, DateStep: 1, Seed: 1, Class: Small}
	in2 := in1
	in2.Seed = 2
	a := Generate(0, in1)
	b := Generate(0, in2)
	if bytes.Equal(a.Contents, b.Contents) {
		t.Fatal("different seeds produced identical contents")
	}
}

func TestSizeDistributionWithinTolerance(t *testing.T) {
	profile := Profile{SmallPct: 30, MediumPct: 30, LargePct: 40}
	cycle := NewCycle(profile)
	const N = 10_000
	counts := map[SizeClass]int{}
	for i := uint64(0); i < N; i++ {
		counts[cycle.Pick(i)]++
	}
	checkProportion(t, counts[Small], N, 0.30, 0.02)
	checkProportion(t, counts[Medium], N, 0.30, 0.02)
	checkProportion(t, counts[Large], N, 0.40, 0.02)
}

func TestNewCyclePanicsOnBadProfile(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on bad profile sum")
		}
	}()
	_ = NewCycle(Profile{SmallPct: 10, MediumPct: 10, LargePct: 10})
}

func TestRowValuesMatchColumns(t *testing.T) {
	in := Inputs{TenantID: 1, StartID: 1, DateBase: 1, DateStep: 1, Seed: 1, Class: Small}
	row := Generate(0, in)
	cols := Columns()
	vals := row.Values()
	if len(cols) != len(vals) {
		t.Fatalf("columns=%d values=%d (mismatch)", len(cols), len(vals))
	}
	if len(cols) != 43 {
		t.Fatalf("expected 43 columns to match v4 INSERT_COLUMNS; got %d", len(cols))
	}
}

func checkProportion(t *testing.T, count, n int, want float64, tol float64) {
	t.Helper()
	got := float64(count) / float64(n)
	if math.Abs(got-want) > tol {
		t.Errorf("proportion=%.3f want %.3f ±%.3f (count=%d/%d)", got, want, tol, count, n)
	}
}
