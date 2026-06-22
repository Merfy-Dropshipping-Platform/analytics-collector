package handler

import (
	"testing"
	"time"
)

// Intraday: a 24h UTC window must yield exactly 12 ascending 2-hour buckets,
// each labelled "YYYY-MM-DDTHH:00" with the hour stepping 00,02,...,22.
func TestBuildIntradayBucketsTwelveOrderedEntries(t *testing.T) {
	start := time.Date(2026, 6, 22, 0, 0, 0, 0, time.UTC)
	end := start.Add(24 * time.Hour)

	ts := buildIntradayBuckets(start, end, map[int64]DashboardTimeSeries{})

	if len(ts) != 12 {
		t.Fatalf("expected 12 buckets, got %d", len(ts))
	}

	wantHours := []string{
		"00:00", "02:00", "04:00", "06:00", "08:00", "10:00",
		"12:00", "14:00", "16:00", "18:00", "20:00", "22:00",
	}
	for i, entry := range ts {
		wantDay := "2026-06-22T" + wantHours[i]
		if entry.Day != wantDay {
			t.Fatalf("bucket %d: expected Day %q, got %q", i, wantDay, entry.Day)
		}
		if entry.Day[:10] != "2026-06-22" {
			t.Fatalf("bucket %d: first 10 chars should be the date 2026-06-22, got %q", i, entry.Day[:10])
		}
	}
}

// Empty windows must be zero-filled while still carrying the correct label.
func TestBuildIntradayBucketsZeroFill(t *testing.T) {
	start := time.Date(2026, 6, 22, 0, 0, 0, 0, time.UTC)
	end := start.Add(24 * time.Hour)

	ts := buildIntradayBuckets(start, end, map[int64]DashboardTimeSeries{})

	for i, entry := range ts {
		if entry.RevenueCents != 0 || entry.Orders != 0 || entry.Visitors != 0 ||
			entry.Sessions != 0 || entry.PageViews != 0 {
			t.Fatalf("bucket %d: expected all-zero metrics, got %+v", i, entry)
		}
	}
}

// Populated buckets keep their metric values and get the window's label.
func TestBuildIntradayBucketsPopulated(t *testing.T) {
	start := time.Date(2026, 6, 22, 0, 0, 0, 0, time.UTC)
	end := start.Add(24 * time.Hour)

	// Window starting at 02:00 UTC -> idx = epoch/7200.
	twoAM := start.Add(2 * time.Hour)
	idx := twoAM.Unix() / 7200

	byBucket := map[int64]DashboardTimeSeries{
		idx: {
			Day:          "ignored-should-be-overwritten",
			RevenueCents: 50000,
			Orders:       3,
			Visitors:     7,
			Sessions:     9,
			PageViews:    42,
		},
	}

	ts := buildIntradayBuckets(start, end, byBucket)

	if len(ts) != 12 {
		t.Fatalf("expected 12 buckets, got %d", len(ts))
	}

	// Bucket index 1 corresponds to 02:00.
	got := ts[1]
	if got.Day != "2026-06-22T02:00" {
		t.Fatalf("expected Day 2026-06-22T02:00, got %q", got.Day)
	}
	if got.RevenueCents != 50000 || got.Orders != 3 || got.Visitors != 7 ||
		got.Sessions != 9 || got.PageViews != 42 {
		t.Fatalf("populated bucket lost its values: %+v", got)
	}

	// A neighbouring window stays zero-filled.
	if ts[0].RevenueCents != 0 || ts[0].Orders != 0 {
		t.Fatalf("bucket 0 should be zero-filled, got %+v", ts[0])
	}
}

// periodRange("24h") must produce exactly a 24h window aligned to UTC midnight.
func TestPeriodRange24h(t *testing.T) {
	ref := time.Date(2026, 6, 22, 13, 37, 5, 0, time.UTC)
	start, end := periodRange("24h", ref)

	wantEnd := time.Date(2026, 6, 23, 0, 0, 0, 0, time.UTC)
	wantStart := time.Date(2026, 6, 22, 0, 0, 0, 0, time.UTC)

	if !start.Equal(wantStart) {
		t.Fatalf("expected start %v, got %v", wantStart, start)
	}
	if !end.Equal(wantEnd) {
		t.Fatalf("expected end %v, got %v", wantEnd, end)
	}
	if end.Sub(start) != 24*time.Hour {
		t.Fatalf("expected 24h window, got %v", end.Sub(start))
	}
}

// resolveRange must parse a valid custom range and otherwise fall back to the
// preset period (default 30d when the token is unknown).
func TestResolveRangeCustomAndFallback(t *testing.T) {
	ref := time.Date(2026, 6, 22, 13, 37, 5, 0, time.UTC)

	// Custom range: end day fully included (end + 24h).
	start, end := resolveRange("custom", "2026-06-01", "2026-06-10", ref)
	wantStart := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	wantEnd := time.Date(2026, 6, 11, 0, 0, 0, 0, time.UTC)
	if !start.Equal(wantStart) {
		t.Fatalf("custom: expected start %v, got %v", wantStart, start)
	}
	if !end.Equal(wantEnd) {
		t.Fatalf("custom: expected end %v, got %v", wantEnd, end)
	}

	// custom with missing dates falls through to periodRange default (30d).
	dStart, dEnd := resolveRange("custom", "", "", ref)
	pStart, pEnd := periodRange("custom", ref)
	if !dStart.Equal(pStart) || !dEnd.Equal(pEnd) {
		t.Fatalf("custom-missing should fall back to periodRange: got [%v,%v) want [%v,%v)", dStart, dEnd, pStart, pEnd)
	}

	// Unknown token defaults to a 30-day window.
	uStart, uEnd := resolveRange("totally-unknown", "", "", ref)
	if uEnd.Sub(uStart) != 30*24*time.Hour {
		t.Fatalf("unknown token should default to 30d window, got %v", uEnd.Sub(uStart))
	}
}
