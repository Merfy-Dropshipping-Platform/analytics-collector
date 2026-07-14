package handler

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
)

// Contract parity (collector↔gateway↔FE): json.Marshal(LocationRow{}) must emit EXACTLY the
// six wire keys. If a field is renamed/added, this fails and flags a broken contract with
// by-location.dto.ts. revenue_cents is intentionally NOT in v1 (matview-only extension).
func TestLocationRow_WireKeys(t *testing.T) {
	data, err := json.Marshal(LocationRow{})
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatal(err)
	}
	got := make([]string, 0, len(m))
	for k := range m {
		got = append(got, k)
	}
	sort.Strings(got)
	want := []string{"city", "country", "orders", "sessions", "share", "subject"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("LocationRow JSON keys = %v; want %v", got, want)
	}
}

// ByLocationResponse wire keys must be exactly {rows, total_sessions}.
func TestByLocationResponse_WireKeys(t *testing.T) {
	data, err := json.Marshal(ByLocationResponse{Rows: []LocationRow{}})
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatal(err)
	}
	if _, ok := m["rows"]; !ok {
		t.Error("missing 'rows' key")
	}
	if _, ok := m["total_sessions"]; !ok {
		t.Error("missing 'total_sessions' key")
	}
	if len(m) != 2 {
		t.Errorf("unexpected keys: %v", m)
	}
}

// Empty rows must serialize as [] (not null) — a clean FE contract.
func TestByLocationResponse_EmptyRowsNotNull(t *testing.T) {
	data, err := json.Marshal(ByLocationResponse{Rows: []LocationRow{}, TotalSessions: 0})
	if err != nil {
		t.Fatal(err)
	}
	s := string(data)
	if want := `"rows":[]`; !contains(s, want) {
		t.Errorf("empty response = %s; want to contain %s", s, want)
	}
	if want := `"total_sessions":0`; !contains(s, want) {
		t.Errorf("empty response = %s; want to contain %s", s, want)
	}
}

func TestRoundShare(t *testing.T) {
	cases := []struct {
		sessions, total int64
		want            float64
	}{
		{0, 0, 0},        // divide-by-zero guard
		{5, 0, 0},        // total 0 → 0 even with sessions
		{50, 200, 25.0},  // exact
		{120, 200, 60.0}, // exact
		{1, 3, 33.33},    // rounded to 2 decimals
		{2, 3, 66.67},    // rounded up
		{200, 200, 100.0},
	}
	for _, c := range cases {
		if got := roundShare(c.sessions, c.total); got != c.want {
			t.Errorf("roundShare(%d,%d) = %v; want %v", c.sessions, c.total, got, c.want)
		}
	}
}

// Golden sample matches the wire contract and shares sum to ~100.
func TestGoldenSampleParsesAndShareSums(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("testdata", "by_location.sample.json"))
	if err != nil {
		t.Fatal(err)
	}
	var resp ByLocationResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		t.Fatalf("golden does not match ByLocationResponse: %v", err)
	}
	if resp.TotalSessions != 200 {
		t.Errorf("total_sessions = %d; want 200", resp.TotalSessions)
	}
	if len(resp.Rows) != 4 {
		t.Fatalf("rows = %d; want 4", len(resp.Rows))
	}
	// NULL-geo bucket present (empty country+subject) and counted.
	var sumShare float64
	var sawNullBucket bool
	for _, r := range resp.Rows {
		sumShare += r.Share
		if r.Country == "" && r.Subject == "" {
			sawNullBucket = true
		}
	}
	if !sawNullBucket {
		t.Error("golden should contain the NULL-geo bucket (country=='' subject=='')")
	}
	if sumShare < 99.95 || sumShare > 100.05 {
		t.Errorf("sum(share) = %v; want ≈100", sumShare)
	}
	// Override label present verbatim (canonical, base-agnostic).
	if resp.Rows[2].Subject != SubjCrimeaLabel {
		t.Errorf("row[2].subject = %q; want %q", resp.Rows[2].Subject, SubjCrimeaLabel)
	}
}

// SubjCrimeaLabel mirrors geo.SubjCrimea for the golden assertion without importing the geo
// package into handler tests (handler stores post-override strings verbatim).
const SubjCrimeaLabel = "Республика Крым"

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
