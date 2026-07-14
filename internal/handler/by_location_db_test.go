//go:build dbtest

package handler

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Integration test for the real by_location handler against a seeded Postgres.
// Gated behind the `dbtest` build tag and GEO_TEST_DB (a DATABASE_URL). Run:
//
//	GEO_TEST_DB=postgres://... go test -tags dbtest -run TestByLocationHandler_DB ./internal/handler/
func TestByLocationHandler_DB(t *testing.T) {
	dsn := os.Getenv("GEO_TEST_DB")
	if dsn == "" {
		t.Skip("GEO_TEST_DB not set")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer pool.Close()

	// Global, default level (subject), period 24h → the seeded shopA data.
	payload := json.RawMessage(`{"period":"24h"}`)
	out, err := HandleGlobalByLocation(ctx, pool, payload)
	if err != nil {
		t.Fatalf("HandleGlobalByLocation: %v", err)
	}
	resp, ok := out.(ByLocationResponse)
	if !ok {
		t.Fatalf("unexpected response type %T", out)
	}

	if resp.TotalSessions != 4 {
		t.Errorf("total_sessions = %d; want 4", resp.TotalSessions)
	}
	if len(resp.Rows) != 4 {
		t.Fatalf("rows = %d; want 4", len(resp.Rows))
	}
	// Sorted by sessions DESC (all 1 here) — assert content by subject.
	bySubject := map[string]LocationRow{}
	var shareSum float64
	var sawNull, sawCrimea, sawMoscow bool
	for _, r := range resp.Rows {
		bySubject[r.Subject] = r
		shareSum += r.Share
		if r.Country == "" && r.Subject == "" {
			sawNull = true
		}
		if r.Subject == "Республика Крым" && r.Country == "RU" {
			sawCrimea = true
		}
		if r.Subject == "Москва" {
			sawMoscow = true
			if r.Orders != 1 {
				t.Errorf("Москва orders = %d; want 1 (session-geo attribution)", r.Orders)
			}
		}
		// city must be empty at subject level
		if r.City != "" {
			t.Errorf("subject-level city = %q; want empty", r.City)
		}
	}
	if !sawNull {
		t.Error("NULL-geo bucket missing from rows")
	}
	if !sawCrimea {
		t.Error("Республика Крым (override) bucket missing")
	}
	if !sawMoscow {
		t.Error("Москва bucket missing")
	}
	// СПб had purchase o2 fully cancelled (dup cancel) → net 0.
	if spb, ok := bySubject["Санкт-Петербург"]; ok && spb.Orders != 0 {
		t.Errorf("СПб orders = %d; want 0 (purchase o2 cancelled, dup not double-counted)", spb.Orders)
	}
	if shareSum < 99.95 || shareSum > 100.05 {
		t.Errorf("sum(share) = %v; want ≈100", shareSum)
	}
	// Each of 4 equal buckets → 25.
	if m := bySubject["Москва"]; m.Share != 25.0 {
		t.Errorf("Москва share = %v; want 25.0", m.Share)
	}

	// level=country collapses to one RU + one NULL bucket.
	out, err = HandleGlobalByLocation(ctx, pool, json.RawMessage(`{"period":"24h","level":"country"}`))
	if err != nil {
		t.Fatalf("country level: %v", err)
	}
	cResp := out.(ByLocationResponse)
	var ruSessions int64
	for _, r := range cResp.Rows {
		if r.Subject != "" || r.City != "" {
			t.Errorf("country level should blank subject/city, got %+v", r)
		}
		if r.Country == "RU" {
			ruSessions = r.Sessions
		}
	}
	if ruSessions != 3 { // СПб + Москва + Крым
		t.Errorf("country-level RU sessions = %d; want 3", ruSessions)
	}

	// Empty period (far future) → rows [] (not nil) and total 0, no divide-by-zero.
	out, err = HandleGlobalByLocation(ctx, pool, json.RawMessage(`{"period":"custom","from":"2000-01-01","to":"2000-01-02"}`))
	if err != nil {
		t.Fatalf("empty period: %v", err)
	}
	eResp := out.(ByLocationResponse)
	if eResp.Rows == nil {
		t.Error("empty rows should be [] not nil")
	}
	if len(eResp.Rows) != 0 || eResp.TotalSessions != 0 {
		t.Errorf("empty period: rows=%d total=%d; want 0/0", len(eResp.Rows), eResp.TotalSessions)
	}

	// Per-shop handler must require shopId.
	if _, err := HandleByLocation(ctx, pool, json.RawMessage(`{"period":"24h"}`)); err == nil {
		t.Error("HandleByLocation without shopId should error")
	}
	// Per-shop with shopId works.
	out, err = HandleByLocation(ctx, pool, json.RawMessage(`{"period":"24h","shopId":"shopA"}`))
	if err != nil {
		t.Fatalf("per-shop: %v", err)
	}
	if out.(ByLocationResponse).TotalSessions != 4 {
		t.Errorf("per-shop total_sessions = %d; want 4", out.(ByLocationResponse).TotalSessions)
	}
	// Per-shop for a different shop → empty.
	out, _ = HandleByLocation(ctx, pool, json.RawMessage(`{"period":"24h","shopId":"nope"}`))
	if out.(ByLocationResponse).TotalSessions != 0 {
		t.Errorf("per-shop unknown shop total = %d; want 0", out.(ByLocationResponse).TotalSessions)
	}
}
