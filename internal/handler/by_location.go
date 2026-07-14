package handler

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// "Сессии по локациям" — READ-ONLY rollup over silver.daily_geo (per-triple counts are
// disjoint and additive, so COUNT(DISTINCT session) rolls up by plain SUM; see migration 015).
//
// Two RPC entrypoints share one implementation:
//   - analytics.global.by_location  → HandleGlobalByLocation (all shops, no shop filter)
//   - analytics.by_location         → HandleByLocation      (per-shop, shopId required; фаза 6)
//
// Wire contract is 1:1 with by-location.dto.ts (gateway) — see the json tags below.

type GlobalByLocationRequest struct {
	Period string `json:"period"`
	From   string `json:"from,omitempty"`
	To     string `json:"to,omitempty"`
	// Level selects granularity: "country" | "subject" (default) | "city". Gateway v1 omits it.
	Level string `json:"level,omitempty"`
	// Limit caps the returned rows (default 100, max 500). Gateway v1 omits it.
	Limit int `json:"limit,omitempty"`
	// ShopID is required only on the per-shop pattern.
	ShopID string `json:"shopId,omitempty"`
}

type LocationRow struct {
	Country  string  `json:"country"`  // ISO-3166 alpha-2; "" → "Не определён" bucket
	Subject  string  `json:"subject"`  // subject; annexed territories already normalized on ingest
	City     string  `json:"city"`     // "" unless level=="city"
	Sessions int64   `json:"sessions"` // COUNT(DISTINCT session_id) over the period
	Orders   int64   `json:"orders"`   // net: purchase − order_cancel
	Share    float64 `json:"share"`    // 0..100, 2 decimals, sessions/total_sessions*100
}

type ByLocationResponse struct {
	Rows          []LocationRow `json:"rows"`           // init []LocationRow{} → "rows":[] never null
	TotalSessions int64         `json:"total_sessions"` // denominator of share; 0 → rows empty
}

// HandleGlobalByLocation answers the platform-wide widget (SuperAdmin): no shop filter.
func HandleGlobalByLocation(ctx context.Context, pool *pgxpool.Pool, payload json.RawMessage) (any, error) {
	return handleByLocation(ctx, pool, payload, false)
}

// HandleByLocation answers a single shop's widget (shop-auth): shopId is required (фаза 6).
func HandleByLocation(ctx context.Context, pool *pgxpool.Pool, payload json.RawMessage) (any, error) {
	return handleByLocation(ctx, pool, payload, true)
}

func handleByLocation(ctx context.Context, pool *pgxpool.Pool, payload json.RawMessage, perShop bool) (any, error) {
	var req GlobalByLocationRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, fmt.Errorf("invalid request: %w", err)
	}
	if req.Period == "" {
		return nil, fmt.Errorf("period is required")
	}
	if perShop && req.ShopID == "" {
		return nil, fmt.Errorf("shopId is required")
	}

	start, end := resolveRange(req.Period, req.From, req.To, timeNow())

	limit := req.Limit
	if limit <= 0 || limit > 500 {
		limit = 100
	}

	// selectCols ALWAYS produces exactly 3 text columns (country, subject, city) so Scan is
	// uniform across levels; NULL geo → '' via COALESCE. grp matches the selected columns.
	var sel, grp string
	switch req.Level {
	case "country":
		sel = `COALESCE(geo_country,''), ''::text, ''::text`
		grp = `geo_country`
	case "city":
		sel = `COALESCE(geo_country,''), COALESCE(geo_subject,''), COALESCE(geo_city,'')`
		grp = `geo_country, geo_subject, geo_city`
	default: // "subject"
		sel = `COALESCE(geo_country,''), COALESCE(geo_subject,''), ''::text`
		grp = `geo_country, geo_subject`
	}

	// shopFilter is a nullable bind: NULL → no filter (global); a value → shop_id = $3.
	var shopFilter any
	if perShop {
		shopFilter = req.ShopID
	}

	query := fmt.Sprintf(`
		SELECT %s, SUM(sessions), SUM(orders)
		FROM silver.daily_geo
		WHERE day >= $1::date AND day < $2::date
		  AND ($3::text IS NULL OR shop_id = $3)
		GROUP BY %s
		ORDER BY SUM(sessions) DESC
		LIMIT $4
	`, sel, grp)

	rows, err := pool.Query(ctx, query, start, end, shopFilter, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Init empty (not nil) → serializes as "rows":[] for a clean FE contract.
	result := []LocationRow{}
	for rows.Next() {
		var lr LocationRow
		if err := rows.Scan(&lr.Country, &lr.Subject, &lr.City, &lr.Sessions, &lr.Orders); err != nil {
			return nil, err
		}
		result = append(result, lr)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// total_sessions is the share denominator over ALL groups (unlimited), so shares of the
	// (possibly limited) rows sum to ≤100 and reflect the true whole.
	var total int64
	err = pool.QueryRow(ctx, `
		SELECT COALESCE(SUM(sessions), 0)
		FROM silver.daily_geo
		WHERE day >= $1::date AND day < $2::date
		  AND ($3::text IS NULL OR shop_id = $3)
	`, start, end, shopFilter).Scan(&total)
	if err != nil {
		return nil, err
	}

	for i := range result {
		result[i].Share = roundShare(result[i].Sessions, total)
	}

	return ByLocationResponse{Rows: result, TotalSessions: total}, nil
}

// roundShare returns sessions/total*100 rounded to 2 decimals; total==0 → 0 (no divide-by-zero).
func roundShare(sessions, total int64) float64 {
	if total <= 0 {
		return 0
	}
	return float64(int64(float64(sessions)/float64(total)*10000+0.5)) / 100
}
