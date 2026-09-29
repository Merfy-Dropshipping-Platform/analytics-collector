package handler

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// «Сессии по локациям» — люди по месту первого визита за период (владелец 28.09: «люди в обеих»).
// Единица и источник — те же, что у «Посещаемости»: посетители считаются из periodVisitsSQL
// (period_uniques.go), каждый — один раз, в гео своего первого визита. Поэтому сумма людей по
// локациям всегда равна unique_visitors дашборда за тот же период.
//
// Имена полей ответа (sessions, total_sessions) — прежние: это контракт шлюза и back-office
// (by-location.dto.ts, kubb). Смысл — люди.
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
	Country string  `json:"country"`  // ISO-3166 alpha-2; "" → "Не определён" bucket
	Subject string  `json:"subject"`  // subject; annexed territories already normalized on ingest
	City    string  `json:"city"`     // "" unless level=="city"
	People  int64   `json:"sessions"` // люди, чей первый визит за период — в этой локации
	Orders  int64   `json:"orders"`   // net: purchase − order_cancel
	Share   float64 `json:"share"`    // 0..100, 2 decimals, people/total*100
}

type ByLocationResponse struct {
	Rows        []LocationRow `json:"rows"`           // init []LocationRow{} → "rows":[] never null
	TotalPeople int64         `json:"total_sessions"` // все люди за период = unique_visitors; делитель долей
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

	// Человек считается один раз за весь период (правило 1, period_uniques.go).
	// total_sessions is the share denominator over ALL groups (unlimited), so shares of the
	// (possibly limited) rows sum to ≤100 and reflect the true whole. Rows init empty (not nil)
	// → serializes as "rows":[] for a clean FE contract.
	result, total, err := periodLocations(ctx, pool, sel, grp, shopFilter, start, end, limit)
	if err != nil {
		return nil, err
	}

	for i := range result {
		result[i].Share = roundShare(result[i].People, total)
	}

	return ByLocationResponse{Rows: result, TotalPeople: total}, nil
}

// roundShare returns part/total*100 rounded to 2 decimals; total==0 → 0 (no divide-by-zero).
func roundShare(part, total int64) float64 {
	if total <= 0 {
		return 0
	}
	return float64(int64(float64(part)/float64(total)*10000+0.5)) / 100
}
