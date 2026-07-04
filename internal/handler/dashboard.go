package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type DashboardRequest struct {
	ShopID   string `json:"shopId"`
	TenantID string `json:"tenantId"`
	Period   string `json:"period"`
	From     string `json:"from,omitempty"`
	To       string `json:"to,omitempty"`
}

type DashboardKPI struct {
	TotalRevenueCents int64   `json:"total_revenue_cents"`
	TotalOrders       int64   `json:"total_orders"`
	AvgOrderCents     int64   `json:"avg_order_cents"`
	UniqueVisitors    int64   `json:"unique_visitors"`
	UniqueSessions    int64   `json:"unique_sessions"`
	PageViews         int64   `json:"page_views"`
	ConversionRate    float64 `json:"conversion_rate"`
}

type DashboardTimeSeries struct {
	Day          string `json:"date"`
	RevenueCents int64  `json:"total_revenue_cents"`
	Orders       int64  `json:"total_orders"`
	Visitors     int64  `json:"unique_visitors"`
	Sessions     int64  `json:"unique_sessions"`
	PageViews    int64  `json:"page_views"`
}

type DashboardResponse struct {
	Period     string                `json:"period"`
	KPI        DashboardKPI          `json:"kpi"`
	KPIPrev    DashboardKPI          `json:"kpi_prev"`
	TimeSeries []DashboardTimeSeries `json:"time_series"`
}

func HandleDashboard(ctx context.Context, pool *pgxpool.Pool, payload json.RawMessage) (any, error) {
	var req DashboardRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, fmt.Errorf("invalid request: %w", err)
	}
	if req.ShopID == "" || req.Period == "" {
		return nil, fmt.Errorf("shopId and period are required")
	}

	now := time.Now().UTC()
	start, end := resolveRange(req.Period, req.From, req.To, now)
	prevStart, prevEnd := resolveRange(req.Period, "", "", start.Add(-time.Second))

	// Current KPI
	kpi, err := queryKPI(ctx, pool, req.ShopID, start, end)
	if err != nil {
		return nil, err
	}

	// Previous KPI
	kpiPrev, err := queryKPI(ctx, pool, req.ShopID, prevStart, prevEnd)
	if err != nil {
		return nil, err
	}

	// Time series.
	// Intraday (12 two-hour buckets, read LIVE from bronze.events) when the resolved window is a
	// single day AND that day is still within bronze retention (~30 days). This covers both the
	// "24h" preset and a custom single calendar date (from==to), which resolve to the same 24h
	// window. Older single days fall back to the daily gold path (gold.dashboard_kpi retains
	// 13 months); reading bronze for them would return empty buckets and silently drop data.
	singleDay := end.Sub(start) == 24*time.Hour
	withinBronzeRetention := !start.Before(now.AddDate(0, 0, -30))

	var ts []DashboardTimeSeries
	if singleDay && withinBronzeRetention {
		byBucket, errTS := queryIntradayBuckets(ctx, pool, req.ShopID, start, end)
		if errTS != nil {
			return nil, errTS
		}
		ts = buildIntradayBuckets(start, end, byBucket)
	} else {
		ts, err = queryTimeSeries(ctx, pool, req.ShopID, start, end)
		if err != nil {
			return nil, err
		}
	}

	return DashboardResponse{
		Period:     req.Period,
		KPI:        kpi,
		KPIPrev:    kpiPrev,
		TimeSeries: ts,
	}, nil
}

func queryKPI(ctx context.Context, pool *pgxpool.Pool, shopID string, start, end time.Time) (DashboardKPI, error) {
	var kpi DashboardKPI
	err := pool.QueryRow(ctx, `
		SELECT
			COALESCE(SUM(total_revenue_cents), 0),
			COALESCE(SUM(order_count), 0),
			CASE WHEN SUM(order_count) > 0
				THEN (SUM(total_revenue_cents) / SUM(order_count))::bigint
				ELSE 0 END,
			COALESCE(SUM(unique_visitors), 0),
			COALESCE(SUM(unique_sessions), 0),
			COALESCE(SUM(page_views), 0),
			CASE WHEN SUM(unique_visitors) > 0
				THEN LEAST(ROUND(
					SUM(CASE WHEN unique_visitors > 0 THEN order_count ELSE 0 END)::numeric
					/ SUM(unique_visitors) * 100, 2), 100)
				ELSE 0 END
		FROM gold.dashboard_kpi
		WHERE shop_id = $1 AND day >= $2::date AND day < $3::date
	`, shopID, start, end).Scan(
		&kpi.TotalRevenueCents, &kpi.TotalOrders, &kpi.AvgOrderCents,
		&kpi.UniqueVisitors, &kpi.UniqueSessions, &kpi.PageViews,
		&kpi.ConversionRate,
	)
	return kpi, err
}

func queryTimeSeries(ctx context.Context, pool *pgxpool.Pool, shopID string, start, end time.Time) ([]DashboardTimeSeries, error) {
	rows, err := pool.Query(ctx, `
		SELECT
			day::text,
			COALESCE(total_revenue_cents, 0),
			COALESCE(order_count, 0),
			COALESCE(unique_visitors, 0),
			COALESCE(unique_sessions, 0),
			COALESCE(page_views, 0)
		FROM gold.dashboard_kpi
		WHERE shop_id = $1 AND day >= $2::date AND day < $3::date
		ORDER BY day
	`, shopID, start, end)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	dataByDay := make(map[string]DashboardTimeSeries)
	for rows.Next() {
		var t DashboardTimeSeries
		if err := rows.Scan(&t.Day, &t.RevenueCents, &t.Orders, &t.Visitors, &t.Sessions, &t.PageViews); err != nil {
			return nil, err
		}
		key := t.Day[:10]
		t.Day = key
		dataByDay[key] = t
	}

	var ts []DashboardTimeSeries
	for d := start; d.Before(end); d = d.Add(24 * time.Hour) {
		key := d.Format("2006-01-02")
		if entry, ok := dataByDay[key]; ok {
			ts = append(ts, entry)
		} else {
			ts = append(ts, DashboardTimeSeries{Day: key})
		}
	}
	return ts, nil
}

// queryIntradayBuckets reads 2-hour UTC buckets LIVE from bronze.events for the
// given [start, end) window. Buckets are keyed by floor(epoch/7200) so they
// align with the loop in buildIntradayBuckets. Used only for the "24h" period.
func queryIntradayBuckets(ctx context.Context, pool *pgxpool.Pool, shopID string, start, end time.Time) (map[int64]DashboardTimeSeries, error) {
	rows, err := pool.Query(ctx, `
		WITH deduped_orders AS (
			SELECT (floor(extract(epoch from e.created_at)/7200))::bigint AS bucket_idx, e.order_id, e.event_type, MAX(e.order_total_cents) AS order_total_cents
			FROM bronze.events e
			WHERE e.shop_id=$1 AND e.created_at >= $2 AND e.created_at < $3 AND e.event_type IN ('purchase','order_cancel') AND e.order_id IS NOT NULL
			  -- Orphan guard (mirrors migration 014): subtract an order_cancel only when a paired
			  -- purchase exists for the same (shop_id, order_id). The EXISTS subquery scans bronze
			  -- WITHOUT the [start,end) window — the paired purchase may be on an earlier day.
			  AND (e.event_type='purchase' OR EXISTS (
			    SELECT 1 FROM bronze.events p
			    WHERE p.event_type='purchase' AND p.shop_id=e.shop_id AND p.order_id=e.order_id
			  ))
			GROUP BY 1, e.order_id, e.event_type
		),
		orders AS (
			SELECT bucket_idx,
				COUNT(*) FILTER (WHERE event_type='purchase') - COUNT(*) FILTER (WHERE event_type='order_cancel') AS order_count,
				COALESCE(SUM(order_total_cents) FILTER (WHERE event_type='purchase'),0) - COALESCE(SUM(order_total_cents) FILTER (WHERE event_type='order_cancel'),0) AS total_revenue_cents
			FROM deduped_orders GROUP BY bucket_idx
		),
		traffic AS (
			SELECT (floor(extract(epoch from created_at)/7200))::bigint AS bucket_idx,
				COUNT(*) FILTER (WHERE event_type='page_view') AS page_views,
				COUNT(DISTINCT session_id) FILTER (WHERE event_type IN ('page_view','session_start')) AS unique_sessions,
				COUNT(DISTINCT visitor_id) AS unique_visitors
			FROM bronze.events WHERE shop_id=$1 AND created_at >= $2 AND created_at < $3 GROUP BY 1
		)
		SELECT COALESCE(t.bucket_idx,o.bucket_idx) AS bucket_idx, COALESCE(o.total_revenue_cents,0), COALESCE(o.order_count,0), COALESCE(t.unique_visitors,0), COALESCE(t.unique_sessions,0), COALESCE(t.page_views,0)
		FROM traffic t FULL OUTER JOIN orders o ON t.bucket_idx = o.bucket_idx
	`, shopID, start, end)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	byBucket := make(map[int64]DashboardTimeSeries)
	for rows.Next() {
		var bucketIdx int64
		var t DashboardTimeSeries
		if err := rows.Scan(&bucketIdx, &t.RevenueCents, &t.Orders, &t.Visitors, &t.Sessions, &t.PageViews); err != nil {
			return nil, err
		}
		byBucket[bucketIdx] = t
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return byBucket, nil
}

// buildIntradayBuckets walks 2-hour steps over [start, end) and emits exactly
// one entry per window (12 for a 24h window), in ascending order. Each entry's
// Day is an ISO datetime "YYYY-MM-DDTHH:00" whose first 10 chars stay
// YYYY-MM-DD. Populated buckets come from byBucket (keyed by floor(epoch/7200));
// empty windows are zero-filled. Pure and DB-free for unit testing.
func buildIntradayBuckets(start, end time.Time, byBucket map[int64]DashboardTimeSeries) []DashboardTimeSeries {
	var ts []DashboardTimeSeries
	for t := start; t.Before(end); t = t.Add(2 * time.Hour) {
		idx := t.Unix() / 7200
		day := t.UTC().Format("2006-01-02T15:04")
		if entry, ok := byBucket[idx]; ok {
			entry.Day = day
			ts = append(ts, entry)
		} else {
			ts = append(ts, DashboardTimeSeries{Day: day})
		}
	}
	return ts
}

func periodRange(period string, ref time.Time) (time.Time, time.Time) {
	u := ref.UTC()
	end := time.Date(u.Year(), u.Month(), u.Day(), 0, 0, 0, 0, time.UTC).Add(24 * time.Hour)
	switch period {
	case "24h":
		return end.Add(-24 * time.Hour), end
	case "7d":
		return end.Add(-7 * 24 * time.Hour), end
	case "30d":
		return end.Add(-30 * 24 * time.Hour), end
	case "3m":
		return end.AddDate(0, -3, 0), end
	case "6m":
		return end.AddDate(0, -6, 0), end
	case "1y":
		return end.AddDate(-1, 0, 0), end
	default:
		return end.Add(-30 * 24 * time.Hour), end
	}
}

// resolveRange handles both preset periods and custom date ranges.
// For period="custom", it parses from/to as YYYY-MM-DD strings.
// Falls back to periodRange for preset periods.
func resolveRange(period, from, to string, ref time.Time) (time.Time, time.Time) {
	if period == "custom" && from != "" && to != "" {
		start, errS := time.Parse("2006-01-02", from)
		end, errE := time.Parse("2006-01-02", to)
		if errS == nil && errE == nil {
			// Include the end day fully
			return start, end.Add(24 * time.Hour)
		}
	}
	return periodRange(period, ref)
}
