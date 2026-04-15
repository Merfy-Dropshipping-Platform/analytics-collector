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

	// Time series
	ts, err := queryTimeSeries(ctx, pool, req.ShopID, start, end)
	if err != nil {
		return nil, err
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
			CASE WHEN SUM(unique_sessions) > 0
				THEN LEAST(ROUND(
					SUM(CASE WHEN unique_sessions > 0 THEN order_count ELSE 0 END)::numeric
					/ SUM(unique_sessions) * 100, 2), 100)
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

	var ts []DashboardTimeSeries
	for rows.Next() {
		var t DashboardTimeSeries
		if err := rows.Scan(&t.Day, &t.RevenueCents, &t.Orders, &t.Visitors, &t.Sessions, &t.PageViews); err != nil {
			return nil, err
		}
		ts = append(ts, t)
	}
	return ts, nil
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
