package handler

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

type TrafficRequest struct {
	ShopID   string `json:"shopId"`
	TenantID string `json:"tenantId"`
	Period   string `json:"period"`
	From     string `json:"from,omitempty"`
	To       string `json:"to,omitempty"`
}

type TrafficSummary struct {
	TotalVisitors     int64   `json:"total_visitors"`
	TotalSessions     int64   `json:"total_sessions"`
	TotalPageViews    int64   `json:"total_page_views"`
	AvgSessionDurSec  int64   `json:"avg_session_duration_sec"`
	BounceRate        float64 `json:"bounce_rate"`
}

type TrafficTS struct {
	Day      string `json:"day"`
	Visitors int64  `json:"visitors"`
	Sessions int64  `json:"sessions"`
	PageViews int64 `json:"page_views"`
}

type TopPage struct {
	PageURL        string `json:"page_url"`
	Views          int64  `json:"views"`
	UniqueVisitors int64  `json:"unique_visitors"`
}

type TopReferrer struct {
	Referrer string `json:"referrer"`
	Sessions int64  `json:"sessions"`
}

type TrafficResponse struct {
	Summary      TrafficSummary `json:"summary"`
	TimeSeries   []TrafficTS    `json:"time_series"`
	TopPages     []TopPage      `json:"top_pages"`
	TopReferrers []TopReferrer  `json:"top_referrers"`
}

func HandleTraffic(ctx context.Context, pool *pgxpool.Pool, payload json.RawMessage) (any, error) {
	var req TrafficRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, fmt.Errorf("invalid request: %w", err)
	}
	if req.ShopID == "" || req.Period == "" {
		return nil, fmt.Errorf("shopId and period are required")
	}

	start, end := resolveRange(req.Period, req.From, req.To, timeNow())

	// Summary
	var summary TrafficSummary
	err := pool.QueryRow(ctx, `
		SELECT
			COALESCE(SUM(unique_visitors), 0),
			COALESCE(SUM(unique_sessions), 0),
			COALESCE(SUM(page_views), 0)
		FROM silver.daily_traffic
		WHERE shop_id = $1 AND day >= $2 AND day < $3
	`, req.ShopID, start, end).Scan(&summary.TotalVisitors, &summary.TotalSessions, &summary.TotalPageViews)
	if err != nil {
		return nil, err
	}

	// Time series
	rows, err := pool.Query(ctx, `
		SELECT day::text, unique_visitors, unique_sessions, page_views
		FROM silver.daily_traffic
		WHERE shop_id = $1 AND day >= $2 AND day < $3
		ORDER BY day
	`, req.ShopID, start, end)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ts []TrafficTS
	for rows.Next() {
		var t TrafficTS
		if err := rows.Scan(&t.Day, &t.Visitors, &t.Sessions, &t.PageViews); err != nil {
			return nil, err
		}
		ts = append(ts, t)
	}

	// Top pages (from bronze for page_url detail)
	pageRows, err := pool.Query(ctx, `
		SELECT page_url, COUNT(*) AS views, COUNT(DISTINCT visitor_id) AS unique_visitors
		FROM bronze.events
		WHERE shop_id = $1 AND event_type = 'page_view'
			AND created_at >= $2 AND created_at < $3
			AND page_url IS NOT NULL
		GROUP BY page_url
		ORDER BY views DESC
		LIMIT 10
	`, req.ShopID, start, end)
	if err != nil {
		return nil, err
	}
	defer pageRows.Close()

	var topPages []TopPage
	for pageRows.Next() {
		var p TopPage
		if err := pageRows.Scan(&p.PageURL, &p.Views, &p.UniqueVisitors); err != nil {
			return nil, err
		}
		topPages = append(topPages, p)
	}

	// Top referrers (from bronze)
	refRows, err := pool.Query(ctx, `
		SELECT COALESCE(referrer, 'direct') AS ref, COUNT(DISTINCT session_id) AS sessions
		FROM bronze.events
		WHERE shop_id = $1 AND event_type IN ('session_start', 'page_view')
			AND created_at >= $2 AND created_at < $3
		GROUP BY COALESCE(referrer, 'direct')
		ORDER BY sessions DESC
		LIMIT 10
	`, req.ShopID, start, end)
	if err != nil {
		return nil, err
	}
	defer refRows.Close()

	var topReferrers []TopReferrer
	for refRows.Next() {
		var r TopReferrer
		if err := refRows.Scan(&r.Referrer, &r.Sessions); err != nil {
			return nil, err
		}
		topReferrers = append(topReferrers, r)
	}

	return TrafficResponse{
		Summary:      summary,
		TimeSeries:   ts,
		TopPages:     topPages,
		TopReferrers: topReferrers,
	}, nil
}
