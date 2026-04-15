package handler

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

type RevenueRequest struct {
	ShopID   string `json:"shopId"`
	TenantID string `json:"tenantId"`
	Period   string `json:"period"`
	From     string `json:"from,omitempty"`
	To       string `json:"to,omitempty"`
}

type RevenueSummary struct {
	TotalRevenueCents int64 `json:"total_revenue_cents"`
	TotalOrders       int64 `json:"total_orders"`
	AvgOrderCents     int64 `json:"avg_order_cents"`
	RefundsCents      int64 `json:"refunds_cents"`
	NetRevenueCents   int64 `json:"net_revenue_cents"`
}

type RevenueTS struct {
	Day          string `json:"day"`
	RevenueCents int64  `json:"revenue_cents"`
	Orders       int64  `json:"orders"`
	AvgCents     int64  `json:"avg_cents"`
}

type RevenueResponse struct {
	Summary    RevenueSummary `json:"summary"`
	TimeSeries []RevenueTS   `json:"time_series"`
}

func HandleRevenue(ctx context.Context, pool *pgxpool.Pool, payload json.RawMessage) (any, error) {
	var req RevenueRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, fmt.Errorf("invalid request: %w", err)
	}
	if req.ShopID == "" || req.Period == "" {
		return nil, fmt.Errorf("shopId and period are required")
	}

	start, end := resolveRange(req.Period, req.From, req.To, timeNow())

	// Summary
	var summary RevenueSummary
	err := pool.QueryRow(ctx, `
		SELECT
			COALESCE(SUM(total_revenue_cents), 0),
			COALESCE(SUM(order_count), 0),
			CASE WHEN SUM(order_count) > 0
				THEN (SUM(total_revenue_cents) / SUM(order_count))::bigint
				ELSE 0 END
		FROM silver.daily_orders
		WHERE shop_id = $1 AND day >= $2 AND day < $3
	`, req.ShopID, start, end).Scan(
		&summary.TotalRevenueCents, &summary.TotalOrders, &summary.AvgOrderCents,
	)
	if err != nil {
		return nil, err
	}
	summary.NetRevenueCents = summary.TotalRevenueCents - summary.RefundsCents

	// Time series
	rows, err := pool.Query(ctx, `
		SELECT day::text, total_revenue_cents, order_count, avg_order_cents
		FROM silver.daily_orders
		WHERE shop_id = $1 AND day >= $2 AND day < $3
		ORDER BY day
	`, req.ShopID, start, end)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ts []RevenueTS
	for rows.Next() {
		var t RevenueTS
		if err := rows.Scan(&t.Day, &t.RevenueCents, &t.Orders, &t.AvgCents); err != nil {
			return nil, err
		}
		ts = append(ts, t)
	}

	return RevenueResponse{
		Summary:    summary,
		TimeSeries: ts,
	}, nil
}
