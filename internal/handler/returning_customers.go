package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"math"

	"github.com/jackc/pgx/v5/pgxpool"
)

type ReturningCustomersRequest struct {
	ShopID   string `json:"shopId"`
	TenantID string `json:"tenantId"`
	Period   string `json:"period"`
}

type ReturningCustomersResponse struct {
	TotalBuyers   int64   `json:"total_buyers"`
	RepeatBuyers  int64   `json:"repeat_buyers"`
	RepeatRate    float64 `json:"repeat_rate"`
}

func HandleReturningCustomers(ctx context.Context, pool *pgxpool.Pool, payload json.RawMessage) (any, error) {
	var req ReturningCustomersRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, fmt.Errorf("invalid request: %w", err)
	}
	if req.ShopID == "" || req.Period == "" {
		return nil, fmt.Errorf("shopId and period are required")
	}

	start, end := periodRange(req.Period, timeNow())

	var totalBuyers, repeatBuyers int64
	err := pool.QueryRow(ctx, `
		WITH buyer_orders AS (
			SELECT visitor_id, COUNT(DISTINCT order_id) AS order_count
			FROM bronze.events
			WHERE shop_id = $1
			  AND event_type = 'purchase'
			  AND visitor_id IS NOT NULL
			  AND order_id IS NOT NULL
			  AND created_at >= $2 AND created_at < $3
			GROUP BY visitor_id
		)
		SELECT
			COUNT(*) AS total_buyers,
			COUNT(*) FILTER (WHERE order_count > 1) AS repeat_buyers
		FROM buyer_orders
	`, req.ShopID, start, end).Scan(&totalBuyers, &repeatBuyers)
	if err != nil {
		return nil, err
	}

	var repeatRate float64
	if totalBuyers > 0 {
		repeatRate = math.Round(float64(repeatBuyers)/float64(totalBuyers)*10000) / 100
	}

	return ReturningCustomersResponse{
		TotalBuyers:  totalBuyers,
		RepeatBuyers: repeatBuyers,
		RepeatRate:   repeatRate,
	}, nil
}
