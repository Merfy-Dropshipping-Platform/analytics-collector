package handler

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

type FunnelRequest struct {
	ShopID   string `json:"shopId"`
	TenantID string `json:"tenantId"`
	Period   string `json:"period"`
}

type FunnelStage struct {
	Name  string  `json:"name"`
	Label string  `json:"label"`
	Count int64   `json:"count"`
	Rate  float64 `json:"rate"`
}

type FunnelResponse struct {
	Stages []FunnelStage `json:"stages"`
}

func HandleFunnel(ctx context.Context, pool *pgxpool.Pool, payload json.RawMessage) (any, error) {
	var req FunnelRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, fmt.Errorf("invalid request: %w", err)
	}
	if req.ShopID == "" || req.Period == "" {
		return nil, fmt.Errorf("shopId and period are required")
	}

	start, end := periodRange(req.Period, timeNow())

	var visits, productViews, addToCart, checkoutStarts, purchases int64
	err := pool.QueryRow(ctx, `
		SELECT
			COALESCE(SUM(visits), 0),
			COALESCE(SUM(product_views), 0),
			COALESCE(SUM(add_to_cart), 0),
			COALESCE(SUM(checkout_starts), 0),
			COALESCE(SUM(purchases), 0)
		FROM silver.daily_funnel
		WHERE shop_id = $1 AND day >= $2 AND day < $3
	`, req.ShopID, start, end).Scan(&visits, &productViews, &addToCart, &checkoutStarts, &purchases)
	if err != nil {
		return nil, err
	}

	stages := []FunnelStage{
		{Name: "visits", Label: "Визиты", Count: visits, Rate: 100.0},
		{Name: "product_views", Label: "Просмотр товара", Count: productViews, Rate: safeRate(productViews, visits)},
		{Name: "add_to_cart", Label: "Корзина", Count: addToCart, Rate: safeRate(addToCart, productViews)},
		{Name: "checkout_starts", Label: "Оформление", Count: checkoutStarts, Rate: safeRate(checkoutStarts, addToCart)},
		{Name: "purchases", Label: "Покупка", Count: purchases, Rate: safeRate(purchases, checkoutStarts)},
	}

	return FunnelResponse{Stages: stages}, nil
}

func safeRate(current, previous int64) float64 {
	if previous == 0 {
		if current > 0 {
			return 100.0
		}
		return 0
	}
	rate := float64(current) / float64(previous) * 100
	if rate > 100 {
		rate = 100
	}
	return rate
}
