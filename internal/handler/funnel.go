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
	From     string `json:"from,omitempty"`
	To       string `json:"to,omitempty"`
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

	start, end := resolveRange(req.Period, req.From, req.To, timeNow())

	// Total visits = all unique sessions (same as "Посещаемость" on dashboard)
	var totalSessions int64
	err := pool.QueryRow(ctx, `
		SELECT COALESCE(SUM(unique_sessions), 0)
		FROM silver.daily_traffic
		WHERE shop_id = $1 AND day >= $2::date AND day < $3::date
	`, req.ShopID, start, end).Scan(&totalSessions)
	if err != nil {
		return nil, err
	}

	// Funnel stages from daily_funnel
	var productViews, addToCart, checkoutStarts, purchases int64
	err = pool.QueryRow(ctx, `
		SELECT
			COALESCE(SUM(product_views), 0),
			COALESCE(SUM(add_to_cart), 0),
			COALESCE(SUM(checkout_starts), 0),
			COALESCE(SUM(purchases), 0)
		FROM silver.daily_funnel
		WHERE shop_id = $1 AND day >= $2::date AND day < $3::date
	`, req.ShopID, start, end).Scan(&productViews, &addToCart, &checkoutStarts, &purchases)
	if err != nil {
		return nil, err
	}

	stages := []FunnelStage{
		{Name: "visits", Label: "Визиты", Count: totalSessions, Rate: 100.0},
		{Name: "product_views", Label: "Просмотр товара", Count: productViews, Rate: safeRate(productViews, totalSessions)},
		{Name: "add_to_cart", Label: "Добавлено в корзину", Count: addToCart, Rate: safeRate(addToCart, totalSessions)},
		{Name: "checkout_starts", Label: "Готов к оплате", Count: checkoutStarts, Rate: safeRate(checkoutStarts, totalSessions)},
		{Name: "purchases", Label: "Оплаченные заказы", Count: purchases, Rate: safeRate(purchases, totalSessions)},
	}

	return FunnelResponse{Stages: stages}, nil
}

func safeRate(current, total int64) float64 {
	if total == 0 {
		return 0
	}
	rate := float64(current) / float64(total) * 100
	if rate > 100 {
		return 100
	}
	return rate
}
