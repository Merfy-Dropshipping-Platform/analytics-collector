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

	// Total visitors = unique visitors (same as "Посещаемость" on dashboard).
	// Правило 1: посетители и визиты шагов — уникальные за весь период (period_uniques.go).
	c, err := periodFunnel(ctx, pool, req.ShopID, start, end)
	if err != nil {
		return nil, err
	}
	totalSessions, productViews, addToCart, checkoutStarts, purchases :=
		c.visitors, c.productViews, c.addToCart, c.checkoutStarts, c.purchases

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
