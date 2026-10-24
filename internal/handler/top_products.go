package handler

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

type TopProductsRequest struct {
	ShopID   string `json:"shopId"`
	TenantID string `json:"tenantId"`
	Period   string `json:"period"`
	From     string `json:"from,omitempty"`
	To       string `json:"to,omitempty"`
	Sort     string `json:"sort"`
	Limit    int    `json:"limit"`
}

type ProductEntry struct {
	ProductID        string `json:"product_id"`
	Name             string `json:"product_name"`
	SalesCount       int64  `json:"total_sold"`
	TotalRevenueCents int64 `json:"total_revenue_cents"`
}

type TopProductsResponse struct {
	Products []ProductEntry `json:"products"`
}

func HandleTopProducts(ctx context.Context, pool *pgxpool.Pool, payload json.RawMessage) (any, error) {
	var req TopProductsRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, fmt.Errorf("invalid request: %w", err)
	}
	if req.ShopID == "" {
		return nil, fmt.Errorf("shopId is required")
	}

	limit := req.Limit
	if limit <= 0 || limit > 100 {
		limit = 10
	}

	orderBy := "sales_count DESC"
	if req.Sort == "revenue" {
		orderBy = "total_revenue_cents DESC"
	}

	query := fmt.Sprintf(`
		SELECT product_id, product_name, sales_count, total_revenue_cents
		FROM gold.top_products
		WHERE shop_id = $1
		ORDER BY %s
		LIMIT $2
	`, orderBy)

	rows, err := pool.Query(ctx, query, req.ShopID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var products []ProductEntry
	for rows.Next() {
		var p ProductEntry
		if err := rows.Scan(&p.ProductID, &p.Name, &p.SalesCount, &p.TotalRevenueCents); err != nil {
			return nil, err
		}
		products = append(products, p)
	}

	return TopProductsResponse{Products: products}, nil
}
