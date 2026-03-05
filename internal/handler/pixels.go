package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Pixel struct {
	ID        string    `json:"id"`
	PixelType string    `json:"pixel_type"`
	PixelID   string    `json:"pixel_id"`
	Name      string    `json:"name"`
	IsActive  bool      `json:"is_active"`
	CreatedAt time.Time `json:"created_at"`
}

type PixelListRequest struct {
	ShopID   string `json:"shopId"`
	TenantID string `json:"tenantId"`
}

type PixelCreateRequest struct {
	ShopID    string `json:"shopId"`
	TenantID  string `json:"tenantId"`
	PixelType string `json:"pixel_type"`
	PixelID   string `json:"pixel_id"`
	Name      string `json:"name"`
}

type PixelUpdateRequest struct {
	ID       string `json:"id"`
	TenantID string `json:"tenantId"`
	IsActive *bool  `json:"is_active,omitempty"`
	Name     *string `json:"name,omitempty"`
}

type PixelDeleteRequest struct {
	ID       string `json:"id"`
	TenantID string `json:"tenantId"`
}

func HandlePixelsList(ctx context.Context, pool *pgxpool.Pool, payload json.RawMessage) (any, error) {
	var req PixelListRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, fmt.Errorf("invalid request: %w", err)
	}

	rows, err := pool.Query(ctx, `
		SELECT id, pixel_type, pixel_id, COALESCE(name, ''), is_active, created_at
		FROM config.tracking_pixels
		WHERE shop_id = $1 AND tenant_id = $2
		ORDER BY created_at DESC
	`, req.ShopID, req.TenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	pixels := make([]Pixel, 0)
	for rows.Next() {
		var p Pixel
		if err := rows.Scan(&p.ID, &p.PixelType, &p.PixelID, &p.Name, &p.IsActive, &p.CreatedAt); err != nil {
			return nil, err
		}
		pixels = append(pixels, p)
	}

	return map[string]any{"pixels": pixels}, nil
}

func HandlePixelsCreate(ctx context.Context, pool *pgxpool.Pool, payload json.RawMessage) (any, error) {
	var req PixelCreateRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, fmt.Errorf("invalid request: %w", err)
	}
	if req.ShopID == "" || req.TenantID == "" || req.PixelType == "" || req.PixelID == "" {
		return nil, fmt.Errorf("shopId, tenantId, pixel_type, pixel_id are required")
	}

	var pixel Pixel
	err := pool.QueryRow(ctx, `
		INSERT INTO config.tracking_pixels (shop_id, tenant_id, pixel_type, pixel_id, name)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING id, pixel_type, pixel_id, COALESCE(name, ''), is_active, created_at
	`, req.ShopID, req.TenantID, req.PixelType, req.PixelID, req.Name).Scan(
		&pixel.ID, &pixel.PixelType, &pixel.PixelID, &pixel.Name, &pixel.IsActive, &pixel.CreatedAt,
	)
	if err != nil {
		return nil, err
	}

	return pixel, nil
}

func HandlePixelsUpdate(ctx context.Context, pool *pgxpool.Pool, payload json.RawMessage) (any, error) {
	var req PixelUpdateRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, fmt.Errorf("invalid request: %w", err)
	}
	if req.ID == "" || req.TenantID == "" {
		return nil, fmt.Errorf("id and tenantId are required")
	}

	// Build dynamic update
	if req.IsActive != nil {
		_, err := pool.Exec(ctx,
			"UPDATE config.tracking_pixels SET is_active = $1, updated_at = now() WHERE id = $2 AND tenant_id = $3",
			*req.IsActive, req.ID, req.TenantID)
		if err != nil {
			return nil, err
		}
	}
	if req.Name != nil {
		_, err := pool.Exec(ctx,
			"UPDATE config.tracking_pixels SET name = $1, updated_at = now() WHERE id = $2 AND tenant_id = $3",
			*req.Name, req.ID, req.TenantID)
		if err != nil {
			return nil, err
		}
	}

	var pixel Pixel
	err := pool.QueryRow(ctx, `
		SELECT id, pixel_type, pixel_id, COALESCE(name, ''), is_active, created_at
		FROM config.tracking_pixels WHERE id = $1 AND tenant_id = $2
	`, req.ID, req.TenantID).Scan(
		&pixel.ID, &pixel.PixelType, &pixel.PixelID, &pixel.Name, &pixel.IsActive, &pixel.CreatedAt,
	)
	if err != nil {
		return nil, err
	}

	return pixel, nil
}

func HandlePixelsDelete(ctx context.Context, pool *pgxpool.Pool, payload json.RawMessage) (any, error) {
	var req PixelDeleteRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, fmt.Errorf("invalid request: %w", err)
	}
	if req.ID == "" || req.TenantID == "" {
		return nil, fmt.Errorf("id and tenantId are required")
	}

	_, err := pool.Exec(ctx,
		"DELETE FROM config.tracking_pixels WHERE id = $1 AND tenant_id = $2",
		req.ID, req.TenantID)
	if err != nil {
		return nil, err
	}

	return map[string]bool{"success": true}, nil
}
