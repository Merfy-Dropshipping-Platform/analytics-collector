package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ---- HTTP handler for loader.js (public, no auth) ----

type PixelHTTPResponse struct {
	PixelType string `json:"pixel_type"`
	PixelID   string `json:"pixel_id"`
	Inject    string `json:"inject"`
}

type PixelsHTTPHandler struct {
	pool *pgxpool.Pool
}

func NewPixelsHTTPHandler(pool *pgxpool.Pool) *PixelsHTTPHandler {
	return &PixelsHTTPHandler{pool: pool}
}

func (h *PixelsHTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	shopID := r.URL.Query().Get("shop_id")
	if shopID == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "shop_id is required"})
		return
	}

	rows, err := h.pool.Query(r.Context(), `
		SELECT pixel_type, pixel_id
		FROM config.tracking_pixels
		WHERE shop_id = $1 AND is_active = true
		ORDER BY created_at
	`, shopID)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	pixels := make([]PixelHTTPResponse, 0)
	for rows.Next() {
		var pType, pID string
		if err := rows.Scan(&pType, &pID); err != nil {
			continue
		}
		inject := generateInjectHTML(pType, pID)
		if inject != "" {
			pixels = append(pixels, PixelHTTPResponse{
				PixelType: pType,
				PixelID:   pID,
				Inject:    inject,
			})
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "public, max-age=300")
	json.NewEncoder(w).Encode(map[string]any{"pixels": pixels})
}

func generateInjectHTML(pixelType, pixelID string) string {
	switch pixelType {
	case "yandex_metrika":
		return fmt.Sprintf(`<script>(function(m,e,t,r,i,k,a){m[i]=m[i]||function(){(m[i].a=m[i].a||[]).push(arguments)};m[i].l=1*new Date();for(var j=0;j<document.scripts.length;j++){if(document.scripts[j].src===r){return}}k=e.createElement(t),a=e.getElementsByTagName(t)[0],k.async=1,k.src=r,a.parentNode.insertBefore(k,a)})(window,document,"script","https://mc.yandex.ru/metrika/tag.js","ym");ym(%s,"init",{clickmap:true,trackLinks:true,accurateTrackBounce:true,webvisor:true});</script><noscript><div><img src="https://mc.yandex.ru/watch/%s" style="position:absolute;left:-9999px" alt=""/></div></noscript>`, pixelID, pixelID)
	case "vk":
		return fmt.Sprintf(`<script>!function(){var t=document.createElement("script");t.type="text/javascript",t.async=!0,t.src="https://vk.com/js/api/openapi.js?169",t.onload=function(){VK.Retargeting.Init("%s"),VK.Retargeting.Hit()},document.head.appendChild(t)}();</script><noscript><img src="https://vk.com/rtrg?p=%s" style="position:fixed;left:-999px" alt=""/></noscript>`, pixelID, pixelID)
	case "meta":
		return fmt.Sprintf(`<script>!function(f,b,e,v,n,t,s){if(f.fbq)return;n=f.fbq=function(){n.callMethod?n.callMethod.apply(n,arguments):n.queue.push(arguments)};if(!f._fbq)f._fbq=n;n.push=n;n.loaded=!0;n.version='2.0';n.queue=[];t=b.createElement(e);t.async=!0;t.src=v;s=b.getElementsByTagName(e)[0];s.parentNode.insertBefore(t,s)}(window,document,'script','https://connect.facebook.net/en_US/fbevents.js');fbq('init','%s');fbq('track','PageView');</script><noscript><img height="1" width="1" style="display:none" src="https://www.facebook.com/tr?id=%s&ev=PageView&noscript=1"/></noscript>`, pixelID, pixelID)
	case "google_analytics":
		return fmt.Sprintf(`<script async src="https://www.googletagmanager.com/gtag/js?id=%s"></script><script>window.dataLayer=window.dataLayer||[];function gtag(){dataLayer.push(arguments)}gtag('js',new Date());gtag('config','%s');</script>`, pixelID, pixelID)
	case "custom":
		return pixelID // custom pixel_id IS the script code itself
	default:
		return ""
	}
}

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
