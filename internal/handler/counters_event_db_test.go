//go:build dbtest

package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5"
)

// Магазин правки и удаления счётчика — из строки config.tracking_pixels своей организации (план блока 11, задача 4).
func TestFindPixelShopByRow(t *testing.T) {
	if testPool == nil {
		t.Skip("GEO_TEST_DB не задан: тест с базой пропущен")
	}
	ctx := context.Background()
	var id string
	err := testPool.QueryRow(ctx, `INSERT INTO config.tracking_pixels (shop_id, tenant_id, pixel_type, pixel_id)
		VALUES ('site-db-11', 'org-db-11', 'vk', 'VK-RTRG-1-a') RETURNING id::text`).Scan(&id)
	if err != nil {
		t.Fatalf("строка счётчика: %v", err)
	}
	own := json.RawMessage(fmt.Sprintf(`{"id":%q,"tenantId":"org-db-11"}`, id))
	if shop, err := FindPixelShop(ctx, testPool, own); err != nil || shop != "site-db-11" {
		t.Fatalf("магазин своей организации: %q, %v", shop, err)
	}
	other := json.RawMessage(fmt.Sprintf(`{"id":%q,"tenantId":"org-other"}`, id))
	if _, err := FindPixelShop(ctx, testPool, other); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("чужая организация: ждали pgx.ErrNoRows, пришло %v", err)
	}
}
