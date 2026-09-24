//go:build dbtest

package consumer

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Пишет 4 события с разными traffic_type через реальный insertBatch и сверяет,
// что в bronze.events (миграция 016, CHECK IN ('human','bot','internal')) легло
// именно то, что нужно: bot и internal — как есть, пустая строка и мусорное
// значение — обе схлопнулись в 'human' (normalizeTrafficType). Если бы вторая
// линия обороны не сработала, INSERT одной пачки на 4 строки упал бы ЦЕЛИКОМ
// на CHECK-констрейнте — тест это тоже покрывает: успешный insertBatch уже
// доказывает, что ни одна строка не нарушила ограничение.
// Gated by build tag `dbtest` + GEO_TEST_DB.
func TestInsertBatchTrafficType_DB(t *testing.T) {
	dsn := os.Getenv("GEO_TEST_DB")
	if dsn == "" {
		t.Skip("GEO_TEST_DB not set")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer pool.Close()

	// Идемпотентно между прогонами: чистим свой магазин перед вставкой.
	if _, err := pool.Exec(ctx, `DELETE FROM bronze.events WHERE shop_id='shopTT'`); err != nil {
		t.Fatalf("cleanup: %v", err)
	}

	bw := &BronzeWriter{pool: pool}
	events := []Event{
		{ShopID: "shopTT", Type: "page_view", SessionID: "tt-bot", TrafficType: "bot", Timestamp: "2026-07-14T10:00:00Z"},
		{ShopID: "shopTT", Type: "page_view", SessionID: "tt-internal", TrafficType: "internal", Timestamp: "2026-07-14T10:00:01Z"},
		{ShopID: "shopTT", Type: "page_view", SessionID: "tt-empty", TrafficType: "", Timestamp: "2026-07-14T10:00:02Z"},
		{ShopID: "shopTT", Type: "page_view", SessionID: "tt-garbage", TrafficType: "zzz", Timestamp: "2026-07-14T10:00:03Z"},
	}

	if err := bw.insertBatch(ctx, events); err != nil {
		t.Fatalf("insertBatch (traffic_type): %v", err)
	}

	var botN, internalN, humanN, totalN int
	if err := pool.QueryRow(ctx, `
		SELECT
		  count(*) FILTER (WHERE traffic_type = 'bot'),
		  count(*) FILTER (WHERE traffic_type = 'internal'),
		  count(*) FILTER (WHERE traffic_type = 'human'),
		  count(*)
		FROM bronze.events WHERE shop_id = 'shopTT'
	`).Scan(&botN, &internalN, &humanN, &totalN); err != nil {
		t.Fatalf("verify select: %v", err)
	}

	if totalN != 4 {
		t.Fatalf("total rows = %d; want 4 (весь батч должен был пройти CHECK)", totalN)
	}
	if botN != 1 {
		t.Errorf("bot rows = %d; want 1", botN)
	}
	if internalN != 1 {
		t.Errorf("internal rows = %d; want 1", internalN)
	}
	if humanN != 2 {
		t.Errorf("human rows = %d; want 2 (пустая строка и мусорное значение свёрнуты в human)", humanN)
	}
}
