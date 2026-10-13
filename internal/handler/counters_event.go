package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Событие пересборки «счётчики мерчанта изменились» (merfy-mcp/docs/plans/storefront-shell/11-analytics/plan.md,
// задача 4; design.md блока 11, С11-4 Б). Номера счётчиков вписывает в конфиг магазина новой темы его сборка, поэтому
// после успешного создания, правки или удаления счётчика магазин пересобирается. Тело — договор сборщика витрин site-gen
// (packages/storefront-builder/src/events.ts): { v: 1, type, siteId, eventAt, source }, ключ маршрута — тип. Магазин
// нынешней темы событие не задевает: сборщик витрин пропускает магазины не новой архитектуры. Не ушло — строка в
// журнале, ответ кабинету тот же: магазин подхватит счётчики со следующей сборкой.

// CountersChangeEvent — id события в rebuild-events.json site-gen.
const CountersChangeEvent = "analytics-counters-change"

const countersEventSource = "analytics-collector"
const countersEventTimeout = 4 * time.Second

// Время события — UTC с миллисекундами, как у сборщика витрин.
const eventTimeLayout = "2006-01-02T15:04:05.000Z07:00"

type contentEventPublisher interface {
	PublishContentEvent(ctx context.Context, routingKey string, body []byte) error
}

// rpcHandler — та же форма, что rpc.Handler: обёртка встаёт в rpcSrv.Register без приведения типов.
type rpcHandler = func(ctx context.Context, pool *pgxpool.Pool, payload json.RawMessage) (any, error)

// PixelShopFinder — магазин, чьи счётчики меняет запрос.
type PixelShopFinder = func(ctx context.Context, pool *pgxpool.Pool, payload json.RawMessage) (string, error)

type contentEvent struct {
	V       int    `json:"v"`
	Type    string `json:"type"`
	SiteID  string `json:"siteId"`
	EventAt string `json:"eventAt"`
	Source  string `json:"source"`
}

type pixelRef struct {
	ShopID   string `json:"shopId"`
	ID       string `json:"id"`
	TenantID string `json:"tenantId"`
}

// FindPixelShop: у создания магазин — в запросе, у правки и удаления — в строке счётчика своей организации.
func FindPixelShop(ctx context.Context, pool *pgxpool.Pool, payload json.RawMessage) (string, error) {
	var ref pixelRef
	if err := json.Unmarshal(payload, &ref); err != nil {
		return "", fmt.Errorf("invalid request: %w", err)
	}
	if ref.ShopID != "" {
		return ref.ShopID, nil
	}
	var shopID string
	err := pool.QueryRow(ctx,
		"SELECT shop_id FROM config.tracking_pixels WHERE id = $1 AND tenant_id = $2",
		ref.ID, ref.TenantID).Scan(&shopID)
	return shopID, err
}

// WithCountersEvent оборачивает RPC правки счётчика. Магазин узнаётся до обработчика — у удалённого счётчика строки
// после уже нет; событие уходит только после успеха.
func WithCountersEvent(pub contentEventPublisher, findShop PixelShopFinder, now func() time.Time, next rpcHandler) rpcHandler {
	return func(ctx context.Context, pool *pgxpool.Pool, payload json.RawMessage) (any, error) {
		shopID, findErr := findShop(ctx, pool, payload)
		result, err := next(ctx, pool, payload)
		if err != nil {
			return nil, err
		}
		if findErr != nil || shopID == "" {
			slog.Error("counters event: shop not found", "error", findErr)
			return result, nil
		}
		publishCountersChange(ctx, pub, shopID, now())
		return result, nil
	}
}

func publishCountersChange(ctx context.Context, pub contentEventPublisher, shopID string, at time.Time) {
	event := contentEvent{
		V:       1,
		Type:    CountersChangeEvent,
		SiteID:  shopID,
		EventAt: at.UTC().Format(eventTimeLayout),
		Source:  countersEventSource,
	}
	body, err := json.Marshal(event)
	if err != nil {
		slog.Error("counters event: marshal", "error", err)
		return
	}
	ctx, cancel := context.WithTimeout(ctx, countersEventTimeout)
	defer cancel()
	if err := pub.PublishContentEvent(ctx, CountersChangeEvent, body); err != nil {
		slog.Error("counters event: publish failed", "shop_id", shopID, "error", err)
	}
}
