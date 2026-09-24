//go:build dbtest

package handler

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/merfy/analytics-collector/internal/db"
)

// ev — одно событие для bronze.events. Пустые поля пишутся как NULL; пустой traffic — 'human';
// нулевой created — равен at (событие записано сразу).
type ev struct {
	shop, session, visitor, typ string
	at, created                 time.Time
	traffic                     string
	order                       string
	total                       int64
	product                     string
	price                       int64
	page                        string
	utm                         string
	country, subject, city      string
}

// today — полночь UTC сегодняшнего дня: сутки отчётов — по UTC, как в resolveRange.
func today() time.Time {
	return time.Now().UTC().Truncate(24 * time.Hour)
}

// testNow — фиксированное «сейчас» тестов: сегодня 12:00 UTC. Берётся от настоящих часов,
// чтобы данные всегда попадали в окна представлений (они считаются от now() базы).
func testNow() time.Time {
	return today().Add(12 * time.Hour)
}

// utc — момент dayOffset дней от сегодняшнего, в hh:mm UTC.
func utc(dayOffset, hh, mm int) time.Time {
	return today().AddDate(0, 0, dayOffset).Add(time.Duration(hh)*time.Hour + time.Duration(mm)*time.Minute)
}

// dayOf — дата по UTC, как её отдают графики по дням.
func dayOf(t time.Time) string {
	return t.UTC().Format(time.DateOnly)
}

// setClock фиксирует «сейчас» обработчиков на время теста.
func setClock(t *testing.T, now time.Time) {
	t.Helper()
	prev := clock
	clock = func() time.Time { return now }
	t.Cleanup(func() { clock = prev })
}

// seed очищает bronze.events, пишет события и обновляет представления тем же путём,
// что и цикл обслуживания (REFRESH … CONCURRENTLY) — ошибка обновления роняет тест.
func seed(t *testing.T, events ...ev) {
	t.Helper()
	pool := needDB(t)
	ctx := context.Background()

	if _, err := pool.Exec(ctx, "TRUNCATE bronze.events"); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	for _, e := range events {
		created := e.created
		if created.IsZero() {
			created = e.at
		}
		traffic := e.traffic
		if traffic == "" {
			traffic = "human"
		}
		_, err := pool.Exec(ctx, `
			INSERT INTO bronze.events (
				shop_id, session_id, visitor_id, event_type, event_timestamp, created_at, traffic_type,
				order_id, order_total_cents, product_id, product_name, product_price_cents,
				page_url, utm_source, geo_country, geo_subject, geo_city
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)`,
			e.shop, e.session, nz(e.visitor), e.typ, e.at, created, traffic,
			nz(e.order), nzi(e.total), nz(e.product), nz(e.product), nzi(e.price),
			nz(e.page), nz(e.utm), nz(e.country), nz(e.subject), nz(e.city),
		)
		if err != nil {
			t.Fatalf("insert %+v: %v", e, err)
		}
	}
	if err := db.RefreshMatviews(ctx, pool); err != nil {
		t.Fatalf("refresh matviews: %v", err)
	}
}

func nz(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func nzi(n int64) *int64 {
	if n == 0 {
		return nil
	}
	return &n
}

// call вызывает RPC-обработчик так же, как RPC-сервер: JSON на входе, ответ — значение.
func call[T any](t *testing.T, h func(context.Context, *pgxpool.Pool, json.RawMessage) (any, error), payload string) T {
	t.Helper()
	out, err := h(context.Background(), needDB(t), json.RawMessage(payload))
	if err != nil {
		t.Fatalf("handler(%s): %v", payload, err)
	}
	res, ok := out.(T)
	if !ok {
		t.Fatalf("handler(%s): response type %T", payload, out)
	}
	return res
}

// pageView — просмотр страницы человеком (самое частое событие фикстур).
func pageView(shop, session, visitor string, at time.Time) ev {
	return ev{shop: shop, session: session, visitor: visitor, typ: "page_view", at: at, page: "/"}
}
