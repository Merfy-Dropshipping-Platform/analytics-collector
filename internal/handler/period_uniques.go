package handler

// Правило 1 (spec 114): итоги трафика за период — уникальные за весь период, а не сумма
// суточных уников. Посетитель, заходивший три дня подряд, за «30 дней» — один посетитель,
// а не три; визит через полночь — один визит, а не два.
//
// Считается прямо по bronze.events теми же правилами, что и суточные представления миграции 017
// (трафик — только люди, деньги — все, окно — по времени события); меняется только то, что уники
// берутся за всё окно разом. Графики по дням остаются по дням.
//
// Отдельный файл нарочно: владелец ещё решает про правило 1. Выкинуть его = удалить этот файл
// (и period_uniques_db_test.go) и вызовы period* в обработчиках — одним откатом.
//
// Во всех запросах: $1 — начало окна, $2 — конец, $3 — магазин (NULL — вся платформа).

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// shopScope — значение $3: магазин или NULL (вся платформа).
func shopScope(shopID string) any {
	if shopID == "" {
		return nil
	}
	return shopID
}

// inShop — событие магазина $3 (или любого, если $3 — NULL).
const inShop = "($3::text IS NULL OR e.shop_id = $3)"

// periodTrafficSQL — трафик людей за окно; определения — как у silver.daily_traffic.
var periodTrafficSQL = `
	SELECT
		COUNT(DISTINCT e.visitor_id),
		COUNT(DISTINCT e.session_id) FILTER (WHERE e.event_type IN ('page_view', 'session_start')),
		COUNT(*) FILTER (WHERE e.event_type = 'page_view')
	FROM bronze.events e
	WHERE ` + inShop + ` AND ` + humanOnly("e") + ` AND ` + eventWindow("e", "$1", "$2")

type periodTraffic struct {
	visitors, sessions, pageViews int64
}

func queryPeriodTraffic(ctx context.Context, pool *pgxpool.Pool, shopID string, start, end time.Time) (periodTraffic, error) {
	var t periodTraffic
	err := pool.QueryRow(ctx, periodTrafficSQL, start, end, shopScope(shopID)).
		Scan(&t.visitors, &t.sessions, &t.pageViews)
	return t, err
}

// periodConversionSQL — конверсия по прежней формуле (queryKPI): заказы только тех дней, где были
// посетители (защита от «сирот»), ÷ посетители × 100, не больше 100 %. Делитель ($4) — посетители
// за весь период: то самое число, что стоит на экране рядом.
const periodConversionSQL = `
	SELECT CASE WHEN $4::bigint > 0
		THEN LEAST(ROUND(
			COALESCE(SUM(order_count) FILTER (WHERE unique_visitors > 0), 0)::numeric
			/ $4::bigint * 100, 2), 100)
		ELSE 0 END
	FROM gold.dashboard_kpi
	WHERE ($3::text IS NULL OR shop_id = $3) AND day >= $1::date AND day < $2::date`

// periodKPI — KPI дашборда с трафиком, уникальным за весь период; деньги — как посчитал queryKPI.
func periodKPI(ctx context.Context, pool *pgxpool.Pool, shopID string, start, end time.Time, kpi DashboardKPI) (DashboardKPI, error) {
	t, err := queryPeriodTraffic(ctx, pool, shopID, start, end)
	if err != nil {
		return kpi, err
	}
	kpi.UniqueVisitors, kpi.UniqueSessions, kpi.PageViews = t.visitors, t.sessions, t.pageViews
	err = pool.QueryRow(ctx, periodConversionSQL, start, end, shopScope(shopID), t.visitors).Scan(&kpi.ConversionRate)
	return kpi, err
}

// periodFunnelSQL — шаги воронки за окно, визиты каждого шага — уникальные за всё окно.
// Шаг накопительный (владелец 28.09): визит считается на каждом шаге до самого дальнего, до
// которого дошёл, — оплативший визит есть и в «Добавлено в корзину», и в «Готов к оплате»,
// даже если витрина не прислала события этих шагов (например, «Купить сейчас»). Шаги до
// оплаты — только визиты людей (есть хоть одно событие человека), оплаты — все, как деньги.
// Определения — как у silver.daily_funnel: шаги визита — только люди, покупки — все (это заказы).
var periodFunnelSQL = `
	WITH visit AS (
		SELECT e.session_id,
			BOOL_OR(` + humanOnly("e") + `) AS human,
			MAX(CASE e.event_type
				WHEN 'product_view' THEN 1
				WHEN 'add_to_cart' THEN 2
				WHEN 'checkout_start' THEN 3
				WHEN 'purchase' THEN 4
				ELSE 0
			END) AS step,
			BOOL_OR(e.event_type = 'purchase') AS purchased
		FROM bronze.events e
		WHERE ` + inShop + `
		  AND e.session_id IS NOT NULL
		  AND ` + eventWindow("e", "$1", "$2") + `
		GROUP BY e.session_id
	)
	SELECT
		COUNT(*) FILTER (WHERE human AND step >= 1),
		COUNT(*) FILTER (WHERE human AND step >= 2),
		COUNT(*) FILTER (WHERE human AND step >= 3),
		COUNT(*) FILTER (WHERE purchased)
	FROM visit`

// funnelCounts — числа для шагов воронки: первый шаг — посетители (как «Посещаемость»).
type funnelCounts struct {
	visitors, productViews, addToCart, checkoutStarts, purchases int64
}

func periodFunnel(ctx context.Context, pool *pgxpool.Pool, shopID string, start, end time.Time) (funnelCounts, error) {
	t, err := queryPeriodTraffic(ctx, pool, shopID, start, end)
	if err != nil {
		return funnelCounts{}, err
	}
	c := funnelCounts{visitors: t.visitors}
	err = pool.QueryRow(ctx, periodFunnelSQL, start, end, shopScope(shopID)).
		Scan(&c.productViews, &c.addToCart, &c.checkoutStarts, &c.purchases)
	return c, err
}

// periodVisitorsByShop — посетители каждого магазина, уникальные за окно (лидерборд платформы).
var periodVisitorsByShopSQL = `
	SELECT e.shop_id, COUNT(DISTINCT e.visitor_id)
	FROM bronze.events e
	WHERE ` + humanOnly("e") + ` AND ` + eventWindow("e", "$1", "$2") + `
	GROUP BY e.shop_id`

func periodVisitorsByShop(ctx context.Context, pool *pgxpool.Pool, start, end time.Time) (map[string]int64, error) {
	rows, err := pool.Query(ctx, periodVisitorsByShopSQL, start, end)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	visitors := map[string]int64{}
	for rows.Next() {
		var shop string
		var n int64
		if err := rows.Scan(&shop, &n); err != nil {
			return nil, err
		}
		visitors[shop] = n
	}
	return visitors, rows.Err()
}

// periodLocationSQL — «Сессии по локациям» за окно: визит считается один раз за всё окно.
// Устройство — как у silver.daily_geo (015/017): гео визита — первое его событие людей с
// известным гео; покупка и отмена — в гео своего визита, без визита человека — «Не определено»;
// дубли событий заказа не удваиваются, отмена без покупки не вычитается (014).
// %[1]s — колонки уровня (страна / субъект / город), %[2]s — группировка по ним.
const periodLocationSQL = `
	WITH session_geo AS (
		SELECT DISTINCT ON (e.shop_id, e.session_id)
			e.shop_id, e.session_id, e.geo_country, e.geo_subject, e.geo_city
		FROM bronze.events e
		WHERE %[3]s AND e.event_type IN ('page_view', 'session_start')
		  AND %[4]s AND %[5]s
		ORDER BY e.shop_id, e.session_id, (e.geo_country IS NULL), e.event_timestamp
	),
	order_events AS (
		SELECT e.shop_id, e.order_id, e.event_type, MAX(e.session_id) AS session_id
		FROM bronze.events e
		WHERE %[3]s AND e.event_type IN ('purchase', 'order_cancel') AND e.order_id IS NOT NULL
		  AND %[5]s
		  AND (e.event_type = 'purchase' OR EXISTS (
		        SELECT 1 FROM bronze.events p
		        WHERE p.event_type = 'purchase' AND p.shop_id = e.shop_id AND p.order_id = e.order_id))
		GROUP BY e.shop_id, e.order_id, e.event_type
	),
	geo_rows AS (
		SELECT geo_country, geo_subject, geo_city, 1 AS sessions, 0 AS orders
		FROM session_geo
		UNION ALL
		SELECT sg.geo_country, sg.geo_subject, sg.geo_city, 0,
		       CASE WHEN oe.event_type = 'purchase' THEN 1 ELSE -1 END
		FROM order_events oe
		LEFT JOIN session_geo sg ON sg.shop_id = oe.shop_id AND sg.session_id = oe.session_id
	)
	SELECT %[1]s, SUM(sessions)::bigint, SUM(orders)::bigint, (SUM(SUM(sessions)) OVER ())::bigint
	FROM geo_rows
	GROUP BY %[2]s
	ORDER BY SUM(sessions) DESC, SUM(orders) DESC, %[2]s
	LIMIT $4`

// periodLocations — строки «Сессий по локациям» за окно и всего визитов (делитель долей).
func periodLocations(ctx context.Context, pool *pgxpool.Pool, sel, grp string, shopFilter any, start, end time.Time, limit int) ([]LocationRow, int64, error) {
	query := fmt.Sprintf(periodLocationSQL, sel, grp, inShop, humanOnly("e"), eventWindow("e", "$1", "$2"))
	rows, err := pool.Query(ctx, query, start, end, shopFilter, limit)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	result := []LocationRow{}
	var total int64
	for rows.Next() {
		var lr LocationRow
		if err := rows.Scan(&lr.Country, &lr.Subject, &lr.City, &lr.Sessions, &lr.Orders, &total); err != nil {
			return nil, 0, err
		}
		result = append(result, lr)
	}
	return result, total, rows.Err()
}
