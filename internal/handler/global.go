package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Глобальные (платформенные) хендлеры аналитики — агрегат по ВСЕМ магазинам.
//
// Зеркалят per-shop dashboard/funnel/top_products, но БЕЗ фильтра shop_id: KPI —
// суммарный агрегат по всем магазинам, time_series — GROUP BY day. Формы ответов
// РОВНО те же (DashboardResponse/FunnelResponse/TopProductsResponse), поэтому
// backOffice переиспользует существующий mapAnalyticsView 1:1.
//
// Семантика уников (unique_visitors/unique_sessions) наследует per-shop: это сумма
// СУТОЧНЫХ уников (посетитель активный N дней считается N раз) — тот же приближённый
// смысл, что уже используется в per-shop дашборде; кросс-магазинное пересечение
// visitor_id/session_id близко к нулю (разные витрины = разные куки).

type GlobalAnalyticsRequest struct {
	Period string `json:"period"`
	From   string `json:"from,omitempty"`
	To     string `json:"to,omitempty"`
}

func HandleGlobalDashboard(ctx context.Context, pool *pgxpool.Pool, payload json.RawMessage) (any, error) {
	var req GlobalAnalyticsRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, fmt.Errorf("invalid request: %w", err)
	}
	if req.Period == "" {
		return nil, fmt.Errorf("period is required")
	}

	now := time.Now().UTC()
	start, end := resolveRange(req.Period, req.From, req.To, now)
	prevStart, prevEnd := resolveRange(req.Period, "", "", start.Add(-time.Second))

	kpi, err := queryGlobalKPI(ctx, pool, start, end)
	if err != nil {
		return nil, err
	}
	kpiPrev, err := queryGlobalKPI(ctx, pool, prevStart, prevEnd)
	if err != nil {
		return nil, err
	}

	// Внутридневные 2-часовые бакеты (LIVE из bronze) для одиночного дня в пределах
	// retention; иначе — суточный gold-путь. Логика 1:1 с per-shop dashboard.
	singleDay := end.Sub(start) == 24*time.Hour
	withinBronzeRetention := !start.Before(now.AddDate(0, 0, -30))

	var ts []DashboardTimeSeries
	if singleDay && withinBronzeRetention {
		byBucket, errTS := queryGlobalIntradayBuckets(ctx, pool, start, end)
		if errTS != nil {
			return nil, errTS
		}
		ts = buildIntradayBuckets(start, end, byBucket)
	} else {
		ts, err = queryGlobalTimeSeries(ctx, pool, start, end)
		if err != nil {
			return nil, err
		}
	}

	return DashboardResponse{
		Period:     req.Period,
		KPI:        kpi,
		KPIPrev:    kpiPrev,
		TimeSeries: ts,
	}, nil
}

func queryGlobalKPI(ctx context.Context, pool *pgxpool.Pool, start, end time.Time) (DashboardKPI, error) {
	var kpi DashboardKPI
	err := pool.QueryRow(ctx, `
		SELECT
			COALESCE(SUM(total_revenue_cents), 0),
			COALESCE(SUM(order_count), 0),
			CASE WHEN SUM(order_count) > 0
				THEN (SUM(total_revenue_cents) / SUM(order_count))::bigint
				ELSE 0 END,
			COALESCE(SUM(unique_visitors), 0),
			COALESCE(SUM(unique_sessions), 0),
			COALESCE(SUM(page_views), 0),
			CASE WHEN SUM(unique_visitors) > 0
				THEN LEAST(ROUND(
					SUM(CASE WHEN unique_visitors > 0 THEN order_count ELSE 0 END)::numeric
					/ SUM(unique_visitors) * 100, 2), 100)
				ELSE 0 END
		FROM gold.dashboard_kpi
		WHERE day >= $1::date AND day < $2::date
	`, start, end).Scan(
		&kpi.TotalRevenueCents, &kpi.TotalOrders, &kpi.AvgOrderCents,
		&kpi.UniqueVisitors, &kpi.UniqueSessions, &kpi.PageViews,
		&kpi.ConversionRate,
	)
	return kpi, err
}

func queryGlobalTimeSeries(ctx context.Context, pool *pgxpool.Pool, start, end time.Time) ([]DashboardTimeSeries, error) {
	rows, err := pool.Query(ctx, `
		SELECT
			day::text,
			COALESCE(SUM(total_revenue_cents), 0),
			COALESCE(SUM(order_count), 0),
			COALESCE(SUM(unique_visitors), 0),
			COALESCE(SUM(unique_sessions), 0),
			COALESCE(SUM(page_views), 0)
		FROM gold.dashboard_kpi
		WHERE day >= $1::date AND day < $2::date
		GROUP BY day
		ORDER BY day
	`, start, end)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	dataByDay := make(map[string]DashboardTimeSeries)
	for rows.Next() {
		var t DashboardTimeSeries
		if err := rows.Scan(&t.Day, &t.RevenueCents, &t.Orders, &t.Visitors, &t.Sessions, &t.PageViews); err != nil {
			return nil, err
		}
		key := t.Day[:10]
		t.Day = key
		dataByDay[key] = t
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	var ts []DashboardTimeSeries
	for d := start; d.Before(end); d = d.Add(24 * time.Hour) {
		key := d.Format("2006-01-02")
		if entry, ok := dataByDay[key]; ok {
			ts = append(ts, entry)
		} else {
			ts = append(ts, DashboardTimeSeries{Day: key})
		}
	}
	return ts, nil
}

// queryGlobalIntradayBuckets — как queryIntradayBuckets, но по ВСЕМ магазинам.
// Пары purchase/order_cancel матчатся по (shop_id, order_id): order_id уникален
// в пределах магазина, но НЕ глобально, поэтому shop_id обязателен в группировке,
// иначе заказы разных магазинов с одинаковым order_id слились бы. session_id/
// visitor_id — клиентские UUID, глобально уникальны → COUNT DISTINCT корректен.
func queryGlobalIntradayBuckets(ctx context.Context, pool *pgxpool.Pool, start, end time.Time) (map[int64]DashboardTimeSeries, error) {
	rows, err := pool.Query(ctx, `
		WITH deduped_orders AS (
			SELECT (floor(extract(epoch from e.created_at)/7200))::bigint AS bucket_idx, e.shop_id, e.order_id, e.event_type, MAX(e.order_total_cents) AS order_total_cents
			FROM bronze.events e
			WHERE e.created_at >= $1 AND e.created_at < $2 AND e.event_type IN ('purchase','order_cancel') AND e.order_id IS NOT NULL
			  AND (e.event_type='purchase' OR EXISTS (
			    SELECT 1 FROM bronze.events p
			    WHERE p.event_type='purchase' AND p.shop_id=e.shop_id AND p.order_id=e.order_id
			  ))
			GROUP BY 1, e.shop_id, e.order_id, e.event_type
		),
		orders AS (
			SELECT bucket_idx,
				COUNT(*) FILTER (WHERE event_type='purchase') - COUNT(*) FILTER (WHERE event_type='order_cancel') AS order_count,
				COALESCE(SUM(order_total_cents) FILTER (WHERE event_type='purchase'),0) - COALESCE(SUM(order_total_cents) FILTER (WHERE event_type='order_cancel'),0) AS total_revenue_cents
			FROM deduped_orders GROUP BY bucket_idx
		),
		traffic AS (
			SELECT (floor(extract(epoch from created_at)/7200))::bigint AS bucket_idx,
				COUNT(*) FILTER (WHERE event_type='page_view') AS page_views,
				COUNT(DISTINCT session_id) FILTER (WHERE event_type IN ('page_view','session_start')) AS unique_sessions,
				COUNT(DISTINCT visitor_id) AS unique_visitors
			FROM bronze.events WHERE created_at >= $1 AND created_at < $2 GROUP BY 1
		)
		SELECT COALESCE(t.bucket_idx,o.bucket_idx) AS bucket_idx, COALESCE(o.total_revenue_cents,0), COALESCE(o.order_count,0), COALESCE(t.unique_visitors,0), COALESCE(t.unique_sessions,0), COALESCE(t.page_views,0)
		FROM traffic t FULL OUTER JOIN orders o ON t.bucket_idx = o.bucket_idx
	`, start, end)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	byBucket := make(map[int64]DashboardTimeSeries)
	for rows.Next() {
		var bucketIdx int64
		var t DashboardTimeSeries
		if err := rows.Scan(&bucketIdx, &t.RevenueCents, &t.Orders, &t.Visitors, &t.Sessions, &t.PageViews); err != nil {
			return nil, err
		}
		byBucket[bucketIdx] = t
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return byBucket, nil
}

func HandleGlobalFunnel(ctx context.Context, pool *pgxpool.Pool, payload json.RawMessage) (any, error) {
	var req GlobalAnalyticsRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, fmt.Errorf("invalid request: %w", err)
	}
	if req.Period == "" {
		return nil, fmt.Errorf("period is required")
	}

	start, end := resolveRange(req.Period, req.From, req.To, timeNow())

	var totalVisitors int64
	err := pool.QueryRow(ctx, `
		SELECT COALESCE(SUM(unique_visitors), 0)
		FROM silver.daily_traffic
		WHERE day >= $1::date AND day < $2::date
	`, start, end).Scan(&totalVisitors)
	if err != nil {
		return nil, err
	}

	var productViews, addToCart, checkoutStarts, purchases int64
	err = pool.QueryRow(ctx, `
		SELECT
			COALESCE(SUM(product_views), 0),
			COALESCE(SUM(add_to_cart), 0),
			COALESCE(SUM(checkout_starts), 0),
			COALESCE(SUM(purchases), 0)
		FROM silver.daily_funnel
		WHERE day >= $1::date AND day < $2::date
	`, start, end).Scan(&productViews, &addToCart, &checkoutStarts, &purchases)
	if err != nil {
		return nil, err
	}

	stages := []FunnelStage{
		{Name: "visits", Label: "Визиты", Count: totalVisitors, Rate: 100.0},
		{Name: "product_views", Label: "Просмотр товара", Count: productViews, Rate: safeRate(productViews, totalVisitors)},
		{Name: "add_to_cart", Label: "Добавлено в корзину", Count: addToCart, Rate: safeRate(addToCart, totalVisitors)},
		{Name: "checkout_starts", Label: "Готов к оплате", Count: checkoutStarts, Rate: safeRate(checkoutStarts, totalVisitors)},
		{Name: "purchases", Label: "Оплаченные заказы", Count: purchases, Rate: safeRate(purchases, totalVisitors)},
	}

	return FunnelResponse{Stages: stages}, nil
}

type GlobalTopProductsRequest struct {
	Period string `json:"period,omitempty"`
	Sort   string `json:"sort"`
	Limit  int    `json:"limit"`
}

func HandleGlobalTopProducts(ctx context.Context, pool *pgxpool.Pool, payload json.RawMessage) (any, error) {
	var req GlobalTopProductsRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, fmt.Errorf("invalid request: %w", err)
	}

	limit := req.Limit
	if limit <= 0 || limit > 100 {
		limit = 10
	}

	// gold.top_products хранит по строке на (shop_id, product_id) без периода
	// (all-time) — как и per-shop хендлер. Глобально: агрегат по product_id
	// (продукт принадлежит одному магазину, product_id глобально уникален).
	orderBy := "SUM(sales_count) DESC"
	if req.Sort == "revenue" {
		orderBy = "SUM(total_revenue_cents) DESC"
	}

	query := fmt.Sprintf(`
		SELECT product_id, MIN(product_name), SUM(sales_count), SUM(total_revenue_cents)
		FROM gold.top_products
		GROUP BY product_id
		ORDER BY %s
		LIMIT $1
	`, orderBy)

	rows, err := pool.Query(ctx, query, limit)
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
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return TopProductsResponse{Products: products}, nil
}
