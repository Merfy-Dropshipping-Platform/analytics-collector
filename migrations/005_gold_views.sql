-- Migration 005: Create gold materialized views
-- Pre-computed dashboard KPIs from silver layer

-- Dashboard KPI (joins traffic + orders)
CREATE MATERIALIZED VIEW IF NOT EXISTS gold.dashboard_kpi AS
SELECT
    t.shop_id,
    t.tenant_id,
    t.day,
    -- Traffic
    t.page_views,
    t.unique_sessions,
    t.unique_visitors,
    -- Orders
    COALESCE(o.order_count, 0) AS order_count,
    COALESCE(o.total_revenue_cents, 0) AS total_revenue_cents,
    COALESCE(o.avg_order_cents, 0) AS avg_order_cents,
    -- Conversion
    CASE WHEN t.unique_sessions > 0
         THEN ROUND(COALESCE(o.order_count, 0)::numeric / t.unique_sessions * 100, 2)
         ELSE 0 END AS conversion_rate
FROM silver.daily_traffic t
LEFT JOIN silver.daily_orders o ON t.shop_id = o.shop_id AND t.day = o.day;

CREATE UNIQUE INDEX ON gold.dashboard_kpi (shop_id, day);

-- Top products (from bronze, last 30 days)
CREATE MATERIALIZED VIEW IF NOT EXISTS gold.top_products AS
SELECT
    shop_id,
    tenant_id,
    product_id,
    MAX(product_name) AS product_name,
    COUNT(*) AS sales_count,
    SUM(product_price_cents) AS total_revenue_cents
FROM bronze.events
WHERE event_type = 'purchase'
  AND product_id IS NOT NULL
  AND created_at >= now() - INTERVAL '30 days'
GROUP BY shop_id, tenant_id, product_id;

CREATE UNIQUE INDEX ON gold.top_products (shop_id, product_id);
