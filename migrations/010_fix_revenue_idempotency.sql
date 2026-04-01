-- Migration 010: Fix revenue idempotency
-- Problem: SUM(order_total_cents) counted duplicate cancel events, causing over-subtraction
-- Fix: CTE deduplicates per (order_id, event_type) before aggregation
-- Both order_count AND revenue are now idempotent

DROP MATERIALIZED VIEW IF EXISTS gold.dashboard_kpi CASCADE;
DROP MATERIALIZED VIEW IF EXISTS gold.top_products CASCADE;
DROP MATERIALIZED VIEW IF EXISTS silver.daily_orders CASCADE;

-- Deduplicate events per (shop_id, day, order_id, event_type) BEFORE aggregating
CREATE MATERIALIZED VIEW silver.daily_orders AS
WITH deduped AS (
  SELECT
    shop_id,
    tenant_id,
    date_trunc('day', created_at)::date AS day,
    order_id,
    event_type,
    MAX(order_total_cents) AS order_total_cents,
    MAX(session_id) AS session_id
  FROM bronze.events
  WHERE event_type IN ('purchase', 'order_cancel')
    AND order_id IS NOT NULL
    AND created_at >= now() - INTERVAL '13 months'
  GROUP BY shop_id, tenant_id, date_trunc('day', created_at)::date, order_id, event_type
)
SELECT
  shop_id,
  tenant_id,
  day,
  COUNT(*) FILTER (WHERE event_type = 'purchase')
    - COUNT(*) FILTER (WHERE event_type = 'order_cancel')
    AS order_count,
  COALESCE(SUM(order_total_cents) FILTER (WHERE event_type = 'purchase'), 0)
    - COALESCE(SUM(order_total_cents) FILTER (WHERE event_type = 'order_cancel'), 0)
    AS total_revenue_cents,
  CASE
    WHEN (COUNT(*) FILTER (WHERE event_type = 'purchase')
          - COUNT(*) FILTER (WHERE event_type = 'order_cancel')) > 0
    THEN (
      COALESCE(SUM(order_total_cents) FILTER (WHERE event_type = 'purchase'), 0)
      - COALESCE(SUM(order_total_cents) FILTER (WHERE event_type = 'order_cancel'), 0)
    ) / (
      COUNT(*) FILTER (WHERE event_type = 'purchase')
      - COUNT(*) FILTER (WHERE event_type = 'order_cancel')
    )
    ELSE 0
  END AS avg_order_cents,
  COUNT(DISTINCT session_id) FILTER (WHERE event_type = 'purchase') AS ordering_sessions
FROM deduped
GROUP BY shop_id, tenant_id, day;

CREATE UNIQUE INDEX ON silver.daily_orders (shop_id, day);

-- Recreate gold.dashboard_kpi
CREATE MATERIALIZED VIEW gold.dashboard_kpi AS
SELECT
  t.shop_id, t.tenant_id, t.day, t.page_views, t.unique_sessions, t.unique_visitors,
  COALESCE(o.order_count, 0) AS order_count,
  COALESCE(o.total_revenue_cents, 0) AS total_revenue_cents,
  COALESCE(o.avg_order_cents, 0) AS avg_order_cents,
  CASE WHEN t.unique_sessions > 0
       THEN ROUND(COALESCE(o.order_count, 0)::numeric / t.unique_sessions * 100, 2)
       ELSE 0 END AS conversion_rate
FROM silver.daily_traffic t
LEFT JOIN silver.daily_orders o ON t.shop_id = o.shop_id AND t.day = o.day;

CREATE UNIQUE INDEX ON gold.dashboard_kpi (shop_id, day);

-- Recreate gold.top_products with same dedup fix
CREATE MATERIALIZED VIEW gold.top_products AS
WITH deduped_products AS (
  SELECT
    shop_id, tenant_id, product_id, event_type,
    MAX(product_name) AS product_name,
    MAX(product_price_cents) AS product_price_cents
  FROM bronze.events
  WHERE event_type IN ('purchase', 'order_cancel')
    AND product_id IS NOT NULL
    AND created_at >= now() - INTERVAL '30 days'
  GROUP BY shop_id, tenant_id, product_id, order_id, event_type
)
SELECT
  shop_id, tenant_id, product_id,
  MAX(product_name) AS product_name,
  COUNT(*) FILTER (WHERE event_type = 'purchase')
    - COUNT(*) FILTER (WHERE event_type = 'order_cancel')
    AS sales_count,
  COALESCE(SUM(product_price_cents) FILTER (WHERE event_type = 'purchase'), 0)
    - COALESCE(SUM(product_price_cents) FILTER (WHERE event_type = 'order_cancel'), 0)
    AS total_revenue_cents
FROM deduped_products
GROUP BY shop_id, tenant_id, product_id;

CREATE UNIQUE INDEX ON gold.top_products (shop_id, product_id);

REFRESH MATERIALIZED VIEW silver.daily_orders;
REFRESH MATERIALIZED VIEW gold.dashboard_kpi;
REFRESH MATERIALIZED VIEW gold.top_products;
