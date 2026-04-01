-- Migration 009: Add order_cancel event type for analytics reversal
-- When orders are cancelled, cancel events subtract from purchase totals
-- Idempotency: uses COUNT(DISTINCT order_id) so duplicate cancels don't double-subtract

-- Step 1: Drop gold views (they depend on silver)
DROP MATERIALIZED VIEW IF EXISTS gold.dashboard_kpi CASCADE;
DROP MATERIALIZED VIEW IF EXISTS gold.top_products CASCADE;

-- Step 2: Drop silver.daily_orders (will be recreated with cancel logic)
DROP MATERIALIZED VIEW IF EXISTS silver.daily_orders CASCADE;

-- Step 3: Update CHECK constraint on bronze.events to allow order_cancel
-- Note: constraint is on parent table, inherited by partitions
ALTER TABLE bronze.events DROP CONSTRAINT IF EXISTS events_event_type_check;
ALTER TABLE bronze.events ADD CONSTRAINT events_event_type_check
    CHECK (event_type IN (
        'page_view', 'product_view', 'add_to_cart', 'remove_from_cart',
        'checkout_start', 'purchase', 'session_start', 'order_cancel'
    ));

-- Step 4: Recreate silver.daily_orders with net calculation (purchase - cancel)
CREATE MATERIALIZED VIEW silver.daily_orders AS
SELECT
    shop_id,
    tenant_id,
    date_trunc('day', created_at)::date AS day,
    COUNT(DISTINCT order_id) FILTER (WHERE event_type = 'purchase')
      - COUNT(DISTINCT order_id) FILTER (WHERE event_type = 'order_cancel')
      AS order_count,
    COALESCE(SUM(order_total_cents) FILTER (WHERE event_type = 'purchase'), 0)
      - COALESCE(SUM(order_total_cents) FILTER (WHERE event_type = 'order_cancel'), 0)
      AS total_revenue_cents,
    CASE
      WHEN (COUNT(DISTINCT order_id) FILTER (WHERE event_type = 'purchase')
            - COUNT(DISTINCT order_id) FILTER (WHERE event_type = 'order_cancel')) > 0
      THEN (
        COALESCE(SUM(order_total_cents) FILTER (WHERE event_type = 'purchase'), 0)
        - COALESCE(SUM(order_total_cents) FILTER (WHERE event_type = 'order_cancel'), 0)
      ) / (
        COUNT(DISTINCT order_id) FILTER (WHERE event_type = 'purchase')
        - COUNT(DISTINCT order_id) FILTER (WHERE event_type = 'order_cancel')
      )
      ELSE 0
    END AS avg_order_cents,
    COUNT(DISTINCT session_id) FILTER (WHERE event_type = 'purchase') AS ordering_sessions
FROM bronze.events
WHERE event_type IN ('purchase', 'order_cancel')
  AND order_id IS NOT NULL
  AND created_at >= now() - INTERVAL '13 months'
GROUP BY shop_id, tenant_id, date_trunc('day', created_at)::date;

CREATE UNIQUE INDEX ON silver.daily_orders (shop_id, day);

-- Step 5: Recreate gold.dashboard_kpi (same definition as migration 005)
CREATE MATERIALIZED VIEW gold.dashboard_kpi AS
SELECT
    t.shop_id,
    t.tenant_id,
    t.day,
    t.page_views,
    t.unique_sessions,
    t.unique_visitors,
    COALESCE(o.order_count, 0) AS order_count,
    COALESCE(o.total_revenue_cents, 0) AS total_revenue_cents,
    COALESCE(o.avg_order_cents, 0) AS avg_order_cents,
    CASE WHEN t.unique_sessions > 0
         THEN ROUND(COALESCE(o.order_count, 0)::numeric / t.unique_sessions * 100, 2)
         ELSE 0 END AS conversion_rate
FROM silver.daily_traffic t
LEFT JOIN silver.daily_orders o ON t.shop_id = o.shop_id AND t.day = o.day;

CREATE UNIQUE INDEX ON gold.dashboard_kpi (shop_id, day);

-- Step 6: Recreate gold.top_products with cancel subtraction
CREATE MATERIALIZED VIEW gold.top_products AS
SELECT
    shop_id,
    tenant_id,
    product_id,
    MAX(product_name) AS product_name,
    COUNT(*) FILTER (WHERE event_type = 'purchase')
      - COUNT(*) FILTER (WHERE event_type = 'order_cancel')
      AS sales_count,
    COALESCE(SUM(product_price_cents) FILTER (WHERE event_type = 'purchase'), 0)
      - COALESCE(SUM(product_price_cents) FILTER (WHERE event_type = 'order_cancel'), 0)
      AS total_revenue_cents
FROM bronze.events
WHERE event_type IN ('purchase', 'order_cancel')
  AND product_id IS NOT NULL
  AND created_at >= now() - INTERVAL '30 days'
GROUP BY shop_id, tenant_id, product_id;

CREATE UNIQUE INDEX ON gold.top_products (shop_id, product_id);

-- Step 7: Refresh all views
REFRESH MATERIALIZED VIEW silver.daily_orders;
REFRESH MATERIALIZED VIEW gold.dashboard_kpi;
REFRESH MATERIALIZED VIEW gold.top_products;
