-- Migration 014: Guard against orphan order_cancel (phantom negative revenue)
--
-- Problem: `order_cancel` was netted as -1 order / -revenue UNCONDITIONALLY, even
-- when NO paired `purchase` was ever counted for that order. `purchase` is emitted by
-- the orders service only on payment success, but several paths cancel an order that
-- never produced a purchase:
--   - unpaid pending/processing/failed orders (admin/client cancel, cart auto-timeout,
--     reconciliation) — now also gated producer-side in orders.service.ts;
--   - COD/auto-confirm & reconciliation orders that set paymentStatus='succeeded' but
--     never emit `purchase` (producer gate can't see these — this guard is the backstop);
--   - a `purchase` lost over the best-effort HTTP collector ingest.
-- Result: a day (and the KPI window) goes negative — e.g. shop ef6e5979… on 2026-07-02
-- showed order_count -1 / total_revenue_cents -10500 for order #0010 (paid_at NULL).
--
-- Fix: subtract an `order_cancel` ONLY when a paired `purchase` exists in bronze for the
-- same (shop_id, order_id). A legitimate paid-then-cancelled order still nets to 0, but
-- an orphan cancel is ignored. This ALSO self-heals existing phantoms: matviews fully
-- recompute from bronze on the 1-min refresh loop, so redefining them retroactively
-- drops the already-counted orphans — no manual backfill needed.
--
-- Safe from the aged-out-purchase trap: bronze retention is ~2 months (monthly-partition
-- drop in maintenance.go), and gold recomputes from bronze, so a purchase that aged out of
-- bronze already lost its +1 from the aggregate — suppressing its cancel keeps the books
-- balanced. NO GREATEST(...,0) floors: a real reversal of a prior-window purchase must be
-- allowed to net negative within its window; we fix orphans, not legitimate netting.

-- Drop in dependency order (dashboard_kpi depends on daily_orders).
DROP MATERIALIZED VIEW IF EXISTS gold.dashboard_kpi CASCADE;
DROP MATERIALIZED VIEW IF EXISTS gold.top_products CASCADE;
DROP MATERIALIZED VIEW IF EXISTS silver.daily_orders CASCADE;

-- silver.daily_orders — carries over migration 010 (dedup) + adds the orphan guard.
CREATE MATERIALIZED VIEW silver.daily_orders AS
WITH valid_orders AS (
  -- Orders that were actually counted as a sale (a purchase event exists).
  SELECT DISTINCT shop_id, order_id
  FROM bronze.events
  WHERE event_type = 'purchase' AND order_id IS NOT NULL
),
deduped AS (
  SELECT
    e.shop_id,
    e.tenant_id,
    date_trunc('day', e.created_at)::date AS day,
    e.order_id,
    e.event_type,
    MAX(e.order_total_cents) AS order_total_cents,
    MAX(e.session_id) AS session_id
  FROM bronze.events e
  WHERE e.event_type IN ('purchase', 'order_cancel')
    AND e.order_id IS NOT NULL
    AND e.created_at >= now() - INTERVAL '13 months'
    -- Orphan guard: keep every purchase; keep an order_cancel only if its order was purchased.
    AND (
      e.event_type = 'purchase'
      OR EXISTS (
        SELECT 1 FROM valid_orders v
        WHERE v.shop_id = e.shop_id AND v.order_id = e.order_id
      )
    )
  GROUP BY e.shop_id, e.tenant_id, date_trunc('day', e.created_at)::date, e.order_id, e.event_type
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

-- gold.dashboard_kpi — verbatim from migration 012 (depends on the recreated daily_orders).
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

-- gold.top_products — carries over migration 011 (SUM line items) + adds the orphan guard.
CREATE MATERIALIZED VIEW gold.top_products AS
WITH valid_orders AS (
  -- MUST share the SAME 30-day lower bound as deduped_products below. Without it,
  -- a purchase 30–60 days ago (still in bronze — retention is ~2 monthly partitions)
  -- would qualify its cancel as "paired" while the purchase's own +revenue is already
  -- outside the 30-day aggregate window → a lone negative/under-counted product row.
  -- Windowing valid_orders keeps the pair symmetric: a cancel whose purchase is out of
  -- window is treated as an orphan and suppressed (net 0), matching that the +revenue
  -- is out of window too. (silver.daily_orders uses a 13-month window >> retention, so
  -- there the unwindowed valid_orders can never diverge and needs no bound.)
  SELECT DISTINCT shop_id, order_id
  FROM bronze.events
  WHERE event_type = 'purchase' AND order_id IS NOT NULL
    AND created_at >= now() - INTERVAL '30 days'
),
deduped_products AS (
  SELECT
    e.shop_id, e.tenant_id, e.product_id, e.order_id, e.event_type,
    MAX(e.product_name) AS product_name,
    SUM(e.product_price_cents) AS product_price_cents
  FROM bronze.events e
  WHERE e.event_type IN ('purchase', 'order_cancel')
    AND e.product_id IS NOT NULL
    AND e.created_at >= now() - INTERVAL '30 days'
    AND (
      e.event_type = 'purchase'
      OR EXISTS (
        SELECT 1 FROM valid_orders v
        WHERE v.shop_id = e.shop_id AND v.order_id = e.order_id
      )
    )
  GROUP BY e.shop_id, e.tenant_id, e.product_id, e.order_id, e.event_type
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
