-- Migration 012: Fix unique_sessions to count only browsing events
-- Problem: unique_sessions counted ALL event types including server-side purchase events,
-- inflating the denominator for conversion rate (e.g., 9.09% instead of 10%).
-- Fix: Count only page_view and session_start events as real user sessions.

-- Step 1: Drop gold view (depends on silver.daily_traffic)
DROP MATERIALIZED VIEW IF EXISTS gold.dashboard_kpi;

-- Step 2: Drop and recreate silver.daily_traffic with fixed unique_sessions
DROP MATERIALIZED VIEW IF EXISTS silver.daily_traffic;

CREATE MATERIALIZED VIEW silver.daily_traffic AS
SELECT
    shop_id,
    tenant_id,
    date_trunc('day', created_at)::date AS day,
    COUNT(*) FILTER (WHERE event_type = 'page_view') AS page_views,
    COUNT(DISTINCT session_id) FILTER (WHERE event_type IN ('page_view', 'session_start')) AS unique_sessions,
    COUNT(DISTINCT visitor_id) AS unique_visitors,
    COUNT(*) FILTER (WHERE event_type = 'session_start') AS new_sessions
FROM bronze.events
WHERE created_at >= now() - INTERVAL '13 months'
GROUP BY shop_id, tenant_id, date_trunc('day', created_at)::date;

CREATE UNIQUE INDEX ON silver.daily_traffic (shop_id, day);

-- Step 3: Recreate gold.dashboard_kpi
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
