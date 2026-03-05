-- Migration 004: Create silver materialized views
-- These aggregate bronze data for dashboard queries

-- Daily traffic
CREATE MATERIALIZED VIEW IF NOT EXISTS silver.daily_traffic AS
SELECT
    shop_id,
    tenant_id,
    date_trunc('day', created_at)::date AS day,
    COUNT(*) FILTER (WHERE event_type = 'page_view') AS page_views,
    COUNT(DISTINCT session_id) AS unique_sessions,
    COUNT(DISTINCT visitor_id) AS unique_visitors,
    COUNT(*) FILTER (WHERE event_type = 'session_start') AS new_sessions
FROM bronze.events
WHERE created_at >= now() - INTERVAL '13 months'
GROUP BY shop_id, tenant_id, date_trunc('day', created_at)::date;

CREATE UNIQUE INDEX ON silver.daily_traffic (shop_id, day);

-- Daily orders
CREATE MATERIALIZED VIEW IF NOT EXISTS silver.daily_orders AS
SELECT
    shop_id,
    tenant_id,
    date_trunc('day', created_at)::date AS day,
    COUNT(*) AS order_count,
    SUM(order_total_cents) AS total_revenue_cents,
    AVG(order_total_cents)::INTEGER AS avg_order_cents,
    COUNT(DISTINCT session_id) AS ordering_sessions
FROM bronze.events
WHERE event_type = 'purchase'
  AND created_at >= now() - INTERVAL '13 months'
GROUP BY shop_id, tenant_id, date_trunc('day', created_at)::date;

CREATE UNIQUE INDEX ON silver.daily_orders (shop_id, day);

-- Daily funnel
CREATE MATERIALIZED VIEW IF NOT EXISTS silver.daily_funnel AS
SELECT
    shop_id,
    tenant_id,
    date_trunc('day', created_at)::date AS day,
    COUNT(DISTINCT session_id) FILTER (WHERE event_type IN ('page_view', 'session_start')) AS visits,
    COUNT(DISTINCT session_id) FILTER (WHERE event_type = 'product_view') AS product_views,
    COUNT(DISTINCT session_id) FILTER (WHERE event_type = 'add_to_cart') AS add_to_cart,
    COUNT(DISTINCT session_id) FILTER (WHERE event_type = 'checkout_start') AS checkout_starts,
    COUNT(DISTINCT session_id) FILTER (WHERE event_type = 'purchase') AS purchases
FROM bronze.events
WHERE created_at >= now() - INTERVAL '13 months'
GROUP BY shop_id, tenant_id, date_trunc('day', created_at)::date;

CREATE UNIQUE INDEX ON silver.daily_funnel (shop_id, day);

-- Daily channel attribution
CREATE MATERIALIZED VIEW IF NOT EXISTS silver.daily_channel_attribution AS
SELECT
    shop_id,
    tenant_id,
    date_trunc('day', created_at)::date AS day,
    COALESCE(utm_source, 'direct') AS channel,
    utm_medium,
    utm_campaign,
    COUNT(*) FILTER (WHERE event_type = 'session_start') AS sessions,
    COUNT(*) FILTER (WHERE event_type = 'purchase') AS orders,
    SUM(order_total_cents) FILTER (WHERE event_type = 'purchase') AS revenue_cents
FROM bronze.events
WHERE created_at >= now() - INTERVAL '13 months'
GROUP BY shop_id, tenant_id, date_trunc('day', created_at)::date,
         COALESCE(utm_source, 'direct'), utm_medium, utm_campaign;

CREATE UNIQUE INDEX ON silver.daily_channel_attribution
    (shop_id, day, channel, COALESCE(utm_medium, ''), COALESCE(utm_campaign, ''));
