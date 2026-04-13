-- Migration 011: Fix top_products revenue calculation
-- Problem: MAX(product_price_cents) in dedup loses multi-variant line items
--   e.g. order with 4 items of same product at prices 200, 200, 500, 300
--   MAX gives 500, but actual revenue is 1200
-- Fix: SUM(product_price_cents) to capture all line item revenue per product per order

DROP MATERIALIZED VIEW IF EXISTS gold.top_products CASCADE;

CREATE MATERIALIZED VIEW gold.top_products AS
WITH deduped_products AS (
  SELECT
    shop_id, tenant_id, product_id, order_id, event_type,
    MAX(product_name) AS product_name,
    SUM(product_price_cents) AS product_price_cents
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

REFRESH MATERIALIZED VIEW gold.top_products;
