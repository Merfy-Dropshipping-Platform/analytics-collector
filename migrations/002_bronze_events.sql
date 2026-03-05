-- Migration 002: Create partitioned bronze.events table
-- Partitions managed by Go service (no pg_partman needed)

CREATE TABLE IF NOT EXISTS bronze.events (
    id              UUID DEFAULT gen_random_uuid(),
    shop_id         TEXT NOT NULL,
    tenant_id       TEXT,
    session_id      TEXT NOT NULL,
    visitor_id      TEXT,
    event_type      TEXT NOT NULL CHECK (event_type IN (
        'page_view', 'product_view', 'add_to_cart', 'remove_from_cart',
        'checkout_start', 'purchase', 'session_start'
    )),
    page_url        TEXT,
    page_title      TEXT,
    referrer        TEXT,
    utm_source      TEXT,
    utm_medium      TEXT,
    utm_campaign    TEXT,
    utm_content     TEXT,
    utm_term        TEXT,
    product_id      TEXT,
    product_name    TEXT,
    product_price_cents INTEGER,
    order_id        TEXT,
    order_total_cents   INTEGER,
    extra           JSONB DEFAULT '{}',
    user_agent      TEXT,
    ip_address      TEXT,
    event_timestamp TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
) PARTITION BY RANGE (created_at);

-- Indexes (applied to each partition automatically)
CREATE INDEX IF NOT EXISTS idx_events_shop_created ON bronze.events (shop_id, created_at);
CREATE INDEX IF NOT EXISTS idx_events_shop_type_created ON bronze.events (shop_id, event_type, created_at);
CREATE INDEX IF NOT EXISTS idx_events_session ON bronze.events (session_id);

-- Initial partitions (Go service will create future ones automatically)
CREATE TABLE IF NOT EXISTS bronze.events_2026_02 PARTITION OF bronze.events
    FOR VALUES FROM ('2026-02-01') TO ('2026-03-01');
CREATE TABLE IF NOT EXISTS bronze.events_2026_03 PARTITION OF bronze.events
    FOR VALUES FROM ('2026-03-01') TO ('2026-04-01');
CREATE TABLE IF NOT EXISTS bronze.events_2026_04 PARTITION OF bronze.events
    FOR VALUES FROM ('2026-04-01') TO ('2026-05-01');
CREATE TABLE IF NOT EXISTS bronze.events_2026_05 PARTITION OF bronze.events
    FOR VALUES FROM ('2026-05-01') TO ('2026-06-01');
