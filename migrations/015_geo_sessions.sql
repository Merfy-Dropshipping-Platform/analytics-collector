-- Migration 015: Geo dimension + "sessions by location" rollup.
--
-- Adds coarse geo (country / subject / city) captured in-process at /collect ingest
-- (resolved from r.RemoteAddr, then the annexed-territory override applied) and a
-- READ-ONLY daily rollup that answers the "Сессии по локациям" widget:
--   COUNT(DISTINCT session_id), net orders, net revenue  GROUP BY country, subject, city, day.
--
-- Privacy (152-ФЗ): only the coarse geo is stored. The raw IP is NEVER written to
-- bronze or carried on RabbitMQ — resolution happens in the collector and only the
-- three geo_* strings reach the DB.
--
-- HOT PATH: this migration adds NO index to bronze.events. The three geo_* columns are
-- nullable / no-default => ADD COLUMN is a metadata-only catalog change (no table/partition
-- rewrite, momentary lock), so the batched INSERT path is untouched. The rollup is a
-- background matview refreshed by the maintenance loop (REFRESH ... CONCURRENTLY) — never
-- in the request path. A geo index on bronze would only slow every INSERT for a report
-- that already reads fine via the matview, so we deliberately do not add one.
--
-- Idempotent (entrypoint.sh re-runs every migration on each boot): ADD COLUMN IF NOT EXISTS,
-- CREATE MATERIALIZED VIEW IF NOT EXISTS, and NAMED unique index with IF NOT EXISTS — so
-- re-runs neither fail nor accumulate duplicate indexes (see migration 013's lesson).

-- 1. Geo columns on the partitioned parent (cascades to every partition, metadata-only).
ALTER TABLE bronze.events ADD COLUMN IF NOT EXISTS geo_country TEXT;
ALTER TABLE bronze.events ADD COLUMN IF NOT EXISTS geo_subject TEXT;
ALTER TABLE bronze.events ADD COLUMN IF NOT EXISTS geo_city    TEXT;

-- 2. silver.daily_geo — additive geo cube at (day, shop, tenant, country, subject, city).
--
-- Session→geo attribution: browsing events (page_view / session_start) carry the REAL client
-- IP resolved at ingest, so a canonical geo is derived per (shop_id, session_id) from them.
-- Server-side purchase/order_cancel events carry the emitter's IP (datacenter) and MUST NOT
-- drive geo; instead each order is attributed to ITS session's browsing geo. Orders whose
-- session never produced a locatable browsing event fall into the NULL ("Не определено")
-- bucket rather than being dropped or mis-placed — so the cube reconciles exactly with the
-- existing dashboard KPIs: SUM(sessions) per day == silver.daily_traffic.unique_sessions,
-- SUM(orders)/SUM(revenue) per day == silver.daily_orders (same orphan guard + dedup as 014).
--
-- Additivity: every session and every order maps to exactly ONE (country,subject,city) triple,
-- so the per-triple counts are DISJOINT and roll up to subject / country (and across shops) by
-- plain SUM — including COUNT(DISTINCT session_id), which is normally non-additive. This is what
-- lets the by_location RPC pick its granularity with a GROUP BY over one small matview.
--
-- Day semantics match the rest of the silver layer: a session/order is counted on each day it
-- was active (daily-unique) — a session spanning midnight counts once per day, same accepted
-- approximation as daily_traffic.unique_sessions.
-- NB: tenant_id is intentionally NOT carried in this cube. The by_location handler groups by
-- geo and filters (optionally) by shop_id only, never by tenant. Including tenant_id would let a
-- shop whose browsing and order streams carry different tenant_id (or NULL vs set) produce two
-- rows for the same (shop, day, geo) triple, colliding on the unique index below → REFRESH would
-- fail and freeze the widget platform-wide. Dropping it makes the grain exactly match the index.
CREATE MATERIALIZED VIEW IF NOT EXISTS silver.daily_geo AS
WITH
-- Canonical geo per session: earliest browsing event that has geo (non-null preferred);
-- if the session was never locatable, its geo triple stays NULL (unknown bucket).
session_geo AS (
    SELECT DISTINCT ON (shop_id, session_id)
        shop_id,
        session_id,
        geo_country,
        geo_subject,
        geo_city
    FROM bronze.events
    WHERE event_type IN ('page_view', 'session_start')
      AND created_at >= now() - INTERVAL '13 months'
    ORDER BY shop_id, session_id,
             (geo_country IS NULL),   -- located events first
             created_at               -- then first touch
),
-- Days each session was active (browsing) — mirrors daily_traffic.unique_sessions filter.
session_days AS (
    SELECT DISTINCT
        shop_id,
        session_id,
        date_trunc('day', created_at)::date AS day
    FROM bronze.events
    WHERE event_type IN ('page_view', 'session_start')
      AND created_at >= now() - INTERVAL '13 months'
),
sessions_agg AS (
    SELECT
        sd.shop_id,
        sd.day,
        sg.geo_country,
        sg.geo_subject,
        sg.geo_city,
        COUNT(*) AS sessions   -- sd is one row per (shop,session,day); each session→one geo
    FROM session_days sd
    JOIN session_geo sg
      ON sg.shop_id = sd.shop_id AND sg.session_id = sd.session_id
    GROUP BY sd.shop_id, sd.day,
             sg.geo_country, sg.geo_subject, sg.geo_city
),
-- Orphan guard (verbatim intent from migration 014): a purchase makes an order "valid";
-- an order_cancel only counts if its order was actually purchased. 13-month window >> bronze
-- retention (~2 monthly partitions), so the unwindowed valid_orders can never diverge here.
valid_orders AS (
    SELECT DISTINCT shop_id, order_id
    FROM bronze.events
    WHERE event_type = 'purchase' AND order_id IS NOT NULL
),
order_events AS (
    SELECT
        e.shop_id,
        date_trunc('day', e.created_at)::date AS day,
        e.order_id,
        e.event_type,
        MAX(e.order_total_cents) AS order_total_cents,
        MAX(e.session_id)        AS session_id   -- to attribute the order to its session's geo
    FROM bronze.events e
    WHERE e.event_type IN ('purchase', 'order_cancel')
      AND e.order_id IS NOT NULL
      AND e.created_at >= now() - INTERVAL '13 months'
      AND (
        e.event_type = 'purchase'
        OR EXISTS (
          SELECT 1 FROM valid_orders v
          WHERE v.shop_id = e.shop_id AND v.order_id = e.order_id
        )
      )
    GROUP BY e.shop_id, date_trunc('day', e.created_at)::date,
             e.order_id, e.event_type
),
orders_agg AS (
    SELECT
        oe.shop_id,
        oe.day,
        sg.geo_country,
        sg.geo_subject,
        sg.geo_city,
        COUNT(*) FILTER (WHERE oe.event_type = 'purchase')
          - COUNT(*) FILTER (WHERE oe.event_type = 'order_cancel') AS orders,
        COALESCE(SUM(oe.order_total_cents) FILTER (WHERE oe.event_type = 'purchase'), 0)
          - COALESCE(SUM(oe.order_total_cents) FILTER (WHERE oe.event_type = 'order_cancel'), 0)
          AS revenue_cents
    FROM order_events oe
    LEFT JOIN session_geo sg   -- LEFT: unlocatable order → NULL geo bucket (reconciles totals)
      ON sg.shop_id = oe.shop_id AND sg.session_id = oe.session_id
    GROUP BY oe.shop_id, oe.day,
             sg.geo_country, sg.geo_subject, sg.geo_city
)
SELECT
    COALESCE(s.shop_id, o.shop_id)     AS shop_id,
    COALESCE(s.day, o.day)             AS day,
    COALESCE(s.geo_country, o.geo_country) AS geo_country,
    COALESCE(s.geo_subject, o.geo_subject) AS geo_subject,
    COALESCE(s.geo_city, o.geo_city)       AS geo_city,
    COALESCE(s.sessions, 0)      AS sessions,
    COALESCE(o.orders, 0)        AS orders,
    COALESCE(o.revenue_cents, 0) AS revenue_cents
FROM sessions_agg s
FULL OUTER JOIN orders_agg o
  ON  s.shop_id = o.shop_id
  AND s.day = o.day
  AND s.geo_country IS NOT DISTINCT FROM o.geo_country
  AND s.geo_subject IS NOT DISTINCT FROM o.geo_subject
  AND s.geo_city    IS NOT DISTINCT FROM o.geo_city;

-- Column-only + NULLS NOT DISTINCT unique index => eligible for REFRESH ... CONCURRENTLY and
-- treats the NULL (unknown) geo bucket as a single key. Grain matches the matview exactly (no
-- tenant_id), so a shop with a split tenant_id stream can never collide here.
CREATE UNIQUE INDEX IF NOT EXISTS uq_daily_geo
    ON silver.daily_geo (shop_id, day, geo_country, geo_subject, geo_city) NULLS NOT DISTINCT;

-- Secondary index for the global by_location path (day-range scan without a shop filter).
-- Matview-only — does not touch the bronze INSERT hot path.
CREATE INDEX IF NOT EXISTS idx_daily_geo_day ON silver.daily_geo (day);

-- Populate now; the maintenance loop refreshes it CONCURRENTLY thereafter.
REFRESH MATERIALIZED VIEW silver.daily_geo;
