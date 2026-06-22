-- Migration 013: Fix daily_channel_attribution concurrent refresh + clean duplicate indexes
--
-- Problem 1 (broken refresh): the unique index on silver.daily_channel_attribution used
--   COALESCE(utm_medium,''), COALESCE(utm_campaign,'') EXPRESSIONS. REFRESH MATERIALIZED VIEW
--   CONCURRENTLY requires a unique index on plain COLUMNS (no expressions, not partial), so
--   the 1-minute maintenance refresh failed every time with SQLSTATE 55000 and the matview
--   was frozen at its initial population. The other silver views index on (shop_id, day)
--   (plain columns) and refresh fine.
--
-- Problem 2 (index accumulation): migration 004 created the index UNNAMED and without
--   IF NOT EXISTS, and entrypoint.sh re-runs every migration on each container boot. The
--   matview itself is skipped (CREATE ... IF NOT EXISTS) but a new auto-named duplicate index
--   was created on every boot. daily_channel_attribution and daily_funnel accumulated dozens
--   of identical indexes (daily_traffic/daily_orders are spared because migrations 010/012
--   DROP ... CASCADE and recreate them each boot, resetting their indexes).
--
-- Fix: migration 004 now creates NAMED, column-based (NULLS NOT DISTINCT) unique indexes with
--   IF NOT EXISTS, so fresh deploys are correct and re-runs no longer accumulate. This migration
--   cleans up the legacy duplicates on already-deployed databases and repopulates the matview.
--
-- Idempotent: safe to run on every boot.

-- 1. Ensure the canonical column-only unique indexes exist (column-only => CONCURRENTLY-eligible).
CREATE UNIQUE INDEX IF NOT EXISTS uq_daily_channel_attribution
    ON silver.daily_channel_attribution (shop_id, day, channel, utm_medium, utm_campaign) NULLS NOT DISTINCT;
CREATE UNIQUE INDEX IF NOT EXISTS uq_daily_funnel
    ON silver.daily_funnel (shop_id, day);

-- 2. Drop every OTHER index on these two matviews (the accumulated unnamed duplicates,
--    including the old expression-based index on daily_channel_attribution).
DO $$
DECLARE r record;
BEGIN
  FOR r IN
    SELECT indexname FROM pg_indexes
    WHERE schemaname = 'silver' AND tablename = 'daily_channel_attribution'
      AND indexname <> 'uq_daily_channel_attribution'
  LOOP
    EXECUTE format('DROP INDEX silver.%I', r.indexname);
  END LOOP;

  FOR r IN
    SELECT indexname FROM pg_indexes
    WHERE schemaname = 'silver' AND tablename = 'daily_funnel'
      AND indexname <> 'uq_daily_funnel'
  LOOP
    EXECUTE format('DROP INDEX silver.%I', r.indexname);
  END LOOP;
END $$;

-- 3. Repopulate channel attribution (was frozen since first migration run).
--    Non-concurrent is fine at migration time; the maintenance loop refreshes CONCURRENTLY after.
REFRESH MATERIALIZED VIEW silver.daily_channel_attribution;
