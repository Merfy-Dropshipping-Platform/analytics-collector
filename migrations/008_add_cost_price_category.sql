-- Migration 008: Add cost_price_cents and category_id for margin analytics (spec 045)

ALTER TABLE bronze.events ADD COLUMN IF NOT EXISTS cost_price_cents INTEGER;
ALTER TABLE bronze.events ADD COLUMN IF NOT EXISTS category_id TEXT;
