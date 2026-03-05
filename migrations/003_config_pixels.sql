-- Migration 003: Create config.tracking_pixels table

CREATE TABLE IF NOT EXISTS config.tracking_pixels (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    shop_id     TEXT NOT NULL,
    tenant_id   TEXT NOT NULL,
    pixel_type  TEXT NOT NULL CHECK (pixel_type IN ('vk', 'yandex_metrika', 'meta', 'custom')),
    pixel_id    TEXT NOT NULL,
    name        TEXT,
    is_active   BOOLEAN DEFAULT true,
    config      JSONB DEFAULT '{}',
    created_at  TIMESTAMPTZ DEFAULT now(),
    updated_at  TIMESTAMPTZ DEFAULT now()
);

-- Unique constraint: one pixel_type+pixel_id per shop
CREATE UNIQUE INDEX IF NOT EXISTS uq_pixels_shop_type_id
    ON config.tracking_pixels (shop_id, pixel_type, pixel_id);

-- Index for loader.js queries
CREATE INDEX IF NOT EXISTS idx_pixels_shop_active
    ON config.tracking_pixels (shop_id, is_active);

-- Index for tenant isolation
CREATE INDEX IF NOT EXISTS idx_pixels_tenant
    ON config.tracking_pixels (tenant_id);
