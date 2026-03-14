-- Migration 007: Add google_analytics to tracking_pixels pixel_type CHECK
-- Also add partition for 2026_06 if missing

ALTER TABLE config.tracking_pixels
    DROP CONSTRAINT tracking_pixels_pixel_type_check;

ALTER TABLE config.tracking_pixels
    ADD CONSTRAINT tracking_pixels_pixel_type_check
    CHECK (pixel_type IN ('vk', 'yandex_metrika', 'meta', 'google_analytics', 'custom'));
