-- Migration 018: отметка согласия у «своего кода» мерчанта (блок 13 «Секций и SDK», часть 13в; В13-1 В).
-- required — код ждёт «Принять» в баннере cookie, none — запускается сразу. Старые строки и счётчики — required.
-- Повторный запуск (entrypoint.sh на каждом старте) ничего не меняет.
ALTER TABLE config.tracking_pixels ADD COLUMN IF NOT EXISTS consent TEXT NOT NULL DEFAULT 'required';
ALTER TABLE config.tracking_pixels DROP CONSTRAINT IF EXISTS chk_tracking_pixels_consent;
ALTER TABLE config.tracking_pixels
    ADD CONSTRAINT chk_tracking_pixels_consent CHECK (consent IN ('required', 'none'));
