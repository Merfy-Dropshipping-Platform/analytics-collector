-- Migration 016: пометка трафика (человек / робот / свои). Правила 3–5 из ресерча.
-- Идемпотентно: entrypoint.sh перезапускает все миграции на каждом старте.
-- ADD COLUMN с константным DEFAULT — только каталог, без перезаписи партиций.
ALTER TABLE bronze.events ADD COLUMN IF NOT EXISTS traffic_type TEXT NOT NULL DEFAULT 'human';

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_events_traffic_type') THEN
    ALTER TABLE bronze.events
      ADD CONSTRAINT chk_events_traffic_type CHECK (traffic_type IN ('human', 'bot', 'internal'));
  END IF;
END $$;
