#!/bin/sh
set -e

echo "Running SQL migrations..."
for f in /migrations/*.sql; do
  if [ -f "$f" ]; then
    echo "Applying: $f"
    psql "$DATABASE_URL" -f "$f" 2>&1 || echo "Warning: migration $f had errors (may be already applied)"
  fi
done
echo "Migrations complete."

exec /collector
