# Codebase Report: Orders Service — Drizzle Migration Mechanism
Generated: 2026-03-25

## Summary

The orders service uses Drizzle ORM with a **CLI-based migration runner** (`src/db/migrate.ts`) that runs as a Docker CMD step before the main NestJS app starts. Migrations are file-based SQL in the `drizzle/` folder. The `cost_price_cents` column is present in `schema.ts` but has **no corresponding SQL migration file** — it was never generated or applied to the production database.

---

## Questions Answered

### Q1: Is there a drizzle.config.ts?

**Location:** `/Users/alexey/projects/merfy/backend/services/orders/drizzle.config.ts`

Config summary:
- `schema`: `./src/db/schema.ts`
- `out`: `./drizzle`
- `dialect`: `postgresql`
- DB URL from `process.env.DATABASE_URL` or local fallback

### Q2: Is there a drizzle/ migrations folder?

**Yes.** Location: `/Users/alexey/projects/merfy/backend/services/orders/drizzle/`

Files present:
```
0000_nappy_silver_samurai.sql    (Dec 25) — initial schema: orders, order_items, order_settings, order_status_history, abandoned_cart_reminders
0001_sharp_baron_zemo.sql        (Dec 26) — order_payments table
0002_common_thundra.sql          (Feb 27) — discounts system tables + order_payments refund columns
0003_light_molecule_man.sql      (Mar 14) — ADD COLUMN customer_id to orders (DUPLICATE of below)
0003_store_customer_id.sql       (Mar 14) — ADD COLUMN customer_id to orders (SAME content, different name)
0004_cdek_tracking_columns.sql   (Mar 23) — ADD COLUMN IF NOT EXISTS cdek_uuid, cdek_number, delivery_status, etc.
meta/_journal.json               — tracks idx 0..4, last entry is 0004_cdek_tracking_columns
```

**WARNING:** There are two files named `0003_*` — `0003_light_molecule_man.sql` and `0003_store_customer_id.sql`. The journal references `0003_store_customer_id` (idx=3). The orphaned `0003_light_molecule_man.sql` is not in the journal and will be ignored by the migrator. Both files contain identical SQL (`ADD COLUMN customer_id`), so there is no data hazard, but this is a stale artifact.

### Q3: How does the service apply migrations on startup?

**Mechanism:** Docker `CMD` in `Dockerfile`:
```
CMD ["sh", "-c", "node dist/src/db/migrate.js && node dist/src/main.js"]
```

Step-by-step:
1. Container starts → `node dist/src/db/migrate.js` runs first
2. `migrate.ts` calls `ensureDatabaseExists()` — creates the DB if it doesn't exist (connects to `postgres` default DB)
3. Then calls `migrate(db, { migrationsFolder })` — applies any pending SQL files from `drizzle/` using Drizzle's built-in migrator (tracks applied migrations in the `__drizzle_migrations` table)
4. On success, `node dist/src/main.js` starts the NestJS app

There is **no `migrate()` call in `main.ts`** — migrations are purely a pre-start step, not an on-app-init hook.

The migration folder path logic in `migrate.ts`:
```ts
const migrationsFromCwd = path.join(process.cwd(), "drizzle");    // /app/drizzle (Docker)
const migrationsFolder = existsSync(migrationsFromCwd)
  ? migrationsFromCwd
  : path.join(__dirname, "../../drizzle");                          // fallback (local dev)
```

The Dockerfile copies `drizzle/` to `/app/drizzle`, so the CWD path is used in production.

### Q4: Is there a package.json script for drizzle push or migrate?

**Yes**, two scripts:
```json
"db:generate": "drizzle-kit generate"
"db:migrate": "node -r ts-node/register src/db/migrate.ts"
```

- `db:generate` — generates new SQL migration files from schema diff (run locally, then commit the file)
- `db:migrate` — runs the migration runner (ts-node, for local dev; Docker uses the compiled JS)

There is **no `drizzle push`** script. The service always uses file-based migrations, never schema push.

### Q5: Was `cost_price_cents` added to schema.ts but migration never generated?

**YES — CONFIRMED.**

`cost_price_cents` appears in `schema.ts` at line 205:
```ts
costPriceCents: integer("cost_price_cents"),
```

It also appears alongside these columns in `orderItems` (also in schema.ts but absent from all SQL files):
- `weightGrams` / `weight_grams`
- `lengthCm` / `length_cm`
- `widthCm` / `width_cm`
- `heightCm` / `height_cm`

**None of these 5 columns appear in any `.sql` migration file.** A grep across all `drizzle/*.sql` files returns zero matches for `cost_price_cents`, `weight_grams`, `length_cm`, `width_cm`, `height_cm`.

The initial `0000_nappy_silver_samurai.sql` does not include them in the `order_items` CREATE TABLE. No subsequent migration adds them either.

**Conclusion:** These 5 columns exist in the TypeScript schema but are NOT in the production database. Any INSERT or SELECT that references them will either fail (if NOT NULL) or silently read NULL. Since all 5 are nullable in the schema (no `.notNull()`), reads return NULL and INSERTs with values will throw a PostgreSQL `column does not exist` error.

---

## Production Migration Flow Diagram

```
Docker container start
        |
        v
node dist/src/db/migrate.js
        |
        +---> ensureDatabaseExists()
        |         connects to 'postgres' DB
        |         CREATE DATABASE if not found
        |
        +---> migrate(db, { migrationsFolder: '/app/drizzle' })
                  reads __drizzle_migrations table
                  applies only unapplied SQL files
                  in idx order: 0000 → 0001 → 0002 → 0003 → 0004
                  (0003_light_molecule_man.sql is NOT in journal → skipped)
        |
        v
node dist/src/main.js
        |
        v
NestJS app starts (DatabaseModule provides `db` via PG_CONNECTION token)
```

---

## Key Files

| File | Purpose |
|------|---------|
| `/Users/alexey/projects/merfy/backend/services/orders/drizzle.config.ts` | Drizzle Kit config (schema path, output dir, DB URL) |
| `/Users/alexey/projects/merfy/backend/services/orders/src/db/schema.ts` | TypeScript schema — source of truth for Drizzle types |
| `/Users/alexey/projects/merfy/backend/services/orders/src/db/migrate.ts` | Migration runner — called by Docker CMD |
| `/Users/alexey/projects/merfy/backend/services/orders/src/db/db.ts` | DB connection (Pool + drizzle instance) |
| `/Users/alexey/projects/merfy/backend/services/orders/drizzle/meta/_journal.json` | Migration journal — lists applied migrations (idx 0-4) |
| `/Users/alexey/projects/merfy/backend/services/orders/Dockerfile` | CMD: `node migrate.js && node main.js` |

---

## Critical Issues Found

| Issue | Severity | Detail |
|-------|----------|--------|
| `cost_price_cents` in schema but no migration | HIGH | Column missing in production DB |
| `weight_grams`, `length_cm`, `width_cm`, `height_cm` in schema but no migration | HIGH | 4 more columns missing in production DB |
| Two `0003_*` files | MEDIUM | `0003_light_molecule_man.sql` is an orphan not tracked in journal |
