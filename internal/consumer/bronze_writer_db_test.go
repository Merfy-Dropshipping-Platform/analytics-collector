//go:build dbtest

package consumer

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Exercises the real 22-column insertBatch against Postgres. pgx validates the placeholder
// count against len(args), so a successful 2-event insert proves the $1..$44 arity, and the
// SELECT proves the three geo_* columns are written (and privacy: no ip_address).
// Gated by build tag `dbtest` + GEO_TEST_DB.
func TestInsertBatch22Cols_DB(t *testing.T) {
	dsn := os.Getenv("GEO_TEST_DB")
	if dsn == "" {
		t.Skip("GEO_TEST_DB not set")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer pool.Close()

	// Idempotent across re-runs: clear this test's shop first.
	if _, err := pool.Exec(ctx, `DELETE FROM bronze.events WHERE shop_id='shopBW'`); err != nil {
		t.Fatalf("cleanup: %v", err)
	}

	bw := &BronzeWriter{pool: pool}
	events := []Event{
		{
			ShopID: "shopBW", TenantID: "tBW", Type: "page_view", SessionID: "bw-s1",
			VisitorID: "bw-v1", PageURL: "/x",
			GeoCountry: "RU", GeoSubject: "Республика Крым", GeoCity: "Симферополь",
			Timestamp: "2026-07-14T10:00:00Z",
		},
		{
			// Second event: empty geo → must land as NULL (nilIfEmpty).
			ShopID: "shopBW", TenantID: "tBW", Type: "purchase", SessionID: "bw-s1",
			OrderID: "bw-o1", OrderTotal: 4200,
			Timestamp: "2026-07-14T10:01:00Z",
		},
	}

	if err := bw.insertBatch(ctx, events); err != nil {
		t.Fatalf("insertBatch (22 cols): %v", err)
	}

	var geoRows, nullGeoRows, ipRows int
	if err := pool.QueryRow(ctx, `
		SELECT
		  count(*) FILTER (WHERE geo_country='RU' AND geo_subject='Республика Крым' AND geo_city='Симферополь'),
		  count(*) FILTER (WHERE geo_country IS NULL AND geo_subject IS NULL AND geo_city IS NULL),
		  count(*) FILTER (WHERE ip_address IS NOT NULL)
		FROM bronze.events WHERE shop_id='shopBW'
	`).Scan(&geoRows, &nullGeoRows, &ipRows); err != nil {
		t.Fatalf("verify select: %v", err)
	}
	if geoRows != 1 {
		t.Errorf("geo-stamped rows = %d; want 1", geoRows)
	}
	if nullGeoRows != 1 {
		t.Errorf("null-geo rows = %d; want 1 (empty geo → NULL)", nullGeoRows)
	}
	if ipRows != 0 {
		t.Errorf("ip_address non-null rows = %d; want 0 (privacy)", ipRows)
	}
}
