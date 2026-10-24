//go:build dbtest

package handler

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/merfy/analytics-collector/internal/db"
)

// Отчёты одной минуты — одна строка со счётчиком: обе вставки в одной транзакции, now() в ней один.
func TestPoolCSPStore_SameMinute_DB(t *testing.T) {
	pool := needDB(t)
	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	store := PoolCSPStore{DB: tx}
	report := CSPReport{Host: "same-minute.example", Directive: "script-src-elem", Blocked: "inline", Disposition: "report"}
	for range 2 {
		if err := store.Add(ctx, report); err != nil {
			t.Fatal(err)
		}
	}
	var rows, reports int
	err = tx.QueryRow(ctx, `SELECT count(*), COALESCE(sum(reports), 0) FROM bronze.csp_reports WHERE host = $1`, report.Host).Scan(&rows, &reports)
	if err != nil || rows != 1 || reports != 2 {
		t.Fatalf("строк %d, отчётов %d, ошибка %v; want 1 и 2", rows, reports, err)
	}
}

func TestPruneCSPReports_DB(t *testing.T) {
	pool := needDB(t)
	ctx := context.Background()
	defer pool.Exec(ctx, `DELETE FROM bronze.csp_reports WHERE host LIKE 'prune-%'`)
	_, err := pool.Exec(ctx, `
		INSERT INTO bronze.csp_reports (minute, host, directive, blocked, disposition, reports) VALUES
		(date_trunc('minute', now()) - interval '40 days', 'prune-old.example', 'img-src', 'data', 'report', 1),
		(date_trunc('minute', now()), 'prune-new.example', 'img-src', 'data', 'report', 1)`)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.PruneCSPReports(ctx, pool, db.CSPReportDays); err != nil {
		t.Fatal(err)
	}
	var left []string
	err = pool.QueryRow(ctx, `SELECT array_agg(host ORDER BY host) FROM bronze.csp_reports WHERE host LIKE 'prune-%'`).Scan(&left)
	if err != nil || len(left) != 1 || left[0] != "prune-new.example" {
		t.Fatalf("осталось %v, ошибка %v; want [prune-new.example]", left, err)
	}
}

// Обработчик RPC пикселей — как его регистрирует main.go.
type pixelHandle func(context.Context, *pgxpool.Pool, json.RawMessage) (any, error)

func pixelCall(t *testing.T, handle pixelHandle, payload map[string]any) (any, error) {
	t.Helper()
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	return handle(context.Background(), needDB(t), raw)
}

// Отметка «своего кода»: нет — required; none; не из двух — ошибка и при создании, и при правке (правка тогда не меняет
// других полей); список отдаёт отметку.
func TestPixelConsent_DB(t *testing.T) {
	pool := needDB(t)
	defer pool.Exec(context.Background(), `DELETE FROM config.tracking_pixels WHERE shop_id = 'shop-consent'`)
	create := func(consent string) (Pixel, error) {
		result, err := pixelCall(t, HandlePixelsCreate, map[string]any{"shopId": "shop-consent", "tenantId": "org-1", "pixel_type": "custom", "pixel_id": "<script>" + consent + "()</script>", "name": "Чат", "consent": consent})
		pixel, _ := result.(Pixel)
		return pixel, err
	}
	plain, err := create("")
	if err != nil || plain.Consent != "required" {
		t.Fatalf("без отметки: %+v, %v; want required", plain, err)
	}
	now, err := create("none")
	if err != nil || now.Consent != "none" {
		t.Fatalf("none: %+v, %v", now, err)
	}
	if _, err := create("later"); err == nil {
		t.Fatal("later принят при создании")
	}
	updated, err := pixelCall(t, HandlePixelsUpdate, map[string]any{"id": plain.ID, "tenantId": "org-1", "consent": "none"})
	if pixel, _ := updated.(Pixel); err != nil || pixel.Consent != "none" {
		t.Fatalf("правка на none: %+v, %v", updated, err)
	}
	if _, err := pixelCall(t, HandlePixelsUpdate, map[string]any{"id": now.ID, "tenantId": "org-1", "consent": "soon", "name": "Другое"}); err == nil {
		t.Fatal("soon принят при правке")
	}
	listed, err := pixelCall(t, HandlePixelsList, map[string]any{"shopId": "shop-consent", "tenantId": "org-1"})
	if err != nil {
		t.Fatal(err)
	}
	page, _ := listed.(map[string]any)
	pixels, _ := page["pixels"].([]Pixel)
	if len(pixels) != 2 {
		t.Fatalf("список: %d пикселей; want 2", len(pixels))
	}
	for _, pixel := range pixels {
		if pixel.ID == now.ID && (pixel.Name != "Чат" || pixel.Consent != "none") {
			t.Fatalf("после отказа правки: %+v; want имя Чат и none", pixel)
		}
	}
}
