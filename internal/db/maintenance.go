package db

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// RetentionMonths — сколько месяцев хранятся сырые события (bronze.events). То же окно
// у материализованных представлений: литерал interval '… months' в миграции 017 —
// тест сверяет его с этой константой.
const RetentionMonths = 13

// matviewRefreshOrder — порядок обновления представлений: сначала silver, потом gold
// (gold.dashboard_kpi читает silver.daily_traffic и silver.daily_orders).
var matviewRefreshOrder = []string{
	"silver.daily_traffic",
	"silver.daily_orders",
	"silver.daily_funnel",
	"silver.daily_channel_attribution",
	"silver.daily_geo",
	"gold.dashboard_kpi",
	"gold.top_products",
}

// RefreshMatviews обновляет представления по порядку, не блокируя чтение (CONCURRENTLY).
// Ошибка одного представления не останавливает остальные; все ошибки возвращаются разом:
// цикл обслуживания их только логирует, тесты — проверяют.
func RefreshMatviews(ctx context.Context, pool *pgxpool.Pool) error {
	start := time.Now()

	var errs []error
	for _, v := range matviewRefreshOrder {
		if _, err := pool.Exec(ctx, "REFRESH MATERIALIZED VIEW CONCURRENTLY "+v); err != nil {
			slog.Error("refresh matview", "view", v, "error", err)
			errs = append(errs, fmt.Errorf("refresh %s: %w", v, err))
		}
	}

	slog.Info("matviews refreshed", "duration_ms", time.Since(start).Milliseconds())
	return errors.Join(errs...)
}

// EnsurePartitions создаёт месячные партиции на 3 месяца вперёд и удаляет те,
// что целиком старше срока хранения keepMonths (см. partitionsToDrop).
func EnsurePartitions(ctx context.Context, pool *pgxpool.Pool, keepMonths int) {
	now := time.Now()

	// Create partitions for next 3 months
	for i := 0; i <= 3; i++ {
		t := now.AddDate(0, i, 0)
		start := time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, time.UTC)
		end := start.AddDate(0, 1, 0)
		name := fmt.Sprintf("bronze.events_%d_%02d", start.Year(), start.Month())

		sql := fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS %s PARTITION OF bronze.events FOR VALUES FROM ('%s') TO ('%s')",
			name, start.Format("2006-01-02"), end.Format("2006-01-02"),
		)

		if _, err := pool.Exec(ctx, sql); err != nil {
			slog.Error("create partition", "name", name, "error", err)
		}
	}

	names, err := listPartitions(ctx, pool)
	if err != nil {
		slog.Error("list partitions", "error", err)
		return
	}

	for _, partName := range partitionsToDrop(names, now, keepMonths) {
		if _, err := pool.Exec(ctx, "DROP TABLE IF EXISTS "+partName); err != nil {
			slog.Error("drop partition", "name", partName, "error", err)
			continue
		}
		slog.Info("dropped old partition", "name", partName)
	}
}

func listPartitions(ctx context.Context, pool *pgxpool.Pool) ([]string, error) {
	rows, err := pool.Query(ctx, `
		SELECT inhrelid::regclass::text
		FROM pg_inherits
		WHERE inhparent = 'bronze.events'::regclass
		ORDER BY 1
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		names = append(names, name)
	}
	return names, rows.Err()
}

// partitionsToDrop решает, какие месячные партиции bronze.events можно удалить.
//
// Партиция bronze.events_YYYY_MM хранит строки с created_at в [1-е число месяца,
// 1-е число следующего) по UTC — так её создаёт EnsurePartitions. Удаляется только
// партиция, которая целиком старше границы now − keepMonths: её конец не позже границы.
// Если в партиции есть хоть одна строка моложе срока, она остаётся вся.
// Имена другого вида (не месячные) не трогаются.
func partitionsToDrop(names []string, now time.Time, keepMonths int) []string {
	cutoff := now.UTC().AddDate(0, -keepMonths, 0)

	var drop []string
	for _, name := range names {
		start, ok := partitionMonth(name)
		if !ok {
			continue
		}
		if !start.AddDate(0, 1, 0).After(cutoff) {
			drop = append(drop, name)
		}
	}
	return drop
}

// partitionMonth достаёт месяц из имени вида bronze.events_2026_08.
func partitionMonth(name string) (time.Time, bool) {
	var year, month int
	if _, err := fmt.Sscanf(name, "bronze.events_%d_%d", &year, &month); err != nil {
		return time.Time{}, false
	}
	if month < 1 || month > 12 {
		return time.Time{}, false
	}
	return time.Date(year, time.Month(month), 1, 0, 0, 0, 0, time.UTC), true
}

// StartMaintenanceLoop runs periodic maintenance tasks.
func StartMaintenanceLoop(ctx context.Context, pool *pgxpool.Pool, refreshInterval time.Duration, keepMonths int) {
	// Initial refresh
	go func() {
		time.Sleep(5 * time.Second) // Wait for data to flow
		RefreshMatviews(ctx, pool)
	}()

	refreshTicker := time.NewTicker(refreshInterval)
	partitionTicker := time.NewTicker(24 * time.Hour)

	go func() {
		defer refreshTicker.Stop()
		defer partitionTicker.Stop()

		// Ensure partitions on start
		EnsurePartitions(ctx, pool, keepMonths)

		for {
			select {
			case <-ctx.Done():
				return
			case <-refreshTicker.C:
				RefreshMatviews(ctx, pool)
			case <-partitionTicker.C:
				EnsurePartitions(ctx, pool, keepMonths)
			}
		}
	}()

	slog.Info("maintenance loop started", "refresh_interval", refreshInterval, "retention_months", keepMonths)
}
