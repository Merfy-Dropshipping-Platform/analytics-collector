package db

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// RefreshMatviews refreshes all materialized views concurrently.
// Runs silver first, then gold (gold depends on silver).
func RefreshMatviews(ctx context.Context, pool *pgxpool.Pool) {
	start := time.Now()

	silverViews := []string{
		"silver.daily_traffic",
		"silver.daily_orders",
		"silver.daily_funnel",
		"silver.daily_channel_attribution",
	}

	for _, v := range silverViews {
		if _, err := pool.Exec(ctx, fmt.Sprintf("REFRESH MATERIALIZED VIEW CONCURRENTLY %s", v)); err != nil {
			slog.Error("refresh matview", "view", v, "error", err)
		}
	}

	goldViews := []string{
		"gold.dashboard_kpi",
		"gold.top_products",
	}

	for _, v := range goldViews {
		if _, err := pool.Exec(ctx, fmt.Sprintf("REFRESH MATERIALIZED VIEW CONCURRENTLY %s", v)); err != nil {
			slog.Error("refresh matview", "view", v, "error", err)
		}
	}

	slog.Info("matviews refreshed", "duration_ms", time.Since(start).Milliseconds())
}

// EnsurePartitions creates monthly partitions for the next 3 months
// and drops partitions older than the retention period.
func EnsurePartitions(ctx context.Context, pool *pgxpool.Pool, retention time.Duration) {
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

	// Drop old partitions (older than retention)
	cutoff := now.Add(-retention)
	cutoffMonth := time.Date(cutoff.Year(), cutoff.Month(), 1, 0, 0, 0, 0, time.UTC)

	// Check for partitions older than cutoff
	rows, err := pool.Query(ctx, `
		SELECT inhrelid::regclass::text
		FROM pg_inherits
		WHERE inhparent = 'bronze.events'::regclass
		ORDER BY 1
	`)
	if err != nil {
		slog.Error("list partitions", "error", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var partName string
		if err := rows.Scan(&partName); err != nil {
			continue
		}
		// Parse partition date from name like bronze.events_2026_01
		var year, month int
		if _, err := fmt.Sscanf(partName, "bronze.events_%d_%d", &year, &month); err != nil {
			continue
		}
		partDate := time.Date(year, time.Month(month), 1, 0, 0, 0, 0, time.UTC)
		if partDate.Before(cutoffMonth) {
			sql := fmt.Sprintf("DROP TABLE IF EXISTS %s", partName)
			if _, err := pool.Exec(ctx, sql); err != nil {
				slog.Error("drop partition", "name", partName, "error", err)
			} else {
				slog.Info("dropped old partition", "name", partName)
			}
		}
	}
}

// StartMaintenanceLoop runs periodic maintenance tasks.
func StartMaintenanceLoop(ctx context.Context, pool *pgxpool.Pool, refreshInterval time.Duration, retention time.Duration) {
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
		EnsurePartitions(ctx, pool, retention)

		for {
			select {
			case <-ctx.Done():
				return
			case <-refreshTicker.C:
				RefreshMatviews(ctx, pool)
			case <-partitionTicker.C:
				EnsurePartitions(ctx, pool, retention)
			}
		}
	}()

	slog.Info("maintenance loop started", "refresh_interval", refreshInterval, "retention", retention)
}
