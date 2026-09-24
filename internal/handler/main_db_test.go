//go:build dbtest

package handler

// Обвязка тестов с базой. Запуск:
//
//	GEO_TEST_DB=postgres://postgres:test@host:5432/postgres?sslmode=disable \
//	  go test -tags dbtest ./internal/handler/
//
// TestMain создаёт на этом сервере СВЕЖУЮ базу, накатывает migrations/*.sql дважды
// подряд (как entrypoint.sh на двух стартах подряд) и удаляет базу после прогона.
// Прод-базу тесты не трогают: им нужен только адрес временного сервера.

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/merfy/analytics-collector/internal/db"
)

var (
	testPool *pgxpool.Pool
	// migrationRuns — ошибки каждого из двух прогонов миграций, по файлам.
	migrationRuns [2][]string
)

func TestMain(m *testing.M) {
	dsn := os.Getenv("GEO_TEST_DB")
	if dsn == "" {
		os.Exit(m.Run()) // тесты с базой пропустят себя сами
	}
	os.Exit(runWithFreshDB(dsn, m))
}

func runWithFreshDB(dsn string, m *testing.M) int {
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		fmt.Fprintln(os.Stderr, "connect admin:", err)
		return 1
	}
	defer admin.Close()

	name := fmt.Sprintf("ac_p2_test_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		fmt.Fprintln(os.Stderr, "create database:", err)
		return 1
	}
	defer admin.Exec(ctx, "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")

	u, err := url.Parse(dsn)
	if err != nil {
		fmt.Fprintln(os.Stderr, "parse dsn:", err)
		return 1
	}
	u.Path = "/" + name
	cfg, err := pgxpool.ParseConfig(u.String())
	if err != nil {
		fmt.Fprintln(os.Stderr, "parse test dsn:", err)
		return 1
	}
	// Сутки в представлениях режет date_trunc('day', …) по поясу сессии базы; сутки отчётов
	// в Go — UTC. В тестах пояс сессии задаём явно, чтобы не зависеть от настроек сервера.
	cfg.ConnConfig.RuntimeParams["timezone"] = "UTC"
	testPool, err = pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, "connect test db:", err)
		return 1
	}
	defer testPool.Close()

	for run := range migrationRuns {
		migrationRuns[run] = applyMigrations(ctx, testPool)
	}

	// Партиции: месячные — как в проде, плюс партиция по умолчанию, чтобы тестовые
	// события любой даты (created_at) было куда писать.
	db.EnsurePartitions(ctx, testPool, db.RetentionMonths)
	if _, err := testPool.Exec(ctx,
		"CREATE TABLE IF NOT EXISTS bronze.events_test_default PARTITION OF bronze.events DEFAULT"); err != nil {
		fmt.Fprintln(os.Stderr, "default partition:", err)
		return 1
	}

	return m.Run()
}

// applyMigrations накатывает migrations/*.sql по порядку имён, каждый файл целиком
// (без аргументов pgx шлёт его простым протоколом — все операторы файла за раз).
// Как и entrypoint.sh, ошибка файла не останавливает следующие — она записывается.
func applyMigrations(ctx context.Context, pool *pgxpool.Pool) []string {
	files, err := filepath.Glob(filepath.Join("..", "..", "migrations", "*.sql"))
	if err != nil || len(files) == 0 {
		return []string{fmt.Sprintf("migrations not found: %v", err)}
	}
	sort.Strings(files)

	var errs []string
	for _, f := range files {
		sql, err := os.ReadFile(f)
		if err != nil {
			errs = append(errs, fmt.Sprintf("%s: %v", filepath.Base(f), err))
			continue
		}
		if _, err := pool.Exec(ctx, string(sql)); err != nil {
			errs = append(errs, fmt.Sprintf("%s: %v", filepath.Base(f), err))
		}
	}
	return errs
}

// needDB пропускает тест без GEO_TEST_DB и отдаёт пул свежей базы.
func needDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	if testPool == nil {
		t.Skip("GEO_TEST_DB not set")
	}
	return testPool
}
