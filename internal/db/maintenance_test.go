package db

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"testing"
	"time"
)

// monthlyPartitions — имена партиций с from по to включительно, как их отдаёт pg_inherits.
func monthlyPartitions(from, to time.Time) []string {
	var names []string
	for m := from; !m.After(to); m = m.AddDate(0, 1, 0) {
		names = append(names, fmt.Sprintf("bronze.events_%d_%02d", m.Year(), m.Month()))
	}
	return names
}

func month(y int, m time.Month) time.Time { return time.Date(y, m, 1, 0, 0, 0, 0, time.UTC) }

// Правило хранения из ресерча: сырые события — 13 месяцев (год + месяц на «год к году»).
func TestRetentionIsThirteenMonths(t *testing.T) {
	if RetentionMonths != 13 {
		t.Fatalf("RetentionMonths = %d; правило хранения — 13 месяцев", RetentionMonths)
	}
}

// Сырые события хранятся 13 месяцев. 1 октября 2026 при старом сроке (30 дней) удалялся
// весь август — теперь удаляется только то, что целиком старше 13 месяцев.
func TestPartitionsToDrop_KeepsThirteenMonths(t *testing.T) {
	names := monthlyPartitions(month(2025, time.June), month(2027, time.January))

	cases := []struct {
		name string
		now  time.Time
		want []string
	}{
		{
			name: "1 октября 2026: август 2026 на месте, удаляется только старше 01.09.2025",
			now:  time.Date(2026, 10, 1, 3, 0, 0, 0, time.UTC),
			want: []string{"bronze.events_2025_06", "bronze.events_2025_07", "bronze.events_2025_08"},
		},
		{
			name: "середина месяца: партиция, где часть строк моложе срока, остаётся целиком",
			now:  time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC),
			want: []string{"bronze.events_2025_06", "bronze.events_2025_07"},
		},
		{
			name: "ровно на границе: конец партиции = граница → удаляется",
			now:  time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC),
			want: []string{"bronze.events_2025_06", "bronze.events_2025_07"},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := partitionsToDrop(names, c.now, RetentionMonths)
			if !reflect.DeepEqual(got, c.want) {
				t.Fatalf("drop = %v\nwant   %v", got, c.want)
			}
		})
	}
}

// Всё, что моложе 13 месяцев, после чистки остаётся: оставшиеся партиции покрывают
// каждую дату от now − 13 месяцев до now.
func TestPartitionsToDrop_RemainingCoverRetentionWindow(t *testing.T) {
	now := time.Date(2026, 10, 1, 3, 0, 0, 0, time.UTC)
	names := monthlyPartitions(month(2025, time.January), month(2026, time.December))
	dropped := map[string]bool{}
	for _, n := range partitionsToDrop(names, now, RetentionMonths) {
		dropped[n] = true
	}

	for d := now.AddDate(0, -RetentionMonths, 0); d.Before(now); d = d.AddDate(0, 0, 1) {
		name := fmt.Sprintf("bronze.events_%d_%02d", d.Year(), d.Month())
		if dropped[name] {
			t.Fatalf("дата %s внутри срока хранения, а её партиция %s удаляется", d.Format(time.DateOnly), name)
		}
	}
}

// Партиции не месячного вида (по умолчанию, чужие) не трогаются.
func TestPartitionsToDrop_IgnoresForeignNames(t *testing.T) {
	names := []string{"bronze.events_default", "bronze.events_test_default", "public.events_2020_01", "bronze.events_2020_13"}
	if got := partitionsToDrop(names, time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC), RetentionMonths); len(got) != 0 {
		t.Fatalf("drop = %v; want none", got)
	}
}

// Окна представлений в миграции 017 — тот же срок, что хранение сырых событий:
// представление не может видеть дальше, чем хранится, и не должно видеть меньше.
func TestMigration017WindowsMatchRetention(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "migrations", "017_event_time_human_traffic.sql"))
	if err != nil {
		t.Fatalf("read migration 017: %v", err)
	}
	windows := regexp.MustCompile(`(?i)interval\s+'(\d+)\s+months?'`).FindAllStringSubmatch(string(raw), -1)
	if len(windows) == 0 {
		t.Fatal("в 017 нет окна interval 'N months'")
	}
	for _, m := range windows {
		if n, _ := strconv.Atoi(m[1]); n != RetentionMonths {
			t.Errorf("017: окно %d мес., а RetentionMonths = %d", n, RetentionMonths)
		}
	}
}
