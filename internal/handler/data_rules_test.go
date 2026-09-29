package handler

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Посетители в суточном представлении (017) считаются по тем же событиям визита, что и в Go
// (visitEventTypes): иначе график по дням и итог за период снова разойдутся.
func TestDailyTrafficVisitorsUseVisitEvents(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "migrations", "017_event_time_human_traffic.sql"))
	if err != nil {
		t.Fatal(err)
	}
	want := "COUNT(DISTINCT visitor_id) FILTER (WHERE event_type IN " + visitEventTypes + ") AS unique_visitors"
	if !strings.Contains(string(raw), want) {
		t.Errorf("silver.daily_traffic: нет %q", want)
	}
}

// Все запросы трафика берут посетителей по событиям визита.
func TestVisitorQueriesUseVisitEvents(t *testing.T) {
	queries := map[string]string{
		"итог за период":   periodTrafficSQL,
		"лидерборд":        periodVisitorsByShopSQL,
		"Главная по часам": hourlyTrafficSQL,
	}
	for name, q := range queries {
		if !strings.Contains(q, visitEvent("e")) {
			t.Errorf("%s: посетители без условия визита", name)
		}
	}
}
