package handler

import (
	"reflect"
	"testing"
	"time"
)

// Окно карточки «Посещаемость» на главной: 10 целых местных часов, последний — текущий.

func mustZone(t *testing.T, name string) *time.Location {
	t.Helper()
	loc, err := time.LoadLocation(name)
	if err != nil {
		t.Fatalf("zone %s: %v", name, err)
	}
	return loc
}

func hourLabels(points []HourlyTrafficPoint) []string {
	labels := make([]string, len(points))
	for i, p := range points {
		labels[i] = p.Hour[11:]
	}
	return labels
}

func TestHourlyWindow_LocalHoursEndWithCurrentHour(t *testing.T) {
	// 06:40 UTC = 09:40 Москвы → окно 00:00…09:00 по Москве, конец — 10:00.
	now := time.Date(2026, 9, 27, 6, 40, 0, 0, time.UTC)
	moscow := mustZone(t, "Europe/Moscow")

	start, end := hourlyWindow(now, moscow, 10)
	points := buildHourlyPoints(start, 10, moscow, nil)

	if got := start.In(moscow).Format("2006-01-02 15:04"); got != "2026-09-27 00:00" {
		t.Errorf("начало окна = %s; want 2026-09-27 00:00 (Москва)", got)
	}
	if got := end.Sub(start); got != 10*time.Hour {
		t.Errorf("длина окна = %v; want 10h", got)
	}
	want := []string{"00:00", "01:00", "02:00", "03:00", "04:00", "05:00", "06:00", "07:00", "08:00", "09:00"}
	if got := hourLabels(points); !reflect.DeepEqual(got, want) {
		t.Errorf("часы = %v; want %v", got, want)
	}
}

func TestHourlyWindow_HalfHourZoneCutsByLocalHours(t *testing.T) {
	// Калькутта +5:30: граница местного часа — :30 по UTC. 06:40 UTC = 12:10 местного.
	now := time.Date(2026, 9, 27, 6, 40, 0, 0, time.UTC)
	kolkata := mustZone(t, "Asia/Kolkata")

	start, _ := hourlyWindow(now, kolkata, 10)

	if got := start.In(kolkata).Format("15:04"); got != "03:00" {
		t.Errorf("начало окна по местному = %s; want 03:00", got)
	}
	if got := start.UTC().Format("15:04"); got != "21:30" {
		t.Errorf("начало окна по UTC = %s; want 21:30 (местный час, а не час UTC)", got)
	}
	if got := hourLabels(buildHourlyPoints(start, 10, kolkata, nil)); got[9] != "12:00" {
		t.Errorf("последний час = %s; want 12:00 (текущий местный)", got[9])
	}
}

func TestHourlyPoints_ZeroFillAndOrder(t *testing.T) {
	moscow := mustZone(t, "Europe/Moscow")
	start := time.Date(2026, 9, 26, 23, 0, 0, 0, moscow)

	points := buildHourlyPoints(start, 4, moscow, map[int]int64{1: 7, 3: 2})

	want := []HourlyTrafficPoint{
		{Hour: "2026-09-26T23:00", Visitors: 0},
		{Hour: "2026-09-27T00:00", Visitors: 7},
		{Hour: "2026-09-27T01:00", Visitors: 0},
		{Hour: "2026-09-27T02:00", Visitors: 2},
	}
	if !reflect.DeepEqual(points, want) {
		t.Errorf("точки = %+v; want %+v", points, want)
	}
}

func TestHourlyRequestDefaults(t *testing.T) {
	cases := []struct {
		name  string
		hours int
		want  int
	}{
		{"не задано", 0, 10},
		{"отрицательное", -3, 10},
		{"как просили", 10, 10},
		{"больше предела", 500, 48},
	}
	for _, c := range cases {
		if got := clampHours(c.hours); got != c.want {
			t.Errorf("%s: clampHours(%d) = %d; want %d", c.name, c.hours, got, c.want)
		}
	}

	for _, name := range []string{"", "Not/AZone", "Moscow"} {
		if _, tz := resolveZone(name); tz != "Europe/Moscow" {
			t.Errorf("пояс %q → %s; want Europe/Moscow", name, tz)
		}
	}
	if _, tz := resolveZone("Asia/Vladivostok"); tz != "Asia/Vladivostok" {
		t.Errorf("пояс Asia/Vladivostok → %s", tz)
	}
}
