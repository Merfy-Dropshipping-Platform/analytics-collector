//go:build dbtest

package handler

// Посещаемость по часам на настоящей базе: SQL режет часы по местному времени пояса, уники —
// за час и за всё окно, роботы и чужие магазины не считаются. Запуск — см. main_db_test.go.

import "testing"

func TestHourlyTraffic_DB(t *testing.T) {
	// «Сейчас» — сегодня 12:00 UTC: 15:00 по Москве, 17:30 по Калькутте.
	setClock(t, testNow())
	const shop = "shop-h"
	seed(t,
		// Москва: окно 06:00…15:00 = 03:00…13:00 UTC
		pageView(shop, "h-s1", "h-v1", utc(0, 3, 10)),  // 06:10 — первый час
		pageView(shop, "h-s2", "h-v1", utc(0, 3, 50)),  // тот же человек в тот же час
		pageView(shop, "h-s3", "h-v2", utc(0, 3, 55)),  // второй человек в первый час
		pageView(shop, "h-s4", "h-v1", utc(0, 11, 20)), // 14:20 — девятый час (Калькутта: 16:50)
		pageView(shop, "h-s5", "h-v3", utc(0, 11, 40)), // 14:40 — девятый час (Калькутта: 17:10)
		ev{shop: shop, session: "h-b1", visitor: "h-vb", typ: "page_view", page: "/", at: utc(0, 4, 0), traffic: "bot"},
		pageView("other-shop", "h-o1", "h-vo", utc(0, 5, 0)),
		// прошлые 10 часов по Москве: 20:00 вчера … 06:00 сегодня
		pageView(shop, "h-s9", "h-v9", utc(0, 1, 0)),
		// раньше прошлого окна — нигде
		pageView(shop, "h-s0", "h-v0", utc(-1, 10, 0)),
	)

	t.Run("Москва", func(t *testing.T) {
		r := call[HourlyTrafficResponse](t, HandleHourlyTraffic, `{"shopId":"shop-h","hours":10,"tz":"Europe/Moscow"}`)
		labels := hourLabels(r.Points)
		if len(r.Points) != 10 || labels[0] != "06:00" || labels[9] != "15:00" {
			t.Fatalf("часы = %v; want 10 точек 06:00…15:00", labels)
		}
		got := map[string]int64{}
		for _, p := range r.Points {
			if p.Visitors > 0 {
				got[p.Hour[11:]] = p.Visitors
			}
		}
		want := map[string]int64{"06:00": 2, "14:00": 2}
		if len(got) != len(want) || got["06:00"] != 2 || got["14:00"] != 2 {
			t.Errorf("посетители по часам = %v; want %v", got, want)
		}
		if r.Visitors != 3 || r.VisitorsPrev != 1 {
			t.Errorf("итог окна / прошлого = %d / %d; want 3 / 1 (уники за окно, без робота и чужого магазина)", r.Visitors, r.VisitorsPrev)
		}
	})

	t.Run("Калькутта: 11:20 и 11:40 UTC — разные местные часы", func(t *testing.T) {
		r := call[HourlyTrafficResponse](t, HandleHourlyTraffic, `{"shopId":"shop-h","tz":"Asia/Kolkata"}`)
		got := map[string]int64{}
		for _, p := range r.Points {
			got[p.Hour[11:]] = p.Visitors
		}
		if got["16:00"] != 1 || got["17:00"] != 1 {
			t.Errorf("16:00 / 17:00 = %d / %d; want 1 / 1", got["16:00"], got["17:00"])
		}
		if r.TZ != "Asia/Kolkata" || r.Hours != 10 {
			t.Errorf("пояс / часов = %s / %d; want Asia/Kolkata / 10", r.TZ, r.Hours)
		}
	})
}
