//go:build dbtest

package handler

import (
	"context"
	"testing"
)

// «Сессии по локациям» на настоящей базе (данные кладёт сам тест, запуск — см. main_db_test.go),
// с 28.09 — люди по первому визиту; здесь у каждого человека один визит:
// визиты — только люди, гео визита — по его событиям; заказ — в гео визита покупки, заказ без
// визита человека (сервис заказов, робот) — в «Не определено»; деньги — все.
func TestByLocationHandler_DB(t *testing.T) {
	setClock(t, testNow())
	const shop = "shopA"
	geo := func(e ev, country, subject, city string) ev {
		e.country, e.subject, e.city = country, subject, city
		return e
	}
	seed(t,
		geo(pageView(shop, "l-m1", "l-v1", utc(-1, 10, 0)), "RU", "Москва", "Москва"),
		geo(pageView(shop, "l-m2", "l-v2", utc(-1, 10, 30)), "RU", "Москва", "Москва"),
		geo(pageView(shop, "l-p1", "l-v3", utc(-1, 11, 0)), "RU", "Санкт-Петербург", "Санкт-Петербург"),
		geo(pageView(shop, "l-k1", "l-v4", utc(-1, 12, 0)), "RU", SubjCrimeaLabel, "Симферополь"),
		pageView(shop, "l-n1", "l-v5", utc(-1, 13, 0)), // гео не определилось
		// робот и свои в гео не попадают
		geo(ev{shop: shop, session: "l-b1", visitor: "l-vb", typ: "page_view", page: "/", at: utc(-1, 14, 0), traffic: "bot"}, "RU", "Москва", "Москва"),
		geo(ev{shop: shop, session: "l-i1", visitor: "l-vi", typ: "page_view", page: "/", at: utc(-1, 15, 0), traffic: "internal"}, "NL", "Limburg", "Eygelshoven"),
		// покупка из визита в Москве
		ev{shop: shop, session: "l-m1", visitor: "l-v1", typ: "purchase", at: utc(-1, 10, 20), order: "l-o1", total: 100},
		// покупка от сервиса заказов: у события гео — IP сервера, его не берём → «Не определено»
		geo(ev{shop: shop, session: "order-l-o2", visitor: "server-l-o2", typ: "purchase", at: utc(-1, 16, 0), order: "l-o2", total: 200}, "DE", "Hessen", "Frankfurt"),
	)

	resp := call[ByLocationResponse](t, HandleGlobalByLocation, `{"period":"30d"}`)
	if resp.TotalPeople != 5 {
		t.Errorf("total_sessions = %d; want 5", resp.TotalPeople)
	}
	want := map[string]struct{ sessions, orders int64 }{
		"Москва":          {2, 1},
		"Санкт-Петербург": {1, 0},
		SubjCrimeaLabel:   {1, 0},
		"":                {1, 1}, // «Не определено»: визит без гео + заказ сервиса
	}
	var shareSum float64
	for _, r := range resp.Rows {
		shareSum += r.Share
		if r.City != "" {
			t.Errorf("уровень субъектов, а город %q", r.City)
		}
		w, ok := want[r.Subject]
		if !ok {
			t.Errorf("лишняя строка: %+v", r)
			continue
		}
		if r.People != w.sessions || r.Orders != w.orders {
			t.Errorf("%q: люди/заказы = %d/%d; want %d/%d", r.Subject, r.People, r.Orders, w.sessions, w.orders)
		}
		delete(want, r.Subject)
	}
	if len(want) != 0 {
		t.Errorf("нет строк: %v", want)
	}
	if shareSum < 99.95 || shareSum > 100.05 {
		t.Errorf("sum(share) = %v; want ≈100", shareSum)
	}

	// Страны: RU и «не определено».
	for _, r := range call[ByLocationResponse](t, HandleGlobalByLocation, `{"period":"30d","level":"country"}`).Rows {
		if r.Subject != "" || r.City != "" {
			t.Errorf("уровень стран, а субъект/город заполнены: %+v", r)
		}
		if r.Country == "RU" && r.People != 4 {
			t.Errorf("RU: людей %d; want 4", r.People)
		}
	}

	// Пустой период → rows [] (не null), total 0.
	eResp := call[ByLocationResponse](t, HandleGlobalByLocation, `{"period":"custom","from":"2000-01-01","to":"2000-01-02"}`)
	if eResp.Rows == nil || len(eResp.Rows) != 0 || eResp.TotalPeople != 0 {
		t.Errorf("пустой период: rows=%v total=%d; want []/0", eResp.Rows, eResp.TotalPeople)
	}

	// Магазинный вызов требует shopId и фильтрует по нему.
	if _, err := HandleByLocation(context.Background(), needDB(t), []byte(`{"period":"30d"}`)); err == nil {
		t.Error("HandleByLocation без shopId должен вернуть ошибку")
	}
	if got := call[ByLocationResponse](t, HandleByLocation, `{"period":"30d","shopId":"shopA"}`).TotalPeople; got != 5 {
		t.Errorf("магазин: total_sessions = %d; want 5", got)
	}
	if got := call[ByLocationResponse](t, HandleByLocation, `{"period":"30d","shopId":"nope"}`).TotalPeople; got != 0 {
		t.Errorf("чужой магазин: total_sessions = %d; want 0", got)
	}
}
