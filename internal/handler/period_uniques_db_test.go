//go:build dbtest

package handler

// Правило 1 (spec 114): итоги трафика за период — уникальные за весь период, а не сумма
// суточных уников. Отдельный набор: владелец ещё решает про правило 1, и его можно выкинуть
// вместе с period_uniques.go одним откатом. Запуск — см. main_db_test.go.

import "testing"

// Посетитель, заходивший три дня подряд, за «30 дней» — один посетитель (сумма суточных давала 3).
// Визит через полночь — один визит (было 2). Везде, где итог периода складывался из суточных.
func TestRule1_UniqueOverWholePeriod_DB(t *testing.T) {
	setClock(t, testNow())
	const shop = "shop-u"
	seed(t,
		// один человек три дня подряд, во второй день — покупка
		pageView(shop, "u-s1", "u-v1", utc(-3, 10, 0)),
		pageView(shop, "u-s2", "u-v1", utc(-2, 10, 0)),
		ev{shop: shop, session: "u-s2", visitor: "u-v1", typ: "purchase", at: utc(-2, 10, 5), order: "u-o1", total: 500},
		pageView(shop, "u-s3", "u-v1", utc(-1, 10, 0)),
		// визит через полночь с корзиной по обе стороны
		pageView(shop, "u-s4", "u-v2", utc(-5, 23, 50)),
		ev{shop: shop, session: "u-s4", visitor: "u-v2", typ: "add_to_cart", at: utc(-5, 23, 55)},
		pageView(shop, "u-s4", "u-v2", utc(-4, 0, 10)),
		ev{shop: shop, session: "u-s4", visitor: "u-v2", typ: "add_to_cart", at: utc(-4, 0, 15)},
		// робот два дня подряд — не посетитель ни в одном итоге
		ev{shop: shop, session: "u-b1", visitor: "u-vb", typ: "page_view", page: "/", at: utc(-2, 11, 0), traffic: "bot"},
		ev{shop: shop, session: "u-b2", visitor: "u-vb", typ: "page_view", page: "/", at: utc(-1, 11, 0), traffic: "bot"},
		// прошлый период: один человек два дня подряд
		pageView(shop, "u-s5", "u-v3", utc(-41, 10, 0)),
		pageView(shop, "u-s6", "u-v3", utc(-40, 10, 0)),
	)
	const shop30d = `{"shopId":"shop-u","period":"30d"}`
	const all30d = `{"period":"30d"}`

	t.Run("dashboard", func(t *testing.T) {
		r := call[DashboardResponse](t, HandleDashboard, shop30d)
		if k := r.KPI; k.UniqueVisitors != 2 || k.UniqueSessions != 4 || k.PageViews != 5 {
			t.Errorf("посетители/визиты/просмотры = %d/%d/%d; want 2/4/5", k.UniqueVisitors, k.UniqueSessions, k.PageViews)
		}
		// формула прежняя (заказы дней с посетителями ÷ посетители), посетители — уникальные: 1 ÷ 2
		if r.KPI.ConversionRate != 50 {
			t.Errorf("конверсия = %v; want 50", r.KPI.ConversionRate)
		}
		if r.KPIPrev.UniqueVisitors != 1 || r.KPIPrev.UniqueSessions != 2 {
			t.Errorf("прошлый период: посетители/визиты = %d/%d; want 1/2", r.KPIPrev.UniqueVisitors, r.KPIPrev.UniqueSessions)
		}
	})
	t.Run("global dashboard", func(t *testing.T) {
		k := call[DashboardResponse](t, HandleGlobalDashboard, all30d).KPI
		if k.UniqueVisitors != 2 || k.UniqueSessions != 4 || k.ConversionRate != 50 {
			t.Errorf("платформа: %+v; want 2 посетителя, 4 визита, конверсия 50", k)
		}
	})
	t.Run("traffic", func(t *testing.T) {
		s := call[TrafficResponse](t, HandleTraffic, shop30d).Summary
		if s.TotalVisitors != 2 || s.TotalSessions != 4 || s.TotalPageViews != 5 {
			t.Errorf("summary = %+v; want 2/4/5", s)
		}
	})
	t.Run("funnel", func(t *testing.T) {
		for _, h := range []struct {
			name    string
			handler func(t *testing.T) FunnelResponse
		}{
			{"shop", func(t *testing.T) FunnelResponse { return call[FunnelResponse](t, HandleFunnel, shop30d) }},
			{"global", func(t *testing.T) FunnelResponse { return call[FunnelResponse](t, HandleGlobalFunnel, all30d) }},
		} {
			st := h.handler(t).Stages
			want := map[string]int64{"visits": 2, "add_to_cart": 1, "purchases": 1}
			for name, n := range want {
				if got := stage(t, st, name).Count; got != n {
					t.Errorf("%s: шаг %s = %d; want %d", h.name, name, got, n)
				}
			}
		}
	})
	t.Run("top shops", func(t *testing.T) {
		r := call[TopShopsResponse](t, HandleGlobalTopShops, all30d)
		if len(r.Shops) != 1 || r.Shops[0].UniqueVisitors != 2 {
			t.Errorf("лидерборд = %+v; want shop-u с 2 посетителями", r.Shops)
		}
	})
	t.Run("by location", func(t *testing.T) {
		for _, payload := range []string{shop30d, all30d} {
			if got := call[ByLocationResponse](t, HandleGlobalByLocation, payload).TotalSessions; got != 4 {
				t.Errorf("%s: total_sessions = %d; want 4 (визит через полночь — один)", payload, got)
			}
		}
		if got := call[ByLocationResponse](t, HandleByLocation, shop30d).TotalSessions; got != 4 {
			t.Errorf("магазин: total_sessions = %d; want 4", got)
		}
	})
}
