//go:build dbtest

package handler

// Качество данных аналитики (spec 114, P2): трафик — только люди, деньги — все; день и окна —
// по времени события, а не записи в базу; миграции 001…017 дважды подряд дают новые определения.
// Формулы и границы периодов — прежние (сутки UTC, сумма суточных, конверсия заказы ÷ посетители).

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/merfy/analytics-collector/internal/db"
)

func stage(t *testing.T, stages []FunnelStage, name string) FunnelStage {
	t.Helper()
	for _, s := range stages {
		if s.Name == name {
			return s
		}
	}
	t.Fatalf("нет шага воронки %q в %+v", name, stages)
	return FunnelStage{}
}

func seriesByDay(ts []DashboardTimeSeries) map[string]DashboardTimeSeries {
	out := map[string]DashboardTimeSeries{}
	for _, e := range ts {
		out[e.Day] = e
	}
	return out
}

// Роботы и свои не входят в посетителей, визиты, просмотры, воронку, каналы, гео и лучшие
// страницы; деньги их покупок входят. День, где был только заказ от робота, деньги не теряет.
func TestTrafficTypes_OnlyHumansInTraffic_MoneyAll_DB(t *testing.T) {
	setClock(t, testNow())
	const shop = "shop-t"
	moscow := func(e ev) ev { e.country, e.subject, e.city = "RU", "Москва", "Москва"; return e }
	seed(t,
		// люди: 4 визита, в первом — корзина и покупка
		moscow(ev{shop: shop, session: "t-h1", visitor: "t-vh1", typ: "session_start", at: utc(-1, 10, 0)}),
		moscow(ev{shop: shop, session: "t-h1", visitor: "t-vh1", typ: "page_view", page: "/", at: utc(-1, 10, 0)}),
		ev{shop: shop, session: "t-h1", visitor: "t-vh1", typ: "add_to_cart", at: utc(-1, 10, 5), product: "p1", price: 1000},
		ev{shop: shop, session: "t-h1", visitor: "t-vh1", typ: "purchase", at: utc(-1, 10, 10), order: "t-o1", total: 1000},
		ev{shop: shop, session: "t-h2", visitor: "t-vh2", typ: "page_view", page: "/", at: utc(-1, 11, 0), country: "RU", subject: "Санкт-Петербург", city: "Санкт-Петербург"},
		pageView(shop, "t-h3", "t-vh3", utc(-1, 12, 0)),
		pageView(shop, "t-h4", "t-vh4", utc(-1, 13, 0)),
		// робот проходит всю воронку и «покупает»
		moscow(ev{shop: shop, session: "t-b1", visitor: "t-vb", typ: "session_start", at: utc(-1, 14, 0), traffic: "bot"}),
		moscow(ev{shop: shop, session: "t-b1", visitor: "t-vb", typ: "page_view", page: "/", at: utc(-1, 14, 0), traffic: "bot"}),
		ev{shop: shop, session: "t-b1", visitor: "t-vb", typ: "page_view", page: "/checkout", at: utc(-1, 14, 1), traffic: "bot"},
		ev{shop: shop, session: "t-b1", visitor: "t-vb", typ: "add_to_cart", at: utc(-1, 14, 2), traffic: "bot"},
		ev{shop: shop, session: "t-b1", visitor: "t-vb", typ: "checkout_start", at: utc(-1, 14, 3), traffic: "bot"},
		ev{shop: shop, session: "t-b1", visitor: "t-vb", typ: "purchase", at: utc(-1, 14, 4), order: "t-ob", total: 5000, traffic: "bot"},
		// свои: владелец смотрит витрину через VPN
		ev{shop: shop, session: "t-i1", visitor: "t-vi", typ: "session_start", at: utc(-1, 15, 0), traffic: "internal", country: "NL", subject: "Limburg", city: "Eygelshoven"},
		ev{shop: shop, session: "t-i1", visitor: "t-vi", typ: "page_view", page: "/", at: utc(-1, 15, 0), traffic: "internal", country: "NL", subject: "Limburg", city: "Eygelshoven"},
		ev{shop: shop, session: "t-i1", visitor: "t-vi", typ: "page_view", page: "/", at: utc(-1, 15, 1), traffic: "internal", country: "NL", subject: "Limburg", city: "Eygelshoven"},
		// день без людей: только заказ, помеченный роботом (сервис заказов со «странной» подписью)
		ev{shop: shop, session: "order-t-late", visitor: "server-t-late", typ: "purchase", at: utc(-3, 12, 0), order: "t-late", total: 700, traffic: "bot"},
	)
	const payload = `{"shopId":"shop-t","period":"30d"}`

	t.Run("dashboard", func(t *testing.T) {
		k := call[DashboardResponse](t, HandleDashboard, payload).KPI
		if k.UniqueVisitors != 4 || k.UniqueSessions != 4 || k.PageViews != 4 {
			t.Errorf("посетители/визиты/просмотры = %d/%d/%d; want 4/4/4 (только люди)", k.UniqueVisitors, k.UniqueSessions, k.PageViews)
		}
		if k.TotalOrders != 3 || k.TotalRevenueCents != 6700 {
			t.Errorf("заказы/выручка = %d/%d; want 3/6700 (деньги робота тоже)", k.TotalOrders, k.TotalRevenueCents)
		}
		// формула прежняя: заказы дней с посетителями ÷ посетители = 2 ÷ 4
		if k.ConversionRate != 50 {
			t.Errorf("конверсия = %v; want 50", k.ConversionRate)
		}
	})
	t.Run("global dashboard", func(t *testing.T) {
		k := call[DashboardResponse](t, HandleGlobalDashboard, `{"period":"30d"}`).KPI
		if k.UniqueVisitors != 4 || k.TotalRevenueCents != 6700 {
			t.Errorf("платформа: посетители %d, выручка %d; want 4 и 6700", k.UniqueVisitors, k.TotalRevenueCents)
		}
	})
	t.Run("funnel", func(t *testing.T) {
		st := call[FunnelResponse](t, HandleFunnel, payload).Stages
		// шаги до оплаты — только люди и накопительно (28.09): визит t-h1 с покупкой есть и в «Готов к оплате»
		want := map[string]int64{"visits": 4, "add_to_cart": 1, "checkout_starts": 1, "purchases": 3}
		for name, n := range want {
			if got := stage(t, st, name).Count; got != n {
				t.Errorf("шаг %s = %d; want %d", name, got, n)
			}
		}
	})
	t.Run("traffic", func(t *testing.T) {
		r := call[TrafficResponse](t, HandleTraffic, payload)
		if r.Summary.TotalVisitors != 4 || r.Summary.TotalPageViews != 4 {
			t.Errorf("summary = %+v; want 4 посетителя, 4 просмотра", r.Summary)
		}
		if len(r.TopPages) != 1 || r.TopPages[0].PageURL != "/" || r.TopPages[0].Views != 4 {
			t.Errorf("лучшие страницы = %+v; want только «/» с 4 просмотрами людей", r.TopPages)
		}
		var refSessions int64
		for _, ref := range r.TopReferrers {
			refSessions += ref.Sessions
		}
		if refSessions != 4 {
			t.Errorf("сессии по источникам = %d; want 4", refSessions)
		}
	})
	t.Run("revenue", func(t *testing.T) {
		s := call[RevenueResponse](t, HandleRevenue, payload).Summary
		if s.TotalRevenueCents != 6700 || s.TotalOrders != 3 {
			t.Errorf("выручка = %+v; want 6700 / 3 заказа", s)
		}
	})
	t.Run("channels", func(t *testing.T) {
		var sessions, orders int64
		for _, c := range call[ChannelsResponse](t, HandleChannels, payload).Channels {
			sessions += c.Sessions
			orders += c.Orders
		}
		if sessions != 1 || orders != 3 {
			t.Errorf("каналы: сессии/заказы = %d/%d; want 1/3", sessions, orders)
		}
	})
	t.Run("by location", func(t *testing.T) {
		r := call[ByLocationResponse](t, HandleByLocation, payload)
		if r.TotalPeople != 4 {
			t.Errorf("total_sessions = %d; want 4", r.TotalPeople)
		}
		for _, row := range r.Rows {
			if row.Country == "NL" {
				t.Errorf("визиты своих попали в гео: %+v", row)
			}
			if row.Subject == "Москва" && (row.People != 1 || row.Orders != 1) {
				t.Errorf("Москва: %+v; want 1 визит (робот не считается), 1 заказ", row)
			}
			if row.Country == "" && row.Orders != 2 {
				t.Errorf("«Не определено»: %+v; want 2 заказа (робота и дня без людей)", row)
			}
		}
	})
}

// День — по времени события, а не записи в базу: событие 14-го, записанное 15-го
// (как при простое 13–15.09), остаётся в 14-м. Отрезки «24 часа» — тоже по времени события.
func TestEventDay_ByEventTimestampNotCreatedAt_DB(t *testing.T) {
	setClock(t, testNow())
	const shop = "shop-d"
	seed(t,
		ev{shop: shop, session: "d-s1", visitor: "d-v1", typ: "page_view", page: "/", at: utc(-3, 12, 0), created: utc(-2, 12, 0)},
		ev{shop: shop, session: "d-s1", visitor: "d-v1", typ: "purchase", at: utc(-3, 13, 0), created: utc(-2, 13, 0), order: "d-o1", total: 1000},
		// сегодня в 10:30, записано в 12:05
		ev{shop: shop, session: "d-s2", visitor: "d-v2", typ: "page_view", page: "/", at: utc(0, 10, 30), created: utc(0, 12, 5)},
	)
	late, next := dayOf(utc(-3, 0, 0)), dayOf(utc(-2, 0, 0))

	t.Run("dashboard days", func(t *testing.T) {
		byDay := seriesByDay(call[DashboardResponse](t, HandleDashboard, `{"shopId":"shop-d","period":"7d"}`).TimeSeries)
		if d := byDay[late]; d.Sessions != 1 || d.PageViews != 1 || d.RevenueCents != 1000 {
			t.Errorf("день события %s: %+v; want 1 визит, 1 просмотр, 1000", late, d)
		}
		if d := byDay[next]; d.Sessions != 0 || d.RevenueCents != 0 {
			t.Errorf("день записи %s: %+v; want пусто", next, d)
		}
	})
	t.Run("traffic days", func(t *testing.T) {
		for _, e := range call[TrafficResponse](t, HandleTraffic, `{"shopId":"shop-d","period":"7d"}`).TimeSeries {
			if e.Day == late && e.Sessions != 1 || e.Day == next && e.Sessions != 0 {
				t.Errorf("день %s: визитов %d", e.Day, e.Sessions)
			}
		}
	})
	t.Run("revenue days", func(t *testing.T) {
		for _, e := range call[RevenueResponse](t, HandleRevenue, `{"shopId":"shop-d","period":"7d"}`).TimeSeries {
			if e.Day == late && e.RevenueCents != 1000 || e.Day == next && e.RevenueCents != 0 {
				t.Errorf("день %s: выручка %d", e.Day, e.RevenueCents)
			}
		}
	})
	t.Run("24h buckets", func(t *testing.T) {
		byBucket := seriesByDay(call[DashboardResponse](t, HandleDashboard, `{"shopId":"shop-d","period":"24h"}`).TimeSeries)
		todayDay := dayOf(utc(0, 0, 0))
		if b := byBucket[todayDay+"T10:00"]; b.PageViews != 1 {
			t.Errorf("отрезок 10:00: %+v; want 1 просмотр (время события 10:30)", b)
		}
		if b := byBucket[todayDay+"T12:00"]; b.PageViews != 0 {
			t.Errorf("отрезок 12:00: %+v; want пусто (12:05 — время записи)", b)
		}
	})
}

// Условия на время записи только отсекают партиции: событие прошлого дня, записанное на следующий
// день, и даже через ~35 часов (простой 13–15.09), остаётся в своём дне и в своих отрезках.
func TestEventWindow_LateWritesStayInPastWindow_DB(t *testing.T) {
	setClock(t, testNow())
	const shop = "shop-w"
	seed(t,
		// записано на следующий день
		ev{shop: shop, session: "w-s1", visitor: "w-v1", typ: "page_view", page: "/", at: utc(-3, 22, 10), created: utc(-2, 9, 0)},
		// записано через 35 часов — как после простоя 13–15.09
		ev{shop: shop, session: "w-s2", visitor: "w-v2", typ: "page_view", page: "/", at: utc(-3, 22, 20), created: utc(-3, 22, 20).Add(35 * time.Hour)},
	)
	day := dayOf(utc(-3, 0, 0))
	payload := fmt.Sprintf(`{"shopId":%q,"period":"custom","from":%q,"to":%q}`, shop, day, day)

	byBucket := seriesByDay(call[DashboardResponse](t, HandleDashboard, payload).TimeSeries)
	if b := byBucket[day+"T22:00"]; b.PageViews != 2 || b.Visitors != 2 {
		t.Errorf("отрезок 22:00 дня %s: %+v; want 2 просмотра (записи на следующий день и через 35 ч)", day, b)
	}
	pages := call[TrafficResponse](t, HandleTraffic, payload).TopPages
	if len(pages) != 1 || pages[0].Views != 2 {
		t.Errorf("лучшие страницы дня %s: %+v; want «/» с 2 просмотрами", day, pages)
	}
}

// «24 часа» с базы (без представлений): робот и свои не входят в отрезки и итог.
func TestLast24h_OnlyHumans_DB(t *testing.T) {
	setClock(t, testNow())
	seed(t,
		pageView("shop-h", "h-s1", "h-v1", utc(0, 10, 10)),
		ev{shop: "shop-h", session: "h-b", visitor: "h-vb", typ: "page_view", page: "/", at: utc(0, 10, 20), traffic: "bot"},
		ev{shop: "shop-h", session: "h-i", visitor: "h-vi", typ: "page_view", page: "/", at: utc(0, 10, 30), traffic: "internal"},
		ev{shop: "shop-h", session: "order-h1", visitor: "server-h1", typ: "purchase", at: utc(0, 8, 0), order: "h1", total: 700, traffic: "bot"},
	)
	todayDay := dayOf(utc(0, 0, 0))
	check := func(t *testing.T, r DashboardResponse) {
		t.Helper()
		byBucket := seriesByDay(r.TimeSeries)
		if b := byBucket[todayDay+"T10:00"]; b.Visitors != 1 || b.PageViews != 1 {
			t.Errorf("отрезок 10:00: %+v; want 1 посетитель (робот и свои не считаются)", b)
		}
		if b := byBucket[todayDay+"T08:00"]; b.Orders != 1 || b.RevenueCents != 700 {
			t.Errorf("отрезок 08:00: %+v; want заказ робота на 700 (деньги — все)", b)
		}
	}
	t.Run("shop", func(t *testing.T) {
		check(t, call[DashboardResponse](t, HandleDashboard, `{"shopId":"shop-h","period":"24h"}`))
	})
	t.Run("global", func(t *testing.T) {
		check(t, call[DashboardResponse](t, HandleGlobalDashboard, `{"period":"24h"}`))
	})
}

// «Топ товаров» — окно 30 дней по времени покупки; покупка робота — деньги, входит.
func TestTopProducts_ThirtyDaysByEventTime_DB(t *testing.T) {
	setClock(t, testNow())
	seed(t,
		ev{shop: "shop-tp", session: "order-tp1", visitor: "server-tp1", typ: "purchase", at: utc(-1, 12, 0), order: "tp1", total: 1000, product: "A", price: 1000, traffic: "bot"},
		// покупка 40 дней назад, записанная вчера, — вне 30 дней
		ev{shop: "shop-tp", session: "order-tp2", visitor: "server-tp2", typ: "purchase", at: utc(-40, 12, 0), created: utc(-1, 12, 0), order: "tp2", total: 2000, product: "B", price: 2000},
	)
	ids := func(r TopProductsResponse) string {
		var out []string
		for _, p := range r.Products {
			out = append(out, p.ProductID)
		}
		sort.Strings(out)
		return strings.Join(out, ",")
	}
	if got := ids(call[TopProductsResponse](t, HandleTopProducts, `{"shopId":"shop-tp","period":"30d"}`)); got != "A" {
		t.Errorf("магазин: товары %q; want A", got)
	}
	if got := ids(call[TopProductsResponse](t, HandleGlobalTopProducts, `{"period":"30d"}`)); got != "A" {
		t.Errorf("платформа: товары %q; want A", got)
	}
}

// Возвращающиеся покупатели — по времени покупки, а не записи в базу.
func TestReturningCustomers_EventTime_DB(t *testing.T) {
	setClock(t, testNow())
	seed(t,
		ev{shop: "shop-r", session: "r-s1", visitor: "r-v1", typ: "purchase", at: utc(-5, 12, 0), order: "r-o1", total: 100},
		ev{shop: "shop-r", session: "r-s2", visitor: "r-v1", typ: "purchase", at: utc(-2, 12, 0), order: "r-o2", total: 100},
		ev{shop: "shop-r", session: "r-s3", visitor: "r-v2", typ: "purchase", at: utc(-1, 12, 0), order: "r-o3", total: 100},
		// покупка 40 дней назад, записанная вчера: в «30 дней» не входит
		ev{shop: "shop-r", session: "r-s4", visitor: "r-v2", typ: "purchase", at: utc(-40, 12, 0), created: utc(-1, 12, 0), order: "r-o4", total: 100},
	)
	r := call[ReturningCustomersResponse](t, HandleReturningCustomers, `{"shopId":"shop-r","period":"30d"}`)
	if r.TotalBuyers != 2 || r.RepeatBuyers != 1 {
		t.Errorf("покупатели/повторные = %d/%d; want 2/1", r.TotalBuyers, r.RepeatBuyers)
	}
}

// Лидерборд платформы: посетители — люди; магазин только с роботами не «активный».
func TestGlobalTopShops_HumansOnly_DB(t *testing.T) {
	setClock(t, testNow())
	seed(t,
		pageView("shop-a", "a-s1", "a-v1", utc(-1, 10, 0)),
		pageView("shop-b", "b-s1", "b-v1", utc(-1, 10, 0)),
		pageView("shop-b", "b-s2", "b-v2", utc(-1, 11, 0)),
		ev{shop: "shop-b", session: "b-s1", visitor: "b-v1", typ: "purchase", at: utc(-1, 12, 0), order: "b1", total: 900},
		ev{shop: "shop-bots", session: "x-s1", visitor: "x-v1", typ: "page_view", page: "/", at: utc(-1, 10, 0), traffic: "bot"},
	)
	r := call[TopShopsResponse](t, HandleGlobalTopShops, `{"period":"30d"}`)
	got := map[string]ShopEntry{}
	for _, s := range r.Shops {
		got[s.ShopID] = s
	}
	if _, ok := got["shop-bots"]; ok {
		t.Errorf("магазин только с роботами попал в лидерборд: %+v", r.Shops)
	}
	if b := got["shop-b"]; b.UniqueVisitors != 2 || b.TotalOrders != 1 || b.TotalRevenueCents != 900 {
		t.Errorf("shop-b: %+v; want 2 посетителя, 1 заказ, 900", b)
	}
	if a := got["shop-a"]; a.UniqueVisitors != 1 {
		t.Errorf("shop-a: %+v; want 1 посетитель", a)
	}
	if r.ActiveShopsCount != 2 {
		t.Errorf("активных магазинов %d; want 2", r.ActiveShopsCount)
	}
}

// Миграции 001…017 дважды подряд на пустой базе: без ошибок, итог — новые определения
// (день и окна — по времени события, трафик — только люди, деньги — без фильтра), у каждого
// представления именованный уникальный индекс, индексы не копятся, REFRESH CONCURRENTLY проходит.
func TestMigrations_TwiceGiveNewDefinitions_DB(t *testing.T) {
	pool := needDB(t)
	for run, errs := range migrationRuns {
		if len(errs) > 0 {
			t.Errorf("прогон %d: ошибки миграций: %v", run+1, errs)
		}
	}

	rules := []struct {
		view, index   string
		must, mustNot []string
	}{
		{"silver.daily_traffic", "uq_daily_traffic", []string{"event_timestamp", "traffic_type"}, []string{"created_at"}},
		{"silver.daily_orders", "uq_daily_orders", []string{"event_timestamp", "order_cancel"}, []string{"created_at", "traffic_type"}},
		{"silver.daily_funnel", "uq_daily_funnel", []string{"event_timestamp", "traffic_type"}, []string{"created_at"}},
		{"silver.daily_channel_attribution", "uq_daily_channel_attribution", []string{"event_timestamp", "traffic_type"}, []string{"created_at"}},
		{"silver.daily_geo", "uq_daily_geo", []string{"event_timestamp", "traffic_type"}, []string{"created_at"}},
		{"gold.dashboard_kpi", "uq_dashboard_kpi", []string{"FULL JOIN"}, []string{"created_at"}},
		{"gold.top_products", "uq_top_products", []string{"event_timestamp", "30 days"}, []string{"created_at", "traffic_type"}},
	}
	ctx := context.Background()
	for _, r := range rules {
		var def string
		if err := pool.QueryRow(ctx, `SELECT pg_get_viewdef($1::regclass)`, r.view).Scan(&def); err != nil {
			t.Errorf("%s: нет представления: %v", r.view, err)
			continue
		}
		for _, s := range r.must {
			if !strings.Contains(def, s) {
				t.Errorf("%s: в определении нет %q", r.view, s)
			}
		}
		for _, s := range r.mustNot {
			if strings.Contains(def, s) {
				t.Errorf("%s: в определении осталось %q", r.view, s)
			}
		}

		schema, name, _ := strings.Cut(r.view, ".")
		var indexes []string
		rows, err := pool.Query(ctx, `SELECT indexname FROM pg_indexes WHERE schemaname = $1 AND tablename = $2 ORDER BY 1`, schema, name)
		if err != nil {
			t.Fatalf("%s: индексы: %v", r.view, err)
		}
		for rows.Next() {
			var ix string
			if err := rows.Scan(&ix); err != nil {
				t.Fatalf("%s: индексы: %v", r.view, err)
			}
			indexes = append(indexes, ix)
		}
		rows.Close()
		if !strings.Contains(","+strings.Join(indexes, ",")+",", ","+r.index+",") {
			t.Errorf("%s: нет уникального индекса %s (есть %v)", r.view, r.index, indexes)
		}
		if len(indexes) > 2 {
			t.Errorf("%s: индексы копятся от прогона к прогону: %v", r.view, indexes)
		}
	}
	if err := db.RefreshMatviews(ctx, pool); err != nil {
		t.Errorf("REFRESH CONCURRENTLY: %v", err)
	}
}
