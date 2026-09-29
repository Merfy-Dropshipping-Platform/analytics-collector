//go:build dbtest

package handler

// Посетитель — тот, у кого был визит (жалоба владельца 28.09: «Сессии по локациям» 43 при
// «Посещаемости» 25 чел.). Сервис заказов шлёт покупку без визита браузера: visitor_id
// "server-<заказ>", session_id "order-<заказ>", подпись сервера не проверяется (это деньги),
// поэтому пометка — human. Такая покупка становилась отдельным «посетителем» без единого визита:
// у MrMerfy 28.09 это 9 из 25 «чел.». Посетители и визиты считаются по одним и тем же событиям,
// деньги покупки остаются. Запуск — см. main_db_test.go.

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestServerPurchaseIsNotAVisitor_DB(t *testing.T) {
	// «Сейчас» — сегодня 12:00 UTC (15:00 МСК).
	setClock(t, testNow())
	const shop = "shop-sp"
	serverPurchase := ev{shop: shop, session: "order-sp-o1", visitor: "server-sp-o1", typ: "purchase",
		at: utc(0, 11, 0), order: "sp-o1", total: 1500} // 14:00 МСК, пометка human (как на проде)
	seed(t,
		// человек: два визита — вчера и сегодня в 09:00 UTC (12:00 МСК)
		pageView(shop, "sp-s1", "sp-v1", utc(-1, 10, 0)),
		pageView(shop, "sp-s2", "sp-v1", utc(0, 9, 0)),
		serverPurchase,
		serverPurchase, // вторая позиция того же заказа
	)
	const payload = `{"shopId":"shop-sp","period":"30d"}`
	todayDay := dayOf(utc(0, 0, 0))

	t.Run("dashboard: посетитель один, визитов два, заказ на месте", func(t *testing.T) {
		k := call[DashboardResponse](t, HandleDashboard, payload).KPI
		if k.UniqueVisitors != 1 || k.UniqueSessions != 2 {
			t.Errorf("посетители/визиты = %d/%d; want 1/2 (покупка сервера — не посетитель)", k.UniqueVisitors, k.UniqueSessions)
		}
		if k.TotalOrders != 1 || k.TotalRevenueCents != 1500 {
			t.Errorf("заказы/выручка = %d/%d; want 1/1500", k.TotalOrders, k.TotalRevenueCents)
		}
	})
	t.Run("dashboard: день из представления", func(t *testing.T) {
		d := seriesByDay(call[DashboardResponse](t, HandleDashboard, payload).TimeSeries)[todayDay]
		if d.Visitors != 1 || d.Sessions != 1 || d.Orders != 1 {
			t.Errorf("сегодня: %+v; want 1 посетитель, 1 визит, 1 заказ", d)
		}
	})
	t.Run("dashboard 24h: отрезок только с покупкой — без посетителей", func(t *testing.T) {
		b := seriesByDay(call[DashboardResponse](t, HandleDashboard, `{"shopId":"shop-sp","period":"24h"}`).TimeSeries)[todayDay+"T10:00"]
		if b.Visitors != 0 || b.Orders != 1 {
			t.Errorf("отрезок 10:00: %+v; want 0 посетителей, 1 заказ", b)
		}
	})
	t.Run("платформа", func(t *testing.T) {
		k := call[DashboardResponse](t, HandleGlobalDashboard, `{"period":"30d"}`).KPI
		if k.UniqueVisitors != 1 {
			t.Errorf("посетители платформы = %d; want 1", k.UniqueVisitors)
		}
		for _, s := range call[TopShopsResponse](t, HandleGlobalTopShops, `{"period":"30d"}`).Shops {
			if s.ShopID == shop && s.UniqueVisitors != 1 {
				t.Errorf("лидерборд: %+v; want 1 посетитель", s)
			}
		}
	})
	t.Run("посещаемость и воронка", func(t *testing.T) {
		if got := call[TrafficResponse](t, HandleTraffic, payload).Summary.TotalVisitors; got != 1 {
			t.Errorf("посещаемость = %d; want 1", got)
		}
		if got := stage(t, call[FunnelResponse](t, HandleFunnel, payload).Stages, "visits").Count; got != 1 {
			t.Errorf("первый шаг воронки = %d; want 1", got)
		}
	})
	t.Run("Главная по часам", func(t *testing.T) {
		r := call[HourlyTrafficResponse](t, HandleHourlyTraffic, `{"shopId":"shop-sp","hours":10,"tz":"Europe/Moscow"}`)
		byHour := map[string]int64{}
		for _, p := range r.Points {
			byHour[p.Hour[11:]] = p.Visitors
		}
		if r.Visitors != 1 || byHour["12:00"] != 1 || byHour["14:00"] != 0 {
			t.Errorf("итог %d, 12:00 → %d, 14:00 → %d; want 1, 1, 0", r.Visitors, byHour["12:00"], byHour["14:00"])
		}
	})
	t.Run("посетителей не больше, чем визитов", func(t *testing.T) {
		k := call[DashboardResponse](t, HandleDashboard, payload).KPI
		if k.UniqueVisitors > k.UniqueSessions {
			t.Errorf("посетителей %d > визитов %d: есть посетитель без визита", k.UniqueVisitors, k.UniqueSessions)
		}
	})
}

// «Сессии по локациям» — люди по месту первого визита, те же, что в «Посещаемости» (владелец
// 28.09: «люди в обеих»). Сторож: на одних и тех же данных сумма людей по локациям == людям в
// строке итога == unique_visitors дашборда — для магазина и платформы, на всех уровнях, в текущем
// и прошлом периоде.
func TestLocationPeopleEqualVisitors_DB(t *testing.T) {
	setClock(t, testNow())
	const shop = "shop-lp"
	at := func(e ev, country, subject, city string) ev {
		e.country, e.subject, e.city = country, subject, city
		return e
	}
	msk := func(e ev) ev { return at(e, "RU", "Москва", "Москва") }
	seed(t,
		// v1: первый визит — Москва, второй — Петербург с покупкой → человек в Москве, заказ там же
		msk(pageView(shop, "lp-s1", "lp-v1", utc(-5, 10, 0))),
		at(pageView(shop, "lp-s2", "lp-v1", utc(-2, 10, 0)), "RU", "Санкт-Петербург", "Санкт-Петербург"),
		ev{shop: shop, session: "lp-s2", visitor: "lp-v1", typ: "purchase", at: utc(-2, 10, 5), order: "lp-o1", total: 900},
		// v2: первый визит без гео, следующий — Казань → Татарстан (первый с известным гео)
		pageView(shop, "lp-s3", "lp-v2", utc(-4, 10, 0)),
		at(pageView(shop, "lp-s4", "lp-v2", utc(-3, 10, 0)), "RU", "Татарстан", "Казань"),
		// v3: гео не определилось ни разу → «Не определено»
		pageView(shop, "lp-s5", "lp-v3", utc(-3, 11, 0)),
		pageView(shop, "lp-s6", "lp-v3", utc(-1, 11, 0)),
		// v4: только начало сессии (тоже визит)
		msk(ev{shop: shop, session: "lp-s7", visitor: "lp-v4", typ: "session_start", at: utc(-1, 12, 0)}),
		// не визит: только просмотр товара — ни посетитель, ни человек в локациях
		msk(ev{shop: shop, session: "lp-s8", visitor: "lp-v5", typ: "product_view", at: utc(-1, 13, 0), product: "p1"}),
		// робот, свои, покупка сервиса заказов без визита
		msk(ev{shop: shop, session: "lp-b1", visitor: "lp-vb", typ: "page_view", page: "/", at: utc(-1, 14, 0), traffic: "bot"}),
		at(ev{shop: shop, session: "lp-i1", visitor: "lp-vi", typ: "page_view", page: "/", at: utc(-1, 15, 0), traffic: "internal"}, "FI", "Uusimaa", "Хельсинки"),
		ev{shop: shop, session: "order-lp-o2", visitor: "server-lp-o2", typ: "purchase", at: utc(-1, 16, 0), order: "lp-o2", total: 500},
		// прошлый период: один человек дважды
		msk(pageView(shop, "lp-s0", "lp-v0", utc(-40, 10, 0))),
		msk(pageView(shop, "lp-s9", "lp-v0", utc(-39, 10, 0))),
		// другой магазин — только в итогах платформы
		msk(pageView("shop-lp-other", "lp-o1s", "lp-vo", utc(-2, 9, 0))),
	)

	t.Run("место первого визита", func(t *testing.T) {
		got := map[string][2]int64{}
		for _, r := range call[ByLocationResponse](t, HandleByLocation, `{"shopId":"shop-lp","period":"30d"}`).Rows {
			got[r.Subject] = [2]int64{r.People, r.Orders}
		}
		want := map[string][2]int64{"Москва": {2, 1}, "Татарстан": {1, 0}, "": {1, 1}}
		if len(got) != len(want) || got["Москва"] != want["Москва"] || got["Татарстан"] != want["Татарстан"] || got[""] != want[""] {
			t.Errorf("люди/заказы по субъектам = %v; want %v", got, want)
		}
	})

	start, end := periodRange("30d", testNow())
	prevStart := start.Add(-end.Sub(start))
	prevWindow := `"period":"custom","from":"` + prevStart.Format("2006-01-02") + `","to":"` + start.Add(-24*time.Hour).Format("2006-01-02") + `"`
	cases := []struct {
		name, shopPart string
		dashboard      func(context.Context, *pgxpool.Pool, json.RawMessage) (any, error)
		location       func(context.Context, *pgxpool.Pool, json.RawMessage) (any, error)
		visitors       int64
	}{
		{"магазин", `"shopId":"shop-lp",`, HandleDashboard, HandleByLocation, 4},
		{"платформа", ``, HandleGlobalDashboard, HandleGlobalByLocation, 5},
	}
	for _, c := range cases {
		r := call[DashboardResponse](t, c.dashboard, `{`+c.shopPart+`"period":"30d"}`)
		if r.KPI.UniqueVisitors != c.visitors {
			t.Errorf("%s: посетителей %d; want %d", c.name, r.KPI.UniqueVisitors, c.visitors)
		}
		windows := map[string]struct {
			payload string
			want    int64
		}{
			"текущий": {`"period":"30d"`, r.KPI.UniqueVisitors},
			"прошлый": {prevWindow, r.KPIPrev.UniqueVisitors},
		}
		for wname, w := range windows {
			for _, level := range []string{"country", "subject", "city"} {
				loc := call[ByLocationResponse](t, c.location, `{`+c.shopPart+w.payload+`,"level":"`+level+`"}`)
				var sum int64
				for _, row := range loc.Rows {
					sum += row.People
				}
				if sum != w.want || loc.TotalPeople != w.want {
					t.Errorf("%s, %s период, %s: людей по локациям %d, итог %d; want %d = unique_visitors",
						c.name, wname, level, sum, loc.TotalPeople, w.want)
				}
			}
		}
	}
}
