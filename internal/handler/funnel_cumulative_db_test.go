//go:build dbtest

package handler

// Воронка — накопительная (владелец 28.09): «Добавлено в корзину» и «Готов к оплате» считают
// все сессии, дошедшие до шага или дальше, — оплатившая сессия не пропадает из предыдущих
// шагов. Раньше шаг считал только сессии с событием именно этого шага: «Оплаченные заказы: 6»
// при «Добавлено в корзину: 4» и «Готов к оплате: 0» (витрины не слали checkout_start, а
// «Купить сейчас» не слал add_to_cart). Запуск — см. main_db_test.go.

import "testing"

func TestFunnelCumulativeSteps_DB(t *testing.T) {
	setClock(t, testNow())
	const shop = "shop-f"
	seed(t,
		// только корзина
		pageView(shop, "f-s1", "f-v1", utc(-2, 10, 0)),
		ev{shop: shop, session: "f-s1", visitor: "f-v1", typ: "add_to_cart", at: utc(-2, 10, 1)},
		// оплата без событий корзины и оформления (как «Купить сейчас»)
		pageView(shop, "f-s2", "f-v2", utc(-2, 11, 0)),
		ev{shop: shop, session: "f-s2", visitor: "f-v2", typ: "purchase", at: utc(-2, 11, 5), order: "f-o1", total: 1000},
		// корзина и оформление без оплаты
		pageView(shop, "f-s3", "f-v3", utc(-1, 12, 0)),
		ev{shop: shop, session: "f-s3", visitor: "f-v3", typ: "add_to_cart", at: utc(-1, 12, 1)},
		ev{shop: shop, session: "f-s3", visitor: "f-v3", typ: "checkout_start", at: utc(-1, 12, 2)},
		// только просмотр
		pageView(shop, "f-s4", "f-v4", utc(-1, 13, 0)),
	)

	want := map[string]int64{"visits": 4, "add_to_cart": 3, "checkout_starts": 2, "purchases": 1}
	for _, h := range []struct {
		name, payload string
		handler       func(t *testing.T, payload string) FunnelResponse
	}{
		{"shop", `{"shopId":"shop-f","period":"30d"}`, func(t *testing.T, p string) FunnelResponse { return call[FunnelResponse](t, HandleFunnel, p) }},
		{"global", `{"period":"30d"}`, func(t *testing.T, p string) FunnelResponse { return call[FunnelResponse](t, HandleGlobalFunnel, p) }},
	} {
		st := h.handler(t, h.payload).Stages
		for name, n := range want {
			if got := stage(t, st, name).Count; got != n {
				t.Errorf("%s: шаг %s = %d; want %d", h.name, name, got, n)
			}
		}
		// Проценты — от всех посетителей периода: 3/4, 2/4, 1/4.
		if r := stage(t, st, "add_to_cart").Rate; r != 75 {
			t.Errorf("%s: корзина %v%%; want 75", h.name, r)
		}
		if r := stage(t, st, "checkout_starts").Rate; r != 50 {
			t.Errorf("%s: готов к оплате %v%%; want 50", h.name, r)
		}
	}
}
