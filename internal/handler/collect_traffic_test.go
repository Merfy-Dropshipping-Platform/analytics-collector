package handler

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/merfy/analytics-collector/internal/geo"
)

// Классификация traffic_type на приёме — правило 4 README, раздел «Пометка трафика»:
// 1) подпись браузера из списка ботов или пустая → bot;
// 2) traffic="bot" (navigator.webdriver) → bot;
// 3) traffic="internal" (метка владельца) → internal;
// 4) иначе → human.
// Подпись браузера читается ОДИН раз на весь батч (см. postCollectUA), а не на
// каждое событие — так и должно быть, UA один на HTTP-запрос.

const realBrowserUA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36"
const botUA = "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"

// freshTS — метка времени внутри окна приёма (не старше 24ч, см. cutoff в
// ServeHTTP). Захардкоженная дата в теле запроса рано или поздно устаревает и
// красит тест независимо от логики, которую он проверяет — ровно так и вышло с
// собственными фикстурами collect_geo_test.go на дату "2026-07-14T10:00:00Z"
// (см. отчёт куска), поэтому здесь время всегда "сейчас".
func freshTS() string {
	return time.Now().UTC().Format(time.RFC3339)
}

// postCollectUA шлёт body с заданным User-Agent — geo не участвует в этих тестах
// (RemoteAddr — приватный, резолвится в zero location через noop-провайдер).
func postCollectUA(t *testing.T, h *CollectHandler, ua, body string) *capturePublisher {
	t.Helper()
	cap := &capturePublisher{}
	h.publisher = cap
	req := httptest.NewRequest("POST", "/collect", strings.NewReader(body))
	if ua != "" {
		req.Header.Set("User-Agent", ua)
	}
	req.RemoteAddr = "10.0.0.9:5555"
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != 204 {
		t.Fatalf("status = %d; want 204, body=%s", w.Code, w.Body.String())
	}
	return cap
}

// eventTrafficTypes читает traffic_type каждого события из опубликованного тела.
func eventTrafficTypes(t *testing.T, body []byte) []string {
	t.Helper()
	var req CollectRequest
	if err := json.Unmarshal(body, &req); err != nil {
		t.Fatalf("unmarshal published body: %v; body=%s", err, body)
	}
	out := make([]string, len(req.Events))
	for i, e := range req.Events {
		out[i] = e.TrafficType
	}
	return out
}

func newTrafficTestHandler() *CollectHandler {
	return &CollectHandler{geo: geo.NewResolver(geo.NoopProvider{})}
}

// Подпись-бот → bot, даже без поля traffic у события.
func TestServeHTTP_TrafficType_BotUA(t *testing.T) {
	h := newTrafficTestHandler()
	body := `{"shop_id":"s1","events":[{"type":"page_view","session_id":"sess-1","timestamp":"` + freshTS() + `"}]}`
	cap := postCollectUA(t, h, botUA, body)
	got := eventTrafficTypes(t, cap.bodies[0])
	if got[0] != "bot" {
		t.Errorf("traffic_type = %q; want bot (UA-бот)", got[0])
	}
}

// Пустая подпись браузера — тоже бот (правило 4.1).
func TestServeHTTP_TrafficType_EmptyUAIsBot(t *testing.T) {
	h := newTrafficTestHandler()
	body := `{"shop_id":"s1","events":[{"type":"page_view","session_id":"sess-1","timestamp":"` + freshTS() + `"}]}`
	cap := postCollectUA(t, h, "", body)
	got := eventTrafficTypes(t, cap.bodies[0])
	if got[0] != "bot" {
		t.Errorf("traffic_type = %q; want bot (пустой UA)", got[0])
	}
}

// Обычная подпись + traffic:"bot" (navigator.webdriver) → bot.
func TestServeHTTP_TrafficType_TrafficFieldBot(t *testing.T) {
	h := newTrafficTestHandler()
	body := `{"shop_id":"s1","events":[{"type":"page_view","session_id":"sess-1","traffic":"bot","timestamp":"` + freshTS() + `"}]}`
	cap := postCollectUA(t, h, realBrowserUA, body)
	got := eventTrafficTypes(t, cap.bodies[0])
	if got[0] != "bot" {
		t.Errorf("traffic_type = %q; want bot (traffic:bot)", got[0])
	}
}

// traffic:"internal" (метка владельца) → internal.
func TestServeHTTP_TrafficType_TrafficFieldInternal(t *testing.T) {
	h := newTrafficTestHandler()
	body := `{"shop_id":"s1","events":[{"type":"page_view","session_id":"sess-1","traffic":"internal","timestamp":"` + freshTS() + `"}]}`
	cap := postCollectUA(t, h, realBrowserUA, body)
	got := eventTrafficTypes(t, cap.bodies[0])
	if got[0] != "internal" {
		t.Errorf("traffic_type = %q; want internal", got[0])
	}
}

// Неизвестное значение traffic (не bot и не internal) → human. Аналогично
// отсутствие поля traffic вовсе → human.
func TestServeHTTP_TrafficType_UnknownAndMissingDefaultHuman(t *testing.T) {
	h := newTrafficTestHandler()
	body := `{"shop_id":"s1","events":[
		{"type":"page_view","session_id":"sess-1","traffic":"zzz","timestamp":"` + freshTS() + `"},
		{"type":"page_view","session_id":"sess-1","timestamp":"` + freshTS() + `"}
	]}`
	cap := postCollectUA(t, h, realBrowserUA, body)
	got := eventTrafficTypes(t, cap.bodies[0])
	if got[0] != "human" {
		t.Errorf("traffic_type[0] = %q; want human (traffic:zzz — неизвестное значение)", got[0])
	}
	if got[1] != "human" {
		t.Errorf("traffic_type[1] = %q; want human (поле traffic отсутствует)", got[1])
	}
}

// Порядок правил: UA-бот побеждает traffic:"internal" (пункт 4 контракта —
// бот идёт первым пунктом, а не внутренним посетителем).
func TestServeHTTP_TrafficType_BotUAWinsOverInternal(t *testing.T) {
	h := newTrafficTestHandler()
	body := `{"shop_id":"s1","events":[{"type":"page_view","session_id":"sess-1","traffic":"internal","timestamp":"` + freshTS() + `"}]}`
	cap := postCollectUA(t, h, botUA, body)
	got := eventTrafficTypes(t, cap.bodies[0])
	if got[0] != "bot" {
		t.Errorf("traffic_type = %q; want bot (UA-бот важнее internal)", got[0])
	}
}

// Подпись браузера НЕ должна попасть ни в опубликованное тело, ни в него самого
// через traffic_type: только вывод классификации, не сырой UA (README, пункт 4).
func TestServeHTTP_TrafficType_UANotLeaked(t *testing.T) {
	h := newTrafficTestHandler()
	body := `{"shop_id":"s1","events":[{"type":"page_view","session_id":"sess-1","timestamp":"` + freshTS() + `"}]}`
	cap := postCollectUA(t, h, botUA, body)
	if strings.Contains(string(cap.bodies[0]), "Googlebot") {
		t.Errorf("published payload leaked the raw User-Agent: %s", cap.bodies[0])
	}
}

// Разные события одного батча получают КАЖДОЕ своё traffic_type по своему
// полю traffic, при обычной (не бот) подписи браузера.
func TestServeHTTP_TrafficType_PerEventWithinBatch(t *testing.T) {
	h := newTrafficTestHandler()
	body := `{"shop_id":"s1","events":[
		{"type":"page_view","session_id":"sess-1","traffic":"internal","timestamp":"` + freshTS() + `"},
		{"type":"page_view","session_id":"sess-1","timestamp":"` + freshTS() + `"}
	]}`
	cap := postCollectUA(t, h, realBrowserUA, body)
	got := eventTrafficTypes(t, cap.bodies[0])
	if got[0] != "internal" {
		t.Errorf("traffic_type[0] = %q; want internal", got[0])
	}
	if got[1] != "human" {
		t.Errorf("traffic_type[1] = %q; want human", got[1])
	}
}

// --- Обход правила 4.1 (проверка User-Agent) для purchase/order_cancel -----
// purchase и order_cancel могут прийти и от браузера (checkout-result.js в
// темах), и от сервера заказов (Node fetch, подпись "node" или пустая) — заранее
// не известно, чья это подпись, а подпись сервера ничего не говорит о
// посетителе. Обход безопасен для отчётов, потому что деньги (purchase,
// order_cancel) считаются без фильтра по traffic_type — см. noUACheckEventTypes.

// TestClassifyTraffic — прямой табличный тест чистой функции: проверяем, что
// обход UA-проверки не размывает остальные правила (2/3 по-прежнему работают)
// и не протекает на другие типы событий.
func TestClassifyTraffic(t *testing.T) {
	cases := []struct {
		name      string
		uaIsBot   bool
		traffic   string
		eventType string
		want      string
	}{
		{"обычный визит, UA не бот", false, "", "page_view", "human"},
		{"UA-бот на page_view — bot", true, "", "page_view", "bot"},
		{"UA-бот на purchase — правило 1 пропущено, human", true, "", "purchase", "human"},
		{"UA-бот на order_cancel — правило 1 пропущено, human", true, "", "order_cancel", "human"},
		{"purchase + explicit traffic:bot — правило 2 всё ещё активно", true, "bot", "purchase", "bot"},
		{"purchase + explicit traffic:internal — правило 3 всё ещё активно", true, "internal", "purchase", "internal"},
		{"add_to_cart НЕ в списке обхода — UA-бот остаётся bot", true, "", "add_to_cart", "bot"},
	}
	for _, c := range cases {
		if got := classifyTraffic(c.uaIsBot, c.traffic, c.eventType); got != c.want {
			t.Errorf("%s: classifyTraffic(%v,%q,%q) = %q; want %q", c.name, c.uaIsBot, c.traffic, c.eventType, got, c.want)
		}
	}
}

// Реалистичная регрессия: настоящая подпись встроенного fetch Node.js — "node".
// ВАЖНО: сама по себе она не бот (botua.TestIsBot_ServerUAIsNotBot), так что
// этот тест НЕ проверяет обход правила 4.1 — он проверяет, что реальный трафик
// сервера заказов классифицируется как ожидается. За обход отвечает
// TestServeHTTP_TrafficType_PurchaseWithBotUA_Human ниже (там UA — из списка
// ботов, и только обход не даёт событию стать bot).
func TestServeHTTP_TrafficType_PurchaseFromServer_RealNodeUA_Human(t *testing.T) {
	h := newTrafficTestHandler()
	body := `{"shop_id":"s1","events":[{"type":"purchase","session_id":"sess-1","order_id":"o1","order_total":5000,"timestamp":"` + freshTS() + `"}]}`
	cap := postCollectUA(t, h, "node", body)
	got := eventTrafficTypes(t, cap.bodies[0])
	if got[0] != "human" {
		t.Errorf("traffic_type = %q; want human (purchase от сервера заказов, UA=node)", got[0])
	}
}

// Обход в деле: UA — Googlebot (заведомо бот по списку), но событие всё равно
// human, потому что noUACheckEventTypes отключает проверку UA для purchase.
// Без обхода это событие стало бы bot — саботаж на это и проверяет.
func TestServeHTTP_TrafficType_PurchaseWithBotUA_Human(t *testing.T) {
	h := newTrafficTestHandler()
	body := `{"shop_id":"s1","events":[{"type":"purchase","session_id":"sess-1","order_id":"o3","order_total":4000,"timestamp":"` + freshTS() + `"}]}`
	cap := postCollectUA(t, h, botUA, body)
	got := eventTrafficTypes(t, cap.bodies[0])
	if got[0] != "human" {
		t.Errorf("traffic_type = %q; want human (обход правила 4.1 для purchase)", got[0])
	}
}

// Сервер заказов может вовсе не поставить User-Agent — пустая подпись НЕ
// должна превратить оплату в bot (иначе все покупки посчитались бы роботами).
func TestServeHTTP_TrafficType_PurchaseEmptyUA_Human(t *testing.T) {
	h := newTrafficTestHandler()
	body := `{"shop_id":"s1","events":[{"type":"purchase","session_id":"sess-1","order_id":"o2","order_total":3000,"timestamp":"` + freshTS() + `"}]}`
	cap := postCollectUA(t, h, "", body)
	got := eventTrafficTypes(t, cap.bodies[0])
	if got[0] != "human" {
		t.Errorf("traffic_type = %q; want human (purchase с пустой подписью)", got[0])
	}
}

// Тот же обход для order_cancel — второй тип из таблицы.
func TestServeHTTP_TrafficType_OrderCancelEmptyUA_Human(t *testing.T) {
	h := newTrafficTestHandler()
	body := `{"shop_id":"s1","events":[{"type":"order_cancel","session_id":"sess-1","order_id":"o2","timestamp":"` + freshTS() + `"}]}`
	cap := postCollectUA(t, h, "", body)
	got := eventTrafficTypes(t, cap.bodies[0])
	if got[0] != "human" {
		t.Errorf("traffic_type = %q; want human (order_cancel с пустой подписью)", got[0])
	}
}

// Клиентская подсказка (поле traffic) не должна доехать до очереди сама по
// себе — наружу уходит только итоговый traffic_type. Проверяем сырой JSON, а
// не структуру: структура декодирует omitempty-поле молча, если оно забыто.
func TestServeHTTP_TrafficType_RawTrafficFieldZeroedInPublished(t *testing.T) {
	h := newTrafficTestHandler()
	body := `{"shop_id":"s1","events":[{"type":"page_view","session_id":"sess-1","traffic":"internal","timestamp":"` + freshTS() + `"}]}`
	cap := postCollectUA(t, h, realBrowserUA, body)

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(cap.bodies[0], &raw); err != nil {
		t.Fatal(err)
	}
	var events []map[string]json.RawMessage
	if err := json.Unmarshal(raw["events"], &events); err != nil {
		t.Fatal(err)
	}
	if _, ok := events[0]["traffic"]; ok {
		t.Errorf("published event still has raw \"traffic\" key: %s", cap.bodies[0])
	}
	if tt, ok := events[0]["traffic_type"]; !ok || string(tt) != `"internal"` {
		t.Errorf("published event traffic_type = %s; want \"internal\"", tt)
	}
}

// Обход НЕ должен протечь на обычные браузерные события — page_view с пустой
// подписью остаётся bot, как и раньше (регрессия: обход строго по типу события).
func TestServeHTTP_TrafficType_PageViewEmptyUA_StillBot(t *testing.T) {
	h := newTrafficTestHandler()
	body := `{"shop_id":"s1","events":[{"type":"page_view","session_id":"sess-1","timestamp":"` + freshTS() + `"}]}`
	cap := postCollectUA(t, h, "", body)
	got := eventTrafficTypes(t, cap.bodies[0])
	if got[0] != "bot" {
		t.Errorf("traffic_type = %q; want bot (page_view — обход purchase/order_cancel сюда не должен протечь)", got[0])
	}
}
