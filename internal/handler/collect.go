package handler

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/merfy/analytics-collector/internal/botua"
	"github.com/merfy/analytics-collector/internal/geo"
	"github.com/merfy/analytics-collector/internal/rabbitmq"
	"github.com/merfy/analytics-collector/internal/traffictype"
	"github.com/merfy/analytics-collector/internal/util"
)

type CollectRequest struct {
	ShopID string `json:"shop_id"`
	// TenantID is a top-level passthrough: bronze_writer reads tenant_id from the payload,
	// so it MUST survive the geo re-marshal below (without this field it would be dropped).
	TenantID string         `json:"tenant_id,omitempty"`
	Events   []CollectEvent `json:"events"`
}

type CollectEvent struct {
	Type            string      `json:"type"`
	SessionID       string      `json:"session_id"`
	VisitorID       string      `json:"visitor_id,omitempty"`
	PageURL         string      `json:"page_url,omitempty"`
	PageTitle       string      `json:"page_title,omitempty"`
	Referrer        string      `json:"referrer,omitempty"`
	UTMSource       string      `json:"utm_source,omitempty"`
	UTMMedium       string      `json:"utm_medium,omitempty"`
	UTMCampaign     string      `json:"utm_campaign,omitempty"`
	ProductID       string      `json:"product_id,omitempty"`
	ProductName     string      `json:"product_name,omitempty"`
	ProductPriceRaw interface{} `json:"product_price,omitempty"`
	ProductPrice    int64       `json:"-"`
	OrderID         string      `json:"order_id,omitempty"`
	OrderTotalRaw   interface{} `json:"order_total,omitempty"`
	OrderTotal      int64       `json:"-"`
	CostPriceCents  *int64      `json:"cost_price_cents,omitempty"`
	CategoryID      *string     `json:"category_id,omitempty"`
	Timestamp       string      `json:"timestamp"`
	// Geo is stamped server-side from the client IP at ingest (never sent by the client);
	// the raw IP is not persisted (152-ФЗ).
	GeoCountry string `json:"geo_country,omitempty"`
	GeoSubject string `json:"geo_subject,omitempty"`
	GeoCity    string `json:"geo_city,omitempty"`
	// Traffic — необязательная подсказка от tracker.js: "bot" (браузер под управлением
	// программы, navigator.webdriver) или "internal" (метка владельца, ?mfy_owner=1).
	// Любое другое значение или отсутствие поля — кандидат в human (см. classifyTraffic).
	// Своё дело подсказка делает только внутри ServeHTTP: перед публикацией
	// обнуляется (см. ServeHTTP), в очередь и в лог уходит только TrafficType.
	Traffic string `json:"traffic,omitempty"`
	// TrafficType — итоговая пометка (human/bot/internal — internal/traffictype),
	// которую проставляет сервер по правилу 4 README «Пометка трафика». Клиент это
	// поле не присылает и не может его подделать: сервер всегда перезаписывает его
	// при каждом /collect.
	TrafficType string `json:"traffic_type,omitempty"`
}

var validEventTypes = map[string]bool{
	"page_view":        true,
	"product_view":     true,
	"add_to_cart":      true,
	"remove_from_cart": true,
	"checkout_start":   true,
	"purchase":         true,
	"session_start":    true,
	"order_cancel":     true,
}

// eventPublisher is the minimal seam CollectHandler needs from the RMQ publisher. It keeps
// the exported constructor's concrete signature while letting tests inject a capturing fake
// (the RMQ transport is not what the hot-path tests exercise). *rabbitmq.Publisher satisfies it.
type eventPublisher interface {
	Publish(ctx context.Context, body []byte) error
}

type CollectHandler struct {
	publisher eventPublisher
	geo       *geo.Resolver
}

func NewCollectHandler(pub *rabbitmq.Publisher, g *geo.Resolver) *CollectHandler {
	return &CollectHandler{publisher: pub, geo: g}
}

func (h *CollectHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20)) // 1MB limit
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_payload", "cannot read body")
		return
	}

	var req CollectRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_payload", "invalid JSON")
		return
	}

	if req.ShopID == "" {
		writeError(w, http.StatusBadRequest, "invalid_payload", "shop_id is required")
		return
	}

	if len(req.Events) == 0 || len(req.Events) > 100 {
		writeError(w, http.StatusBadRequest, "invalid_payload", "events: must have 1-100 elements")
		return
	}

	// Normalize flexible price fields to int64
	for i := range req.Events {
		req.Events[i].ProductPrice = util.ToInt64Price(req.Events[i].ProductPriceRaw)
		req.Events[i].OrderTotal = util.ToInt64Price(req.Events[i].OrderTotalRaw)
	}

	cutoff := time.Now().Add(-24 * time.Hour)
	for i, e := range req.Events {
		if e.SessionID == "" {
			writeError(w, http.StatusBadRequest, "invalid_payload", "events[].session_id is required")
			return
		}
		if !validEventTypes[e.Type] {
			writeError(w, http.StatusBadRequest, "invalid_payload", "events[].type: invalid value")
			return
		}
		if e.Timestamp != "" {
			ts, err := time.Parse(time.RFC3339, e.Timestamp)
			if err != nil {
				writeError(w, http.StatusBadRequest, "invalid_payload", "events[].timestamp: invalid ISO 8601")
				return
			}
			if ts.Before(cutoff) {
				writeError(w, http.StatusBadRequest, "invalid_payload", "events[].timestamp: older than 24h")
				return
			}
		} else {
			req.Events[i].Timestamp = time.Now().UTC().Format(time.RFC3339)
		}
	}

	// Geo enrichment (graceful, in-process, ~microseconds). middleware.RealIP already put
	// the real client IP in r.RemoteAddr. We resolve ONE geo per batch and stamp it on every
	// event. The raw IP is NEVER placed into req/body/logs (152-ФЗ). On any hiccup
	// (unlocatable IP) geo stays null — /collect never fails because of geo.
	if h.geo != nil {
		if loc := h.geo.Resolve(r.RemoteAddr); !loc.IsZero() {
			for i := range req.Events {
				req.Events[i].GeoCountry = loc.CountryISO
				req.Events[i].GeoSubject = loc.Subject
				req.Events[i].GeoCity = loc.City
			}
		}
	}

	// Пометка трафика — правило 4 README «Пометка трафика». Подпись браузера
	// читается ОДИН раз на весь батч (User-Agent запроса, а не поле события —
	// она одна на HTTP-запрос), поле traffic — своё у каждого события. Подпись
	// браузера в сообщение и в лог не попадает, летит только вывод.
	uaIsBot := botua.IsBot(r.UserAgent())
	for i := range req.Events {
		req.Events[i].TrafficType = classifyTraffic(uaIsBot, req.Events[i].Traffic, req.Events[i].Type)
		// Подсказка клиента сделала своё дело в classifyTraffic — дальше в очередь
		// и в лог уходит только итоговый TrafficType, сырое поле обнуляем.
		req.Events[i].Traffic = ""
	}

	// Пересобираем тело ВСЕГДА, а не только когда гео резолвится: traffic_type
	// обязан уехать на каждое событие независимо от IP. Ошибка маршалинга тут не
	// должна случаться (req получен Unmarshal'ом из уже провалидированного JSON).
	if out, err := json.Marshal(req); err == nil {
		body = out
	} else {
		// Тело и подпись браузера в лог не идут (152-ФЗ действует и на сбоях) —
		// только факт и shop_id, чтобы найти запрос по остальным логам сервиса.
		// Публикуем исходное тело: /collect не должен падать из-за пометки трафика.
		slog.Warn("collect: re-marshal with traffic_type failed, publishing original body", "error", err, "shop_id", req.ShopID)
	}

	// Publish to RabbitMQ
	if err := h.publisher.Publish(r.Context(), body); err != nil {
		slog.Error("publish events", "error", err, "shop_id", req.ShopID)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// noUACheckEventTypes — типы событий, для которых подпись браузера (правило 4.1)
// ничего не решает про посетителя. purchase и order_cancel шлёт то браузер
// (checkout-result.js в темах — window._mfy.trackPurchase со страницы после
// оплаты), то сам сервер заказов (orders: payment.service.ts, orders.service.ts —
// Node fetch, подпись "node" или вовсе пустая, с session_id/visitor_id покупателя
// из заказа) — источник запроса к /collect заранее не известен, а подпись
// сервера ничего не говорит о посетителе. Обход всё равно безопасен: README
// «Пометка трафика», правило «что считается» — выручка и заказы (purchase,
// order_cancel) идут в отчёты БЕЗ фильтра по traffic_type, это деньги, их
// P2-отчётность не имеет права терять. Данные, а не отдельная ветка if на
// каждый тип.
var noUACheckEventTypes = map[string]bool{
	"purchase":     true,
	"order_cancel": true,
}

// classifyTraffic — правило 4 README «Пометка трафика», дословный порядок,
// первое совпадение (ранний выход через switch, без лестницы if):
//  1. подпись браузера из списка ботов или пустая → bot (аргумент uaIsBot,
//     посчитан один раз на весь батч вызывающей стороной) — ПРОПУСКАЕТСЯ для
//     noUACheckEventTypes (см. комментарий);
//  2. traffic == "bot" (navigator.webdriver) → bot;
//  3. traffic == "internal" (метка владельца) → internal;
//  4. иначе → human.
func classifyTraffic(uaIsBot bool, traffic string, eventType string) string {
	browserIsBot := uaIsBot && !noUACheckEventTypes[eventType]
	switch {
	case browserIsBot:
		return traffictype.Bot
	case traffic == traffictype.Bot:
		return traffictype.Bot
	case traffic == traffictype.Internal:
		return traffictype.Internal
	default:
		return traffictype.Human
	}
}

func writeError(w http.ResponseWriter, status int, errCode, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{
		"error":   errCode,
		"message": msg,
	})
}
