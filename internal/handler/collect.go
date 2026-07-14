package handler

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/merfy/analytics-collector/internal/geo"
	"github.com/merfy/analytics-collector/internal/rabbitmq"
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
	Type           string      `json:"type"`
	SessionID      string      `json:"session_id"`
	VisitorID      string      `json:"visitor_id,omitempty"`
	PageURL        string      `json:"page_url,omitempty"`
	PageTitle      string      `json:"page_title,omitempty"`
	Referrer       string      `json:"referrer,omitempty"`
	UTMSource      string      `json:"utm_source,omitempty"`
	UTMMedium      string      `json:"utm_medium,omitempty"`
	UTMCampaign    string      `json:"utm_campaign,omitempty"`
	ProductID      string      `json:"product_id,omitempty"`
	ProductName    string      `json:"product_name,omitempty"`
	ProductPriceRaw interface{} `json:"product_price,omitempty"`
	ProductPrice   int64       `json:"-"`
	OrderID        string      `json:"order_id,omitempty"`
	OrderTotalRaw  interface{} `json:"order_total,omitempty"`
	OrderTotal     int64       `json:"-"`
	CostPriceCents *int64      `json:"cost_price_cents,omitempty"`
	CategoryID     *string     `json:"category_id,omitempty"`
	Timestamp      string      `json:"timestamp"`
	// Geo is stamped server-side from the client IP at ingest (never sent by the client);
	// the raw IP is not persisted (152-ФЗ).
	GeoCountry string `json:"geo_country,omitempty"`
	GeoSubject string `json:"geo_subject,omitempty"`
	GeoCity    string `json:"geo_city,omitempty"`
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
	// event, then re-marshal. The raw IP is NEVER placed into req/body/logs (152-ФЗ). On any
	// hiccup (unlocatable IP, marshal error) geo stays null and we ship the original body —
	// /collect never fails because of geo.
	if h.geo != nil {
		if loc := h.geo.Resolve(r.RemoteAddr); !loc.IsZero() {
			for i := range req.Events {
				req.Events[i].GeoCountry = loc.CountryISO
				req.Events[i].GeoSubject = loc.Subject
				req.Events[i].GeoCity = loc.City
			}
			if out, err := json.Marshal(req); err == nil {
				body = out
			}
		}
	}

	// Publish to RabbitMQ
	if err := h.publisher.Publish(r.Context(), body); err != nil {
		slog.Error("publish events", "error", err, "shop_id", req.ShopID)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func writeError(w http.ResponseWriter, status int, errCode, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{
		"error":   errCode,
		"message": msg,
	})
}
