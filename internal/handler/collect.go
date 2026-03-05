package handler

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/merfy/analytics-collector/internal/rabbitmq"
)

type CollectRequest struct {
	ShopID string         `json:"shop_id"`
	Events []CollectEvent `json:"events"`
}

type CollectEvent struct {
	Type         string `json:"type"`
	SessionID    string `json:"session_id"`
	VisitorID    string `json:"visitor_id,omitempty"`
	PageURL      string `json:"page_url,omitempty"`
	PageTitle    string `json:"page_title,omitempty"`
	Referrer     string `json:"referrer,omitempty"`
	UTMSource    string `json:"utm_source,omitempty"`
	UTMMedium    string `json:"utm_medium,omitempty"`
	UTMCampaign  string `json:"utm_campaign,omitempty"`
	ProductID    string `json:"product_id,omitempty"`
	ProductName  string `json:"product_name,omitempty"`
	ProductPrice int64  `json:"product_price,omitempty"`
	OrderID      string `json:"order_id,omitempty"`
	OrderTotal   int64  `json:"order_total,omitempty"`
	Timestamp    string `json:"timestamp"`
}

var validEventTypes = map[string]bool{
	"page_view":        true,
	"product_view":     true,
	"add_to_cart":      true,
	"remove_from_cart": true,
	"checkout_start":   true,
	"purchase":         true,
	"session_start":    true,
}

type CollectHandler struct {
	publisher *rabbitmq.Publisher
}

func NewCollectHandler(pub *rabbitmq.Publisher) *CollectHandler {
	return &CollectHandler{publisher: pub}
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
