package handler

import (
	"encoding/json"
	"testing"
)

// T072: CollectEvent struct must have CostPriceCents and CategoryID fields
func TestCollectEventHasCostPriceAndCategory(t *testing.T) {
	cp := int64(2500)
	cat := "cat-hats"
	e := CollectEvent{
		Type:           "purchase",
		SessionID:      "sess-1",
		OrderID:        "ord-1",
		OrderTotal:     5000,
		CostPriceCents: &cp,
		CategoryID:     &cat,
		Timestamp:      "2026-03-25T12:00:00Z",
	}

	if e.CostPriceCents == nil || *e.CostPriceCents != 2500 {
		t.Fatal("CostPriceCents should be 2500")
	}
	if e.CategoryID == nil || *e.CategoryID != "cat-hats" {
		t.Fatal("CategoryID should be cat-hats")
	}
}

// T072: CollectEvent JSON deserialization with new fields
func TestCollectEventJSONDeserialization(t *testing.T) {
	raw := `{
		"type": "purchase",
		"session_id": "sess-1",
		"order_id": "ord-1",
		"order_total": 10000,
		"cost_price_cents": 7500,
		"category_id": "cat-electronics",
		"timestamp": "2026-03-25T12:00:00Z"
	}`

	var e CollectEvent
	if err := json.Unmarshal([]byte(raw), &e); err != nil {
		t.Fatal(err)
	}

	if e.CostPriceCents == nil || *e.CostPriceCents != 7500 {
		t.Fatalf("CostPriceCents should be 7500, got %v", e.CostPriceCents)
	}
	if e.CategoryID == nil || *e.CategoryID != "cat-electronics" {
		t.Fatalf("CategoryID should be cat-electronics, got %v", e.CategoryID)
	}
}

// T072: New fields omitted when nil
func TestCollectEventOmitsNilFields(t *testing.T) {
	e := CollectEvent{
		Type:      "page_view",
		SessionID: "sess-1",
		Timestamp: "2026-03-25T12:00:00Z",
	}

	data, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}

	var m map[string]interface{}
	json.Unmarshal(data, &m)

	if _, ok := m["cost_price_cents"]; ok {
		t.Fatal("cost_price_cents should be omitted when nil")
	}
	if _, ok := m["category_id"]; ok {
		t.Fatal("category_id should be omitted when nil")
	}
}

// T072: Full CollectRequest round-trip
func TestCollectRequestRoundTrip(t *testing.T) {
	raw := `{
		"shop_id": "shop-42",
		"events": [
			{
				"type": "purchase",
				"session_id": "sess-1",
				"order_id": "ord-1",
				"order_total": 10000,
				"cost_price_cents": 5000,
				"category_id": "cat-1",
				"timestamp": "2026-03-25T12:00:00Z"
			},
			{
				"type": "page_view",
				"session_id": "sess-2",
				"page_url": "/products",
				"timestamp": "2026-03-25T12:00:01Z"
			}
		]
	}`

	var req CollectRequest
	if err := json.Unmarshal([]byte(raw), &req); err != nil {
		t.Fatal(err)
	}

	if len(req.Events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(req.Events))
	}

	// First event (purchase) has new fields
	if req.Events[0].CostPriceCents == nil {
		t.Fatal("first event should have CostPriceCents")
	}
	if *req.Events[0].CostPriceCents != 5000 {
		t.Fatal("first event CostPriceCents should be 5000")
	}

	// Second event (page_view) has nil new fields
	if req.Events[1].CostPriceCents != nil {
		t.Fatal("second event should have nil CostPriceCents")
	}
	if req.Events[1].CategoryID != nil {
		t.Fatal("second event should have nil CategoryID")
	}
}
