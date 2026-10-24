package consumer

import (
	"encoding/json"
	"testing"
)

// T071: Event struct must have CostPriceCents and CategoryID fields
func TestEventStructHasCostPriceAndCategory(t *testing.T) {
	e := Event{
		ShopID:         "shop-1",
		Type:           "purchase",
		SessionID:      "sess-1",
		OrderID:        "ord-1",
		OrderTotal:     10000,
		CostPriceCents: ptrInt64(5000),
		CategoryID:     ptrString("cat-shoes"),
		Timestamp:      "2026-03-25T12:00:00Z",
	}

	if e.CostPriceCents == nil || *e.CostPriceCents != 5000 {
		t.Fatal("CostPriceCents should be 5000")
	}
	if e.CategoryID == nil || *e.CategoryID != "cat-shoes" {
		t.Fatal("CategoryID should be cat-shoes")
	}
}

// T071: Nil fields should be omitted in JSON
func TestEventStructOmitsNilFields(t *testing.T) {
	e := Event{
		ShopID:    "shop-1",
		Type:      "page_view",
		SessionID: "sess-1",
		Timestamp: "2026-03-25T12:00:00Z",
	}

	data, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}

	str := string(data)
	if containsKey(str, "cost_price_cents") {
		t.Fatal("cost_price_cents should be omitted when nil")
	}
	if containsKey(str, "category_id") {
		t.Fatal("category_id should be omitted when nil")
	}
}

// T071: Fields should serialize to correct JSON keys
func TestEventStructJSONKeys(t *testing.T) {
	cp := int64(3000)
	cat := "cat-123"
	e := Event{
		ShopID:         "shop-1",
		Type:           "purchase",
		SessionID:      "sess-1",
		CostPriceCents: &cp,
		CategoryID:     &cat,
		Timestamp:      "2026-03-25T12:00:00Z",
	}

	data, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}

	var m map[string]interface{}
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatal(err)
	}

	if v, ok := m["cost_price_cents"]; !ok {
		t.Fatal("cost_price_cents key missing in JSON")
	} else if int64(v.(float64)) != 3000 {
		t.Fatalf("cost_price_cents should be 3000, got %v", v)
	}

	if v, ok := m["category_id"]; !ok {
		t.Fatal("category_id key missing in JSON")
	} else if v.(string) != "cat-123" {
		t.Fatalf("category_id should be cat-123, got %v", v)
	}
}

// T071: CollectPayload should properly unmarshal events with new fields
func TestCollectPayloadUnmarshalWithNewFields(t *testing.T) {
	raw := `{
		"shop_id": "shop-1",
		"events": [{
			"type": "purchase",
			"session_id": "sess-1",
			"order_id": "ord-1",
			"order_total": 10000,
			"cost_price_cents": 5000,
			"category_id": "cat-shoes",
			"timestamp": "2026-03-25T12:00:00Z"
		}]
	}`

	var payload CollectPayload
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		t.Fatal(err)
	}

	if len(payload.Events) != 1 {
		t.Fatal("expected 1 event")
	}

	e := payload.Events[0]
	if e.CostPriceCents == nil || *e.CostPriceCents != 5000 {
		t.Fatal("CostPriceCents should be 5000")
	}
	if e.CategoryID == nil || *e.CategoryID != "cat-shoes" {
		t.Fatal("CategoryID should be cat-shoes")
	}
}

func containsKey(jsonStr, key string) bool {
	var m map[string]interface{}
	json.Unmarshal([]byte(jsonStr), &m)
	_, ok := m[key]
	return ok
}

func ptrInt64(v int64) *int64   { return &v }
func ptrString(v string) *string { return &v }
