package handler

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/merfy/analytics-collector/internal/geo"
)

// capturePublisher records the exact bytes ServeHTTP would put on RMQ, so we can assert what
// (does not) leave the process — without touching a real broker.
type capturePublisher struct{ bodies [][]byte }

func (c *capturePublisher) Publish(_ context.Context, body []byte) error {
	c.bodies = append(c.bodies, append([]byte(nil), body...))
	return nil
}

const geoBatchBody = `{
  "shop_id": "shopHP",
  "tenant_id": "tenant-XYZ",
  "events": [
    {"type":"page_view","session_id":"s1","page_url":"/p","timestamp":"2026-07-14T10:00:00Z"},
    {"type":"purchase","session_id":"s1","order_id":"o9","product_price":1234,"order_total":5678,"timestamp":"2026-07-14T10:01:00Z"}
  ]
}`

// realClientIP is a public IP we set as r.RemoteAddr — it must NEVER appear in the published
// payload (152-ФЗ), whether geo resolves or not.
const realClientIP = "203.0.113.7"

func postCollect(t *testing.T, h *CollectHandler, remoteAddr string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest("POST", "/collect", strings.NewReader(geoBatchBody))
	req.RemoteAddr = remoteAddr
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	return w
}

func assertNoRawIP(t *testing.T, body []byte) {
	t.Helper()
	if strings.Contains(string(body), realClientIP) {
		t.Errorf("published payload leaked the raw client IP %q: %s", realClientIP, body)
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(body, &m); err != nil {
		t.Fatalf("published body is not valid JSON: %v", err)
	}
	for _, k := range []string{"ip", "ip_address", "remote_addr"} {
		if _, ok := m[k]; ok {
			t.Errorf("published payload contains forbidden key %q", k)
		}
	}
}

// Graceful noop: no geo DB → 204, events published WITHOUT geo, no raw IP, tenant preserved.
func TestServeHTTP_GracefulNoop(t *testing.T) {
	cap := &capturePublisher{}
	h := &CollectHandler{publisher: cap, geo: geo.NewResolver(geo.NoopProvider{})}

	w := postCollect(t, h, realClientIP+":45678")
	if w.Code != 204 {
		t.Fatalf("status = %d; want 204", w.Code)
	}
	if len(cap.bodies) != 1 {
		t.Fatalf("published %d messages; want 1", len(cap.bodies))
	}
	body := cap.bodies[0]
	assertNoRawIP(t, body)

	var req CollectRequest
	if err := json.Unmarshal(body, &req); err != nil {
		t.Fatal(err)
	}
	if req.TenantID != "tenant-XYZ" {
		t.Errorf("tenant_id = %q; want tenant-XYZ (passthrough lost)", req.TenantID)
	}
	if req.ShopID != "shopHP" {
		t.Errorf("shop_id = %q; want shopHP", req.ShopID)
	}
	for i, e := range req.Events {
		if e.GeoCountry != "" || e.GeoSubject != "" || e.GeoCity != "" {
			t.Errorf("event[%d] got geo under noop: %+v", i, e)
		}
	}
	// Round-trip preserved the flexible price fields (product_price / order_total).
	var m map[string]json.RawMessage
	json.Unmarshal(body, &m)
	var evs []map[string]json.RawMessage
	json.Unmarshal(m["events"], &evs)
	if _, ok := evs[1]["product_price"]; !ok {
		t.Error("product_price dropped in re-marshal")
	}
	if _, ok := evs[1]["order_total"]; !ok {
		t.Error("order_total dropped in re-marshal")
	}
}

// Private/garbage RemoteAddr with a real DB still resolves to nothing → graceful, no geo, 204.
func TestServeHTTP_PrivateIPGraceful(t *testing.T) {
	cap := &capturePublisher{}
	// mmdb if available, otherwise noop — either way a private IP yields no geo.
	res := mmdbResolverOrNoop(t)
	h := &CollectHandler{publisher: cap, geo: res}

	w := postCollect(t, h, "10.0.0.5:5000")
	if w.Code != 204 {
		t.Fatalf("status = %d; want 204", w.Code)
	}
	var req CollectRequest
	json.Unmarshal(cap.bodies[0], &req)
	for i, e := range req.Events {
		if e.GeoCountry != "" {
			t.Errorf("event[%d] got geo for a private IP: %q", i, e.GeoCountry)
		}
	}
}

// With a real mmdb, a locatable public IP stamps geo on EVERY event, keeps tenant, no raw IP.
func TestServeHTTP_GeoStampedWithMMDB(t *testing.T) {
	path := os.Getenv("GEO_TEST_MMDB")
	if path == "" {
		t.Skip("GEO_TEST_MMDB not set")
	}
	p, err := geo.OpenMMDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	cap := &capturePublisher{}
	h := &CollectHandler{publisher: cap, geo: geo.NewResolver(p)}

	// 176.57.218.121 → RU / Санкт-Петербург (anchor).
	w := postCollect(t, h, "176.57.218.121:45678")
	if w.Code != 204 {
		t.Fatalf("status = %d; want 204", w.Code)
	}
	body := cap.bodies[0]
	assertNoRawIP(t, body)
	// The RemoteAddr IP is public but is NOT the "realClientIP" const; assert it too.
	if strings.Contains(string(body), "176.57.218.121") {
		t.Errorf("published payload leaked the client IP: %s", body)
	}

	var req CollectRequest
	json.Unmarshal(body, &req)
	if req.TenantID != "tenant-XYZ" {
		t.Errorf("tenant_id lost: %q", req.TenantID)
	}
	if len(req.Events) != 2 {
		t.Fatalf("events = %d; want 2", len(req.Events))
	}
	for i, e := range req.Events {
		if e.GeoCountry != "RU" {
			t.Errorf("event[%d] geo_country = %q; want RU", i, e.GeoCountry)
		}
		if !strings.Contains(e.GeoSubject, "Санкт-Петербург") {
			t.Errorf("event[%d] geo_subject = %q; want contains Санкт-Петербург", i, e.GeoSubject)
		}
	}
}

func mmdbResolverOrNoop(t *testing.T) *geo.Resolver {
	t.Helper()
	path := os.Getenv("GEO_TEST_MMDB")
	if path == "" {
		return geo.NewResolver(geo.NoopProvider{})
	}
	p, err := geo.OpenMMDB(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Close() })
	return geo.NewResolver(p)
}
