package handler

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func doHealth(t *testing.T, h *HealthHandler) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health", nil))

	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("health response is not JSON: %v (%s)", err, rec.Body.String())
	}
	return rec, body
}

func TestHealthOkWhenAllConsumersAlive(t *testing.T) {
	h := NewHealthHandler(
		Probe{Name: "rpc", Alive: func() bool { return true }},
		Probe{Name: "bronze_writer", Alive: func() bool { return true }},
	)
	rec, body := doHealth(t, h)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	// Контракт ответа не должен ломаться — его дергает HEALTHCHECK контейнера.
	if body["status"] != "ok" {
		t.Fatalf(`expected status "ok", got %v`, body["status"])
	}
	if _, ok := body["uptime_seconds"]; !ok {
		t.Fatal("uptime_seconds must stay in the response")
	}
	consumers, ok := body["consumers"].(map[string]any)
	if !ok {
		t.Fatalf("consumers must be an object, got %T", body["consumers"])
	}
	if consumers["rpc"] != true || consumers["bronze_writer"] != true {
		t.Fatalf("both consumers must be reported alive, got %v", consumers)
	}
}

// Главная защита второго уровня: мёртвый консьюмер обязан ронять healthcheck,
// иначе платформа не перезапустит контейнер и простой тянется сутками.
func TestHealthDegradedWhenConsumerIsDead(t *testing.T) {
	h := NewHealthHandler(
		Probe{Name: "rpc", Alive: func() bool { return false }},
		Probe{Name: "bronze_writer", Alive: func() bool { return true }},
	)
	rec, body := doHealth(t, h)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("dead consumer must yield 503 so wget fails, got %d", rec.Code)
	}
	if body["status"] != "degraded" {
		t.Fatalf(`expected status "degraded", got %v`, body["status"])
	}
	consumers := body["consumers"].(map[string]any)
	if consumers["rpc"] != false {
		t.Fatalf("the dead consumer must be named in the response, got %v", consumers)
	}
}

func TestHealthTreatsNilProbeAsDead(t *testing.T) {
	h := NewHealthHandler(Probe{Name: "rpc"})
	rec, _ := doHealth(t, h)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("a probe without an Alive func must not pass as healthy, got %d", rec.Code)
	}
}

func TestHealthWithoutProbesStaysOk(t *testing.T) {
	rec, body := doHealth(t, NewHealthHandler())
	if rec.Code != http.StatusOK || body["status"] != "ok" {
		t.Fatalf("no probes must keep the old behaviour, got %d / %v", rec.Code, body["status"])
	}
}
