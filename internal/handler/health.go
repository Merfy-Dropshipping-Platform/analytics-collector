package handler

import (
	"encoding/json"
	"net/http"
	"time"
)

// Probe — именованная проверка живости фоновой подписки (RPC-сервер, bronze writer).
// Нужна как защита второго уровня: даже если реконнект однажды не сработает,
// мёртвый консьюмер должен уронить healthcheck, а не тихо копить очередь сутками.
type Probe struct {
	Name  string
	Alive func() bool
}

type HealthHandler struct {
	startTime time.Time
	probes    []Probe
}

// NewHealthHandler принимает пробы вариативно: без них поведение остаётся прежним
// (всегда 200 ok), с ними /health начинает отражать состояние AMQP-подписок.
func NewHealthHandler(probes ...Probe) *HealthHandler {
	return &HealthHandler{startTime: time.Now(), probes: probes}
}

func (h *HealthHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	consumers := make(map[string]bool, len(h.probes))
	healthy := true
	for _, p := range h.probes {
		alive := p.Alive != nil && p.Alive()
		consumers[p.Name] = alive
		if !alive {
			healthy = false
		}
	}

	// Отдаём 503, когда хоть одна подписка мертва: HEALTHCHECK в Dockerfile ходит
	// через `wget -qO- .../health`, а wget на 503 выходит с ненулевым кодом →
	// Coolify сам перезапустит контейнер. Порог retries=3 * interval=30s уже даёт
	// ~90 с терпимости, поэтому короткий реконнект (бэкофф стартует с 1 с)
	// healthcheck не роняет — падает только по-настоящему мёртвый консьюмер.
	status := "ok"
	code := http.StatusOK
	if !healthy {
		status = "degraded"
		code = http.StatusServiceUnavailable
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]any{
		"status":         status,
		"uptime_seconds": int(time.Since(h.startTime).Seconds()),
		"consumers":      consumers,
	})
}
