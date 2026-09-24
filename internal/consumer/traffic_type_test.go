package consumer

import "testing"

// normalizeTrafficType — защита insertBatch от чужой ошибки: bronze.events имеет
// CHECK (traffic_type IN ('human','bot','internal')) (миграция 016). Пачка
// вставляется ОДНИМ SQL-запросом на весь батч — если хотя бы одна строка не
// пройдёт CHECK, упадёт вся вставка и события зависнут в буфере. Поэтому любое
// значение вне разрешённого набора (пусто, опечатка, старый клиент без поля)
// нормализуется в безопасный 'human' ДО того, как попадёт в args.
func TestNormalizeTrafficType(t *testing.T) {
	cases := map[string]string{
		"bot":      "bot",
		"internal": "internal",
		"human":    "human",
		"":         "human",
		"zzz":      "human",
		"Bot":      "human", // регистр не нормализуем здесь — это уже сделал collect.go
	}
	for in, want := range cases {
		if got := normalizeTrafficType(in); got != want {
			t.Errorf("normalizeTrafficType(%q) = %q; want %q", in, got, want)
		}
	}
}
