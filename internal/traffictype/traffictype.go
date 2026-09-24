// Package traffictype — три значения пометки трафика (человек/робот/свои) и
// их полный набор в одном месте. internal/handler проставляет пометку при
// приёме (/collect), internal/consumer пишет её в bronze.events — общее место
// не даёт строкам "bot"/"internal"/"human" разъехаться по двум пакетам.
// Значения и порядок — README, раздел «Пометка трафика»; CHECK-констрейнт
// chk_events_traffic_type — migrations/016_traffic_type.sql.
package traffictype

const (
	Human    = "human"
	Bot      = "bot"
	Internal = "internal"
)

// All — полный набор допустимых значений (данные, не цепочка if). Имя не
// врёт: здесь ровно три значения из CHECK-констрейнта, включая Human — в
// отличие от промежуточных наборов, которые проверяют лишь "не default".
var All = map[string]bool{
	Human:    true,
	Bot:      true,
	Internal: true,
}
