package handler

import "fmt"

// Правила качества данных для запросов прямо к bronze.events — те же, что в представлениях
// миграции 017. Формулы отчётов они не меняют, только отбирают, из каких событий считать.

// humanOnly — трафик (посетители, визиты, просмотры, страницы, источники) считается только по
// людям: роботы и свои (traffic_type 'bot' / 'internal', миграция 016) не входят.
// Деньги (purchase, order_cancel) этим фильтром не режутся — это деньги.
func humanOnly(alias string) string {
	return alias + ".traffic_type = 'human'"
}

// Запас для условий на created_at. Они ничего не отбирают по смыслу — только отсекают партиции
// bronze, нарезанные по времени записи (created_at), а не события.
const (
	// clockAheadSlack — запись раньше события бывает только у спешащих часов браузера.
	clockAheadSlack = "1 day"
	// writeDelaySlack — насколько запись в базу может отстать от события. Простой 13–15.09 — около
	// 35 часов (события копились в очереди); неделя — с запасом. С одними сутками события последних
	// часов окна, записанные после такого простоя, выпали бы из итогов прошедших окон.
	writeDelaySlack = "7 days"
)

// eventWindow — событие попало в окно [from, to) по времени события, а не записи в базу:
// опоздавшая запись (простой 13–15.09) не переезжает на другой день. Условия на created_at —
// только для отсечения партиций, с запасом clockAheadSlack / writeDelaySlack.
func eventWindow(alias, from, to string) string {
	return fmt.Sprintf(
		"%[1]s.event_timestamp >= %[2]s AND %[1]s.event_timestamp < %[3]s"+
			" AND %[1]s.created_at >= %[2]s::timestamptz - interval '%[4]s'"+
			" AND %[1]s.created_at < %[3]s::timestamptz + interval '%[5]s'",
		alias, from, to, clockAheadSlack, writeDelaySlack,
	)
}
