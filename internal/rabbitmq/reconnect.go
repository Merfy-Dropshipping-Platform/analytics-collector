package rabbitmq

import "time"

// Общая политика переподключения для всех AMQP-подписок сервиса (RPC-сервер и
// bronze writer). Живёт рядом с publisher'ом, потому что publisher — эталонный
// (единственный работавший) реконнект в этом сервисе.
const (
	// ReconnectMinDelay — пауза перед первой попыткой. Не ноль: без паузы обрыв
	// превращается в горячий цикл Dial и сервис сам добивает брокер.
	ReconnectMinDelay = 1 * time.Second
	// ReconnectMaxDelay — потолок бэкоффа. Держим невысоким: простой подписки
	// означает мёртвую аналитику, поэтому «долго спать» дороже, чем ретраить.
	ReconnectMaxDelay = 30 * time.Second
)

// NextReconnectDelay удваивает задержку до потолка ReconnectMaxDelay.
func NextReconnectDelay(d time.Duration) time.Duration {
	if d <= 0 {
		return ReconnectMinDelay
	}
	next := d * 2
	if next > ReconnectMaxDelay {
		return ReconnectMaxDelay
	}
	return next
}

// ShouldLogAttempt прореживает лог неудачных попыток: первые три видно целиком
// (по ним понятно, что случилось), дальше — каждая десятая. Иначе лежащий час
// брокер зальёт stdout тысячами одинаковых строк и утопит остальные логи.
func ShouldLogAttempt(attempt int) bool {
	if attempt <= 3 {
		return true
	}
	return attempt%10 == 0
}
