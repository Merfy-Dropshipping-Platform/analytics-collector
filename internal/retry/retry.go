// Package retry — повтор СТАРТОВЫХ подключений с бэкоффом.
//
// Причина существования — гонка на загрузке хоста. Сервер под Coolify
// перезагружается примерно раз в неделю под апгрейды ядра, и контейнер
// коллектора регулярно стартует раньше RabbitMQ. Одиночный Dial в этот момент
// возвращает connection refused, процесс выходит фатально, а политика
// restart: unless-stopped поднимает его снова — за одну загрузку набегало
// 6-10 рестартов (76 стартов за жизнь контейнера, из них 66 умерли на дозвоне
// до брокера).
//
// Это не только шум в статистике: Coolify считает крэш-рестарты сам и после
// седьмого метит ресурс exited:unhealthy, переставая поднимать его молча. То
// есть сервис жил в одной неудачной загрузке от тихого исчезновения.
//
// Реконнект уже установленного соединения живёт в internal/rabbitmq — здесь
// только холодный старт, но политика бэкоффа берётся оттуда же, чтобы
// поведение при недоступном брокере было одним и тем же.
package retry

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// Policy описывает, как долго и с какими паузами повторять попытку.
type Policy struct {
	// Budget — суммарный потолок ожидания. Нужен, чтобы битый URL падал
	// громко, а не висел вечно: исчерпали — возвращаем последнюю ошибку.
	// Budget <= 0 означает «без повторов», одна попытка.
	Budget time.Duration
	// Next вычисляет паузу перед следующей попыткой по предыдущей паузе.
	// Обязателен.
	Next func(time.Duration) time.Duration
	// ShouldLog прореживает лог неудачных попыток. nil — логировать все.
	ShouldLog func(attempt int) bool
}

// Dial повторяет dial, пока он не удастся, не кончится бюджет или не отменят
// ctx. Возвращает первое успешное значение либо последнюю ошибку, обёрнутую
// вместе с числом попыток, — по ней в логе видно, сколько сервис ждал.
func Dial[T any](ctx context.Context, what string, p Policy, dial func() (T, error)) (T, error) {
	var zero T
	if p.Next == nil {
		return zero, fmt.Errorf("retry.Dial(%s): policy без Next", what)
	}

	deadline := time.Now().Add(p.Budget)
	var delay time.Duration

	for attempt := 1; ; attempt++ {
		v, err := dial()
		if err == nil {
			// Логируем только когда повторы реально были: на здоровом старте
			// строка лишняя, а после ребута по ней видно длину гонки.
			if attempt > 1 {
				slog.Info("startup dial succeeded after retries", "what", what, "attempts", attempt)
			}
			return v, nil
		}

		delay = p.Next(delay)
		// Спать дольше остатка бюджета бессмысленно — выходим сразу, не
		// растягивая падение на лишнюю паузу.
		if !time.Now().Add(delay).Before(deadline) {
			return zero, fmt.Errorf("%s: не поднялся за %s (%d попыток): %w", what, p.Budget, attempt, err)
		}

		if p.ShouldLog == nil || p.ShouldLog(attempt) {
			slog.Warn("startup dial failed, retrying",
				"what", what, "attempt", attempt, "retry_in", delay.String(), "error", err)
		}

		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return zero, ctx.Err()
		}
	}
}
