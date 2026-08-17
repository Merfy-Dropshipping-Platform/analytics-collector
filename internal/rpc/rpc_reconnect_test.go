package rpc

import (
	"context"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Регрессия на дефект, из-за которого аналитика на проде лежала ~2 суток: при
// закрытии канала доставки горутина просто выходила, и analytics_queue росла
// без консьюмера. drain обязан сообщить наверх, что нужен реконнект.
func TestDrainRequestsReconnectWhenDeliveryChannelClosed(t *testing.T) {
	s := &Server{handlers: make(map[string]Handler)}

	msgs := make(chan amqp.Delivery)
	close(msgs) // так выглядит обрыв соединения/канала со стороны RabbitMQ

	done := make(chan bool, 1)
	go func() { done <- s.drain(context.Background(), msgs) }()

	select {
	case needReconnect := <-done:
		if !needReconnect {
			t.Fatal("closed delivery channel must request a reconnect, not a silent exit")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("drain did not return on a closed delivery channel")
	}
}

// Штатное завершение: реконнект-цикл заводиться не должен.
func TestDrainExitsWithoutReconnectOnContextCancel(t *testing.T) {
	s := &Server{handlers: make(map[string]Handler)}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	msgs := make(chan amqp.Delivery) // открыт и пуст — сработать может только ctx.Done

	done := make(chan bool, 1)
	go func() { done <- s.drain(ctx, msgs) }()

	select {
	case needReconnect := <-done:
		if needReconnect {
			t.Fatal("graceful shutdown must not trigger a reconnect")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("drain did not return on context cancel")
	}
}

// Реконнект не должен ни дозваниваться до брокера, ни зависать, если контекст
// уже отменён — иначе graceful shutdown повиснет на таймауте Dial.
func TestReconnectBailsOutOnCancelledContext(t *testing.T) {
	s := &Server{handlers: make(map[string]Handler), connURL: "amqp://127.0.0.1:1/"}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	type result struct {
		ok bool
	}
	done := make(chan result, 1)
	go func() {
		_, ok := s.reconnect(ctx)
		done <- result{ok: ok}
	}()

	select {
	case r := <-done:
		if r.ok {
			t.Fatal("reconnect must report failure once the context is cancelled")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("reconnect ignored the cancelled context")
	}
}

func TestServerIsNotAliveBeforeStartAndAfterClose(t *testing.T) {
	s := &Server{handlers: make(map[string]Handler)}
	if s.Alive() {
		t.Fatal("server must not report a live subscription before Start")
	}
	s.alive.Store(true)
	s.Close()
	if s.Alive() {
		t.Fatal("server must not report a live subscription after Close")
	}
}
