package consumer

import (
	"context"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Тот же класс дефекта, что в RPC-сервере: на проде analytics.raw.consumer
// набрала 24 сообщения при нуле консьюмеров, и события с витрин не доезжали
// в bronze до рестарта контейнера.
func TestDrainRequestsReconnectWhenDeliveryChannelClosed(t *testing.T) {
	bw := &BronzeWriter{}

	msgs := make(chan amqp.Delivery)
	close(msgs) // так выглядит обрыв соединения/канала со стороны RabbitMQ

	done := make(chan bool, 1)
	go func() { done <- bw.drain(context.Background(), msgs) }()

	select {
	case needReconnect := <-done:
		if !needReconnect {
			t.Fatal("closed delivery channel must request a reconnect, not a silent exit")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("drain did not return on a closed delivery channel")
	}
}

func TestDrainExitsWithoutReconnectOnContextCancel(t *testing.T) {
	bw := &BronzeWriter{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	msgs := make(chan amqp.Delivery) // открыт и пуст — сработать может только ctx.Done

	done := make(chan bool, 1)
	go func() { done <- bw.drain(ctx, msgs) }()

	select {
	case needReconnect := <-done:
		if needReconnect {
			t.Fatal("graceful shutdown must not trigger a reconnect")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("drain did not return on context cancel")
	}
}

func TestReconnectBailsOutOnCancelledContext(t *testing.T) {
	bw := &BronzeWriter{connURL: "amqp://127.0.0.1:1/"}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan bool, 1)
	go func() {
		_, ok := bw.reconnect(ctx)
		done <- ok
	}()

	select {
	case ok := <-done:
		if ok {
			t.Fatal("reconnect must report failure once the context is cancelled")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("reconnect ignored the cancelled context")
	}
}

func TestBronzeWriterIsNotAliveBeforeStartAndAfterClose(t *testing.T) {
	bw := &BronzeWriter{}
	if bw.Alive() {
		t.Fatal("writer must not report a live subscription before Start")
	}
	bw.alive.Store(true)
	bw.Close()
	if bw.Alive() {
		t.Fatal("writer must not report a live subscription after Close")
	}
}
