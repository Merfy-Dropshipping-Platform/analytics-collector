//go:build amqptest

package rabbitmq

import (
	"context"
	"os"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Договор со сборщиком витрин на настоящем брокере: content.events объявляется так же, как в site-gen
// (packages/storefront-builder/src/broker.ts: topic, durable). Другие параметры — брокер ответит PRECONDITION_FAILED, и
// NewPublisher упадёт. Запуск — с брокером стенда site-gen:
//
//	AMQP_TEST_URL=amqp://builder-test:builder-test-only@127.0.0.1:18682 go test -tags amqptest ./internal/rabbitmq/
func TestPublishContentEventReachesBoundQueue(t *testing.T) {
	url := os.Getenv("AMQP_TEST_URL")
	if url == "" {
		t.Skip("AMQP_TEST_URL не задан: тест с брокером пропущен")
	}
	conn, err := amqp.Dial(url)
	if err != nil {
		t.Fatalf("брокер: %v", err)
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("канал: %v", err)
	}
	// Объявление сборщика витрин: assertExchange('content.events', 'topic', { durable: true }).
	if err := ch.ExchangeDeclare(ContentExchange, "topic", true, false, false, false, nil); err != nil {
		t.Fatalf("точка обмена сборщика витрин: %v", err)
	}
	queue, err := ch.QueueDeclare("", false, true, true, false, nil)
	if err != nil {
		t.Fatalf("очередь теста: %v", err)
	}
	if err := ch.QueueBind(queue.Name, "analytics-counters-change", ContentExchange, false, nil); err != nil {
		t.Fatalf("привязка: %v", err)
	}
	messages, err := ch.Consume(queue.Name, "", true, true, false, false, nil)
	if err != nil {
		t.Fatalf("подписка: %v", err)
	}
	pub, err := NewPublisher(url)
	if err != nil {
		t.Fatalf("NewPublisher с content.events: %v", err)
	}
	defer pub.Close()
	body := []byte(`{"v":1,"type":"analytics-counters-change","siteId":"site-1","eventAt":"2026-10-10T09:00:00.000Z","source":"analytics-collector"}`)
	if err := pub.PublishContentEvent(context.Background(), "analytics-counters-change", body); err != nil {
		t.Fatalf("публикация: %v", err)
	}
	select {
	case message := <-messages:
		if string(message.Body) != string(body) || message.DeliveryMode != amqp.Persistent || message.RoutingKey != "analytics-counters-change" {
			t.Fatalf("сообщение: %s, режим %d, ключ %s", message.Body, message.DeliveryMode, message.RoutingKey)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("сообщение не пришло за 5 с")
	}
}
