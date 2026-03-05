package rabbitmq

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Publisher struct {
	conn    *amqp.Connection
	ch      *amqp.Channel
	mu      sync.Mutex
	connURL string
}

func NewPublisher(connURL string) (*Publisher, error) {
	p := &Publisher{connURL: connURL}
	if err := p.connect(); err != nil {
		return nil, err
	}
	return p, nil
}

func (p *Publisher) connect() error {
	conn, err := amqp.Dial(p.connURL)
	if err != nil {
		return fmt.Errorf("rabbitmq dial: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return fmt.Errorf("rabbitmq channel: %w", err)
	}

	// Declare the analytics.raw exchange (fanout)
	if err := ch.ExchangeDeclare("analytics.raw", "fanout", true, false, false, false, nil); err != nil {
		ch.Close()
		conn.Close()
		return fmt.Errorf("declare exchange: %w", err)
	}

	// Declare analytics_queue for RPC
	_, err = ch.QueueDeclare("analytics_queue", true, false, false, false, nil)
	if err != nil {
		ch.Close()
		conn.Close()
		return fmt.Errorf("declare analytics_queue: %w", err)
	}

	// Declare consumer queue bound to the exchange
	_, err = ch.QueueDeclare("analytics.raw.consumer", true, false, false, false, nil)
	if err != nil {
		ch.Close()
		conn.Close()
		return fmt.Errorf("declare consumer queue: %w", err)
	}
	if err := ch.QueueBind("analytics.raw.consumer", "", "analytics.raw", false, nil); err != nil {
		ch.Close()
		conn.Close()
		return fmt.Errorf("bind consumer queue: %w", err)
	}

	p.conn = conn
	p.ch = ch
	return nil
}

func (p *Publisher) Publish(ctx context.Context, body []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.ch == nil || p.ch.IsClosed() {
		slog.Info("rabbitmq reconnecting publisher")
		if err := p.connect(); err != nil {
			return err
		}
	}

	return p.ch.PublishWithContext(ctx,
		"analytics.raw", // exchange
		"",              // routing key
		false, false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		},
	)
}

func (p *Publisher) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ch != nil {
		p.ch.Close()
	}
	if p.conn != nil {
		p.conn.Close()
	}
}
