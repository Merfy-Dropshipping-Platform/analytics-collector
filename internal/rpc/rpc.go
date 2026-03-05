package rpc

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Handler func(ctx context.Context, pool *pgxpool.Pool, payload json.RawMessage) (any, error)

type Server struct {
	pool     *pgxpool.Pool
	conn     *amqp.Connection
	ch       *amqp.Channel
	connURL  string
	handlers map[string]Handler
}

func NewServer(pool *pgxpool.Pool, connURL string) (*Server, error) {
	s := &Server{
		pool:     pool,
		connURL:  connURL,
		handlers: make(map[string]Handler),
	}
	if err := s.connect(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Server) connect() error {
	conn, err := amqp.Dial(s.connURL)
	if err != nil {
		return fmt.Errorf("rpc dial: %w", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return fmt.Errorf("rpc channel: %w", err)
	}
	_, err = ch.QueueDeclare("analytics_queue", true, false, false, false, nil)
	if err != nil {
		ch.Close()
		conn.Close()
		return fmt.Errorf("rpc queue declare: %w", err)
	}
	if err := ch.Qos(10, 0, false); err != nil {
		ch.Close()
		conn.Close()
		return fmt.Errorf("rpc qos: %w", err)
	}
	s.conn = conn
	s.ch = ch
	return nil
}

func (s *Server) Register(pattern string, handler Handler) {
	s.handlers[pattern] = handler
}

type RPCRequest struct {
	Pattern string          `json:"pattern"`
	Data    json.RawMessage `json:"data"`
	ID      string          `json:"id,omitempty"`
}

func (s *Server) Start(ctx context.Context) error {
	msgs, err := s.ch.Consume("analytics_queue", "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("rpc consume: %w", err)
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-msgs:
				if !ok {
					return
				}
				s.handleMessage(ctx, msg)
			}
		}
	}()

	slog.Info("rpc server started", "handlers", len(s.handlers))
	return nil
}

func (s *Server) handleMessage(ctx context.Context, msg amqp.Delivery) {
	var req RPCRequest
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		slog.Error("rpc unmarshal", "error", err)
		s.reply(msg, map[string]any{"success": false, "error": "invalid request"})
		msg.Ack(false)
		return
	}

	handler, ok := s.handlers[req.Pattern]
	if !ok {
		slog.Warn("rpc unknown pattern", "pattern", req.Pattern)
		s.reply(msg, map[string]any{"success": false, "error": fmt.Sprintf("unknown pattern: %s", req.Pattern)})
		msg.Ack(false)
		return
	}

	result, err := handler(ctx, s.pool, req.Data)
	if err != nil {
		slog.Error("rpc handler error", "pattern", req.Pattern, "error", err)
		s.reply(msg, map[string]any{"success": false, "error": err.Error()})
		msg.Ack(false)
		return
	}

	s.reply(msg, map[string]any{"success": true, "data": result})
	msg.Ack(false)
}

func (s *Server) reply(msg amqp.Delivery, response any) {
	if msg.ReplyTo == "" {
		return
	}

	body, err := json.Marshal(response)
	if err != nil {
		slog.Error("rpc marshal reply", "error", err)
		return
	}

	s.ch.PublishWithContext(context.Background(),
		"", msg.ReplyTo, false, false,
		amqp.Publishing{
			ContentType:   "application/json",
			CorrelationId: msg.CorrelationId,
			Body:          body,
		},
	)
}

func (s *Server) Close() {
	if s.ch != nil {
		s.ch.Close()
	}
	if s.conn != nil {
		s.conn.Close()
	}
}
