package rpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/merfy/analytics-collector/internal/rabbitmq"
	amqp "github.com/rabbitmq/amqp091-go"
)

// queueName — очередь RPC-запросов (gateway / back-office → analytics).
const queueName = "analytics_queue"

// errClosed возвращается, когда сервер уже остановлен: реконнект-цикл по этой
// ошибке выходит, а не пытается подключаться снова.
var errClosed = errors.New("rpc server closed")

type Handler func(ctx context.Context, pool *pgxpool.Pool, payload json.RawMessage) (any, error)

type Server struct {
	pool     *pgxpool.Pool
	connURL  string
	handlers map[string]Handler

	// mu защищает conn/ch/closed: их переписывает реконнект-горутина, а Close()
	// приходит из main при shutdown. Dial под локом НЕ держим — иначе Close
	// заблокируется на таймауте подключения и сломает graceful shutdown.
	mu     sync.Mutex
	conn   *amqp.Connection
	ch     *amqp.Channel
	closed bool

	// alive — признак живости подписки для /health. Без него мёртвый консьюмер
	// снаружи неотличим от здорового сервиса: HTTP отвечает, matview-loop крутится.
	alive atomic.Bool
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
	fail := func(err error) error {
		ch.Close()
		conn.Close()
		return err
	}
	// Очередь объявляем на КАЖДОМ подключении, а не только на первом: если брокер
	// перезапустился и потерял её, Consume по несуществующей очереди сразу закроет
	// канал — и реконнект будет крутиться вечно. Параметры совпадают с publisher'ом
	// (internal/rabbitmq/publisher.go), иначе брокер ответит PRECONDITION_FAILED.
	if _, err := ch.QueueDeclare(queueName, true, false, false, false, nil); err != nil {
		return fail(fmt.Errorf("rpc queue declare: %w", err))
	}
	if err := ch.Qos(10, 0, false); err != nil {
		return fail(fmt.Errorf("rpc qos: %w", err))
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		ch.Close()
		conn.Close()
		return errClosed
	}
	// Прошлые conn/ch после обрыва остаются полуживыми объектами — закрываем,
	// чтобы не течь сокетами при каждом переподключении.
	if s.ch != nil {
		s.ch.Close()
	}
	if s.conn != nil {
		s.conn.Close()
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
	msgs, err := s.consume()
	if err != nil {
		return fmt.Errorf("rpc consume: %w", err)
	}

	s.alive.Store(true)
	go s.consumeLoop(ctx, msgs)

	slog.Info("rpc server started", "handlers", len(s.handlers), "queue", queueName)
	return nil
}

// Alive сообщает /health, жива ли подписка на analytics_queue.
func (s *Server) Alive() bool {
	return s.alive.Load()
}

func (s *Server) consume() (<-chan amqp.Delivery, error) {
	s.mu.Lock()
	ch := s.ch
	closed := s.closed
	s.mu.Unlock()
	if closed || ch == nil {
		return nil, errClosed
	}
	return ch.Consume(queueName, "", false, false, false, false, nil)
}

// consumeLoop обрабатывает доставки и переподключается, когда брокер закрывает канал.
func (s *Server) consumeLoop(ctx context.Context, msgs <-chan amqp.Delivery) {
	defer s.alive.Store(false)

	for {
		if !s.drain(ctx, msgs) {
			return // штатное завершение по ctx — реконнектиться не надо
		}

		s.alive.Store(false)
		slog.Error("rpc delivery channel closed, consumer is down", "queue", queueName)

		next, ok := s.reconnect(ctx)
		if !ok {
			return
		}
		msgs = next
		s.alive.Store(true)
		slog.Info("rpc consumer restored", "queue", queueName, "handlers", len(s.handlers))
	}
}

// drain читает доставки, пока канал жив. Возвращает true, если канал доставки
// закрыт брокером (нужен реконнект), и false — если пришла отмена по ctx.
func (s *Server) drain(ctx context.Context, msgs <-chan amqp.Delivery) bool {
	for {
		select {
		case <-ctx.Done():
			return false
		case msg, ok := <-msgs:
			if !ok {
				// ВАЖНО: здесь НЕЛЬЗЯ просто выйти из горутины. RabbitMQ закрывает
				// канал доставки при любом обрыве соединения/канала, и раньше на этом
				// месте стоял `return` — горутина умирала молча: ни лога, ни ошибки,
				// ни переподключения. Процесс продолжал выглядеть здоровым (/health
				// = ok, maintenance-loop крутит матвью, контейнер healthy), а
				// analytics_queue росла без консьюмера. На проде это дало ~2 суток
				// мёртвой аналитики, которые вылечил только рестарт контейнера.
				return true
			}
			s.handleMessage(ctx, msg)
		}
	}
}

// reconnect переподключается с экспоненциальным бэкоффом и возобновляет Consume.
// Возвращает false, если пора завершаться (ctx отменён или сервер закрыт).
func (s *Server) reconnect(ctx context.Context) (<-chan amqp.Delivery, bool) {
	delay := rabbitmq.ReconnectMinDelay
	for attempt := 1; ; attempt++ {
		// Пауза ДО попытки, а не после: обрыв обычно означает, что брокера прямо
		// сейчас нет, и мгновенный ретрай только сожжёт CPU.
		select {
		case <-ctx.Done():
			return nil, false
		case <-time.After(delay):
		}

		msgs, err := s.reconnectOnce()
		if err == nil {
			return msgs, true
		}
		if errors.Is(err, errClosed) {
			return nil, false
		}
		if rabbitmq.ShouldLogAttempt(attempt) {
			slog.Error("rpc reconnect failed", "queue", queueName, "attempt", attempt, "retry_in", delay.String(), "error", err)
		}
		delay = rabbitmq.NextReconnectDelay(delay)
	}
}

func (s *Server) reconnectOnce() (<-chan amqp.Delivery, error) {
	if err := s.connect(); err != nil {
		return nil, err
	}
	return s.consume()
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

	// Канал читаем под локом: реконнект мог его подменить.
	s.mu.Lock()
	ch := s.ch
	s.mu.Unlock()
	if ch == nil {
		return
	}

	if err := ch.PublishWithContext(context.Background(),
		"", msg.ReplyTo, false, false,
		amqp.Publishing{
			ContentType:   "application/json",
			CorrelationId: msg.CorrelationId,
			Body:          body,
		},
	); err != nil {
		slog.Error("rpc publish reply", "reply_to", msg.ReplyTo, "error", err)
	}
}

func (s *Server) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	s.alive.Store(false)
	if s.ch != nil {
		s.ch.Close()
	}
	if s.conn != nil {
		s.conn.Close()
	}
}
