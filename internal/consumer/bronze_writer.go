package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/merfy/analytics-collector/internal/rabbitmq"
	"github.com/merfy/analytics-collector/internal/util"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	// rawExchange / rawQueue — fanout-топология сырых событий с витрин.
	// Те же имена и параметры объявляет publisher (internal/rabbitmq/publisher.go).
	rawExchange = "analytics.raw"
	rawQueue    = "analytics.raw.consumer"
)

// errClosed возвращается, когда writer уже остановлен: реконнект-цикл по этой
// ошибке выходит, а не пытается подключаться снова.
var errClosed = errors.New("bronze writer closed")

type Event struct {
	ShopID         string  `json:"shop_id"`
	TenantID       string  `json:"tenant_id,omitempty"`
	Type           string  `json:"type"`
	SessionID      string  `json:"session_id"`
	VisitorID      string  `json:"visitor_id,omitempty"`
	PageURL        string  `json:"page_url,omitempty"`
	PageTitle      string  `json:"page_title,omitempty"`
	Referrer       string  `json:"referrer,omitempty"`
	UTMSource      string  `json:"utm_source,omitempty"`
	UTMMedium      string  `json:"utm_medium,omitempty"`
	UTMCampaign    string  `json:"utm_campaign,omitempty"`
	ProductID      string  `json:"product_id,omitempty"`
	ProductName    string      `json:"product_name,omitempty"`
	ProductPriceRaw interface{} `json:"product_price,omitempty"`
	ProductPrice   int64       `json:"-"`
	OrderID        string      `json:"order_id,omitempty"`
	OrderTotalRaw  interface{} `json:"order_total,omitempty"`
	OrderTotal     int64       `json:"-"`
	CostPriceCents *int64  `json:"cost_price_cents,omitempty"`
	CategoryID     *string `json:"category_id,omitempty"`
	Timestamp      string  `json:"timestamp"`
	// Coarse geo stamped at ingest (post-override). Raw IP is never carried here.
	GeoCountry string `json:"geo_country,omitempty"`
	GeoSubject string `json:"geo_subject,omitempty"`
	GeoCity    string `json:"geo_city,omitempty"`
}

type CollectPayload struct {
	ShopID   string  `json:"shop_id"`
	TenantID string  `json:"tenant_id,omitempty"`
	Events   []Event `json:"events"`
}

type BronzeWriter struct {
	pool      *pgxpool.Pool
	connURL   string
	batchSize int
	flushSec  int

	buffer []Event
	mu     sync.Mutex

	// connMu защищает conn/ch/closed: их переписывает реконнект-горутина, а Close()
	// приходит из main при shutdown. Dial под локом НЕ держим — иначе Close
	// заблокируется на таймауте подключения и сломает graceful shutdown.
	connMu sync.Mutex
	conn   *amqp.Connection
	ch     *amqp.Channel
	closed bool

	// alive — признак живости подписки для /health. Без него мёртвый консьюмер
	// снаружи неотличим от здорового сервиса.
	alive atomic.Bool

	cancelFunc context.CancelFunc
}

func NewBronzeWriter(pool *pgxpool.Pool, connURL string, batchSize, flushSec int) (*BronzeWriter, error) {
	bw := &BronzeWriter{
		pool:      pool,
		connURL:   connURL,
		batchSize: batchSize,
		flushSec:  flushSec,
		buffer:    make([]Event, 0, batchSize),
	}
	if err := bw.connect(); err != nil {
		return nil, err
	}
	return bw, nil
}

func (bw *BronzeWriter) connect() error {
	conn, err := amqp.Dial(bw.connURL)
	if err != nil {
		return fmt.Errorf("consumer dial: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return fmt.Errorf("consumer channel: %w", err)
	}
	fail := func(err error) error {
		ch.Close()
		conn.Close()
		return err
	}

	// Топологию объявляем на КАЖДОМ подключении. Раньше её объявлял только
	// publisher; если брокер перезапустится и потеряет очередь, Consume по
	// несуществующей очереди сразу закроет канал и реконнект будет крутиться
	// вечно. Параметры обязаны совпадать с publisher'ом
	// (internal/rabbitmq/publisher.go), иначе брокер ответит PRECONDITION_FAILED.
	if err := ch.ExchangeDeclare(rawExchange, "fanout", true, false, false, false, nil); err != nil {
		return fail(fmt.Errorf("consumer declare exchange: %w", err))
	}
	if _, err := ch.QueueDeclare(rawQueue, true, false, false, false, nil); err != nil {
		return fail(fmt.Errorf("consumer declare queue: %w", err))
	}
	if err := ch.QueueBind(rawQueue, "", rawExchange, false, nil); err != nil {
		return fail(fmt.Errorf("consumer bind queue: %w", err))
	}
	if err := ch.Qos(100, 0, false); err != nil {
		return fail(fmt.Errorf("consumer qos: %w", err))
	}

	bw.connMu.Lock()
	defer bw.connMu.Unlock()
	if bw.closed {
		ch.Close()
		conn.Close()
		return errClosed
	}
	// Прошлые conn/ch после обрыва остаются полуживыми объектами — закрываем,
	// чтобы не течь сокетами при каждом переподключении.
	if bw.ch != nil {
		bw.ch.Close()
	}
	if bw.conn != nil {
		bw.conn.Close()
	}
	bw.conn = conn
	bw.ch = ch
	return nil
}

func (bw *BronzeWriter) Start(ctx context.Context) error {
	msgs, err := bw.consume()
	if err != nil {
		return fmt.Errorf("consume: %w", err)
	}

	ctx, cancel := context.WithCancel(ctx)
	bw.cancelFunc = cancel

	bw.alive.Store(true)

	consumeDone := make(chan struct{})
	go func() {
		defer close(consumeDone)
		bw.consumeLoop(ctx, msgs)
	}()

	// Флаш вынесен в отдельную горутину. Раньше тикер сидел в том же select, что и
	// доставки: на время переподключения (до 30 с между попытками) буфер вообще не
	// сбрасывался бы в БД, и уже подтверждённые события висели бы в памяти.
	go func() {
		ticker := time.NewTicker(time.Duration(bw.flushSec) * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-consumeDone:
				// Финальный флаш — только после остановки консьюмера, иначе можно
				// потерять батч, который тот успел заакать уже после нашего флаша.
				bw.flush(context.Background())
				return
			case <-ticker.C:
				bw.flush(ctx)
			}
		}
	}()

	slog.Info("bronze writer started", "batch_size", bw.batchSize, "flush_sec", bw.flushSec, "queue", rawQueue)
	return nil
}

// Alive сообщает /health, жива ли подписка на analytics.raw.consumer.
func (bw *BronzeWriter) Alive() bool {
	return bw.alive.Load()
}

func (bw *BronzeWriter) consume() (<-chan amqp.Delivery, error) {
	bw.connMu.Lock()
	ch := bw.ch
	closed := bw.closed
	bw.connMu.Unlock()
	if closed || ch == nil {
		return nil, errClosed
	}
	return ch.Consume(rawQueue, "", false, false, false, false, nil)
}

// consumeLoop обрабатывает доставки и переподключается, когда брокер закрывает канал.
func (bw *BronzeWriter) consumeLoop(ctx context.Context, msgs <-chan amqp.Delivery) {
	defer bw.alive.Store(false)

	for {
		if !bw.drain(ctx, msgs) {
			return // штатное завершение по ctx — реконнектиться не надо
		}

		bw.alive.Store(false)
		slog.Error("bronze writer delivery channel closed, consumer is down", "queue", rawQueue)

		next, ok := bw.reconnect(ctx)
		if !ok {
			return
		}
		msgs = next
		bw.alive.Store(true)
		slog.Info("bronze writer consumer restored", "queue", rawQueue)
	}
}

// drain читает доставки, пока канал жив. Возвращает true, если канал доставки
// закрыт брокером (нужен реконнект), и false — если пришла отмена по ctx.
func (bw *BronzeWriter) drain(ctx context.Context, msgs <-chan amqp.Delivery) bool {
	for {
		select {
		case <-ctx.Done():
			return false
		case msg, ok := <-msgs:
			if !ok {
				// ВАЖНО: здесь НЕЛЬЗЯ просто выйти из горутины. RabbitMQ закрывает
				// канал доставки при любом обрыве соединения/канала, и раньше на этом
				// месте стоял `return` — горутина умирала молча, без лога и без
				// переподключения. Контейнер оставался healthy, а analytics.raw.consumer
				// копила сообщения при нуле консьюмеров: события с витрин просто
				// перестают доезжать в bronze, и это видно только по пустым графикам.
				return true
			}
			bw.handleMessage(msg)
		}
	}
}

// reconnect переподключается с экспоненциальным бэкоффом и возобновляет Consume.
// Возвращает false, если пора завершаться (ctx отменён или writer закрыт).
func (bw *BronzeWriter) reconnect(ctx context.Context) (<-chan amqp.Delivery, bool) {
	delay := rabbitmq.ReconnectMinDelay
	for attempt := 1; ; attempt++ {
		// Пауза ДО попытки, а не после: обрыв обычно означает, что брокера прямо
		// сейчас нет, и мгновенный ретрай только сожжёт CPU.
		select {
		case <-ctx.Done():
			return nil, false
		case <-time.After(delay):
		}

		msgs, err := bw.reconnectOnce()
		if err == nil {
			return msgs, true
		}
		if errors.Is(err, errClosed) {
			return nil, false
		}
		if rabbitmq.ShouldLogAttempt(attempt) {
			slog.Error("bronze writer reconnect failed", "queue", rawQueue, "attempt", attempt, "retry_in", delay.String(), "error", err)
		}
		delay = rabbitmq.NextReconnectDelay(delay)
	}
}

func (bw *BronzeWriter) reconnectOnce() (<-chan amqp.Delivery, error) {
	if err := bw.connect(); err != nil {
		return nil, err
	}
	return bw.consume()
}

func (bw *BronzeWriter) handleMessage(msg amqp.Delivery) {
	var payload CollectPayload
	if err := json.Unmarshal(msg.Body, &payload); err != nil {
		slog.Error("unmarshal event", "error", err)
		msg.Nack(false, false)
		return
	}

	bw.mu.Lock()
	for i := range payload.Events {
		e := payload.Events[i]
		if e.ShopID == "" {
			e.ShopID = payload.ShopID
		}
		if e.TenantID == "" {
			e.TenantID = payload.TenantID
		}
		e.ProductPrice = util.ToInt64Price(e.ProductPriceRaw)
		e.OrderTotal = util.ToInt64Price(e.OrderTotalRaw)
		bw.buffer = append(bw.buffer, e)
	}
	needFlush := len(bw.buffer) >= bw.batchSize
	bw.mu.Unlock()

	msg.Ack(false)

	if needFlush {
		bw.flush(context.Background())
	}
}

func (bw *BronzeWriter) flush(ctx context.Context) {
	bw.mu.Lock()
	if len(bw.buffer) == 0 {
		bw.mu.Unlock()
		return
	}
	batch := bw.buffer
	bw.buffer = make([]Event, 0, bw.batchSize)
	bw.mu.Unlock()

	if err := bw.insertBatch(ctx, batch); err != nil {
		slog.Error("batch insert failed", "error", err, "count", len(batch))
		// Put back in buffer for retry
		bw.mu.Lock()
		bw.buffer = append(batch, bw.buffer...)
		bw.mu.Unlock()
		return
	}

	slog.Info("batch inserted", "count", len(batch))
}

func (bw *BronzeWriter) insertBatch(ctx context.Context, events []Event) error {
	if len(events) == 0 {
		return nil
	}

	var b strings.Builder
	b.WriteString(`INSERT INTO bronze.events (
		shop_id, tenant_id, event_type, session_id, visitor_id,
		page_url, page_title, referrer,
		utm_source, utm_medium, utm_campaign,
		product_id, product_name, product_price_cents,
		order_id, order_total_cents, event_timestamp,
		cost_price_cents, category_id,
		geo_country, geo_subject, geo_city
	) VALUES `)

	const colCount = 22
	args := make([]any, 0, len(events)*colCount)
	for i, e := range events {
		if i > 0 {
			b.WriteString(",")
		}
		base := i * colCount
		fmt.Fprintf(&b, "($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)",
			base+1, base+2, base+3, base+4, base+5, base+6, base+7,
			base+8, base+9, base+10, base+11, base+12, base+13, base+14,
			base+15, base+16, base+17, base+18, base+19, base+20, base+21, base+22)

		ts, _ := time.Parse(time.RFC3339, e.Timestamp)
		if ts.IsZero() {
			ts = time.Now()
		}

		args = append(args,
			e.ShopID, nilIfEmpty(e.TenantID), e.Type, e.SessionID, nilIfEmpty(e.VisitorID),
			nilIfEmpty(e.PageURL), nilIfEmpty(e.PageTitle), nilIfEmpty(e.Referrer),
			nilIfEmpty(e.UTMSource), nilIfEmpty(e.UTMMedium), nilIfEmpty(e.UTMCampaign),
			nilIfEmpty(e.ProductID), nilIfEmpty(e.ProductName), nilIfZero(e.ProductPrice),
			nilIfEmpty(e.OrderID), nilIfZero(e.OrderTotal), ts,
			e.CostPriceCents, e.CategoryID,
			nilIfEmpty(e.GeoCountry), nilIfEmpty(e.GeoSubject), nilIfEmpty(e.GeoCity),
		)
	}

	_, err := bw.pool.Exec(ctx, b.String(), args...)
	return err
}

func nilIfEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func nilIfZero(n int64) *int64 {
	if n == 0 {
		return nil
	}
	return &n
}

func (bw *BronzeWriter) Close() {
	if bw.cancelFunc != nil {
		bw.cancelFunc()
	}
	bw.connMu.Lock()
	defer bw.connMu.Unlock()
	bw.closed = true
	bw.alive.Store(false)
	if bw.ch != nil {
		bw.ch.Close()
	}
	if bw.conn != nil {
		bw.conn.Close()
	}
}
