package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/merfy/analytics-collector/internal/util"
	amqp "github.com/rabbitmq/amqp091-go"
)

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
}

type CollectPayload struct {
	ShopID   string  `json:"shop_id"`
	TenantID string  `json:"tenant_id,omitempty"`
	Events   []Event `json:"events"`
}

type BronzeWriter struct {
	pool       *pgxpool.Pool
	conn       *amqp.Connection
	ch         *amqp.Channel
	connURL    string
	batchSize  int
	flushSec   int
	buffer     []Event
	mu         sync.Mutex
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

	if err := ch.Qos(100, 0, false); err != nil {
		ch.Close()
		conn.Close()
		return fmt.Errorf("consumer qos: %w", err)
	}

	bw.conn = conn
	bw.ch = ch
	return nil
}

func (bw *BronzeWriter) Start(ctx context.Context) error {
	msgs, err := bw.ch.Consume("analytics.raw.consumer", "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume: %w", err)
	}

	ctx, cancel := context.WithCancel(ctx)
	bw.cancelFunc = cancel

	ticker := time.NewTicker(time.Duration(bw.flushSec) * time.Second)

	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				bw.flush(context.Background())
				return
			case msg, ok := <-msgs:
				if !ok {
					return
				}
				bw.handleMessage(msg)
			case <-ticker.C:
				bw.flush(ctx)
			}
		}
	}()

	slog.Info("bronze writer started", "batch_size", bw.batchSize, "flush_sec", bw.flushSec)
	return nil
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
		cost_price_cents, category_id
	) VALUES `)

	const colCount = 19
	args := make([]any, 0, len(events)*colCount)
	for i, e := range events {
		if i > 0 {
			b.WriteString(",")
		}
		base := i * colCount
		fmt.Fprintf(&b, "($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)",
			base+1, base+2, base+3, base+4, base+5, base+6, base+7,
			base+8, base+9, base+10, base+11, base+12, base+13, base+14,
			base+15, base+16, base+17, base+18, base+19)

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
	if bw.ch != nil {
		bw.ch.Close()
	}
	if bw.conn != nil {
		bw.conn.Close()
	}
}
