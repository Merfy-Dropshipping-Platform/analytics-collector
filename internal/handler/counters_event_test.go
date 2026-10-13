package handler

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Событие пересборки после правки счётчика (план блока 11 «Каркаса витрины», задача 4): тело — договор сборщика
// витрин site-gen, ответ кабинету не меняется.

type publishedEvent struct {
	key  string
	body string
}

type fakeContentPublisher struct {
	events []publishedEvent
	err    error
}

func (f *fakeContentPublisher) PublishContentEvent(_ context.Context, key string, body []byte) error {
	f.events = append(f.events, publishedEvent{key: key, body: string(body)})
	return f.err
}

func fixedTime() time.Time { return time.Date(2026, 10, 10, 9, 0, 0, 0, time.UTC) }

func shopIs(id string, err error) PixelShopFinder {
	return func(context.Context, *pgxpool.Pool, json.RawMessage) (string, error) { return id, err }
}

func answerWith(result any, err error) rpcHandler {
	return func(context.Context, *pgxpool.Pool, json.RawMessage) (any, error) { return result, err }
}

var editPayload = json.RawMessage(`{"id":"p-1","tenantId":"org-1"}`)

const wantEvent = `{"v":1,"type":"analytics-counters-change","siteId":"site-1","eventAt":"2026-10-10T09:00:00.000Z","source":"analytics-collector"}`

func TestCountersEventAfterSuccess(t *testing.T) {
	pub := &fakeContentPublisher{}
	wrapped := WithCountersEvent(pub, shopIs("site-1", nil), fixedTime, answerWith(map[string]bool{"success": true}, nil))
	result, err := wrapped(context.Background(), nil, editPayload)
	if err != nil || !reflect.DeepEqual(result, map[string]bool{"success": true}) {
		t.Fatalf("ответ обработчика изменился: %v, %v", result, err)
	}
	if len(pub.events) != 1 || pub.events[0].key != CountersChangeEvent || pub.events[0].body != wantEvent {
		t.Fatalf("событие: %+v", pub.events)
	}
}

func TestCountersEventNotSentWhenHandlerFails(t *testing.T) {
	pub := &fakeContentPublisher{}
	wrapped := WithCountersEvent(pub, shopIs("site-1", nil), fixedTime, answerWith(nil, errors.New("id and tenantId are required")))
	if _, err := wrapped(context.Background(), nil, editPayload); err == nil {
		t.Fatal("ошибка обработчика потерялась")
	}
	if len(pub.events) != 0 {
		t.Fatalf("событие после ошибки: %+v", pub.events)
	}
}

func TestCountersEventPublishFailureKeepsAnswer(t *testing.T) {
	pub := &fakeContentPublisher{err: errors.New("broker down")}
	wrapped := WithCountersEvent(pub, shopIs("site-1", nil), fixedTime, answerWith("ok", nil))
	result, err := wrapped(context.Background(), nil, editPayload)
	if err != nil || result != "ok" || len(pub.events) != 1 {
		t.Fatalf("сбой публикации поменял ответ: %v, %v, %d", result, err, len(pub.events))
	}
}

func TestCountersEventShopNotFound(t *testing.T) {
	pub := &fakeContentPublisher{}
	wrapped := WithCountersEvent(pub, shopIs("", errors.New("no rows in result set")), fixedTime, answerWith("ok", nil))
	result, err := wrapped(context.Background(), nil, editPayload)
	if err != nil || result != "ok" || len(pub.events) != 0 {
		t.Fatalf("без магазина: %v, %v, %+v", result, err, pub.events)
	}
}

func TestFindPixelShopFromCreatePayload(t *testing.T) {
	create := json.RawMessage(`{"shopId":"site-9","tenantId":"org-1","pixel_type":"vk","pixel_id":"VK-RTRG-1-a"}`)
	shop, err := FindPixelShop(context.Background(), nil, create)
	if err != nil || shop != "site-9" {
		t.Fatalf("магазин из запроса создания: %q, %v", shop, err)
	}
	if _, err := FindPixelShop(context.Background(), nil, json.RawMessage(`not json`)); err == nil {
		t.Fatal("битый запрос прошёл")
	}
}
