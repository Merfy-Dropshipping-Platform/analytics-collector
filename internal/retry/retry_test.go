package retry

import (
	"context"
	"errors"
	"testing"
	"time"
)

// fastPolicy — бэкофф в миллисекунду: тесты проверяют логику повторов, а не
// реальные паузы.
func fastPolicy(budget time.Duration) Policy {
	return Policy{
		Budget: budget,
		Next:   func(time.Duration) time.Duration { return time.Millisecond },
	}
}

func TestDialReturnsImmediatelyOnSuccess(t *testing.T) {
	calls := 0
	got, err := Dial(context.Background(), "ok", fastPolicy(time.Second), func() (int, error) {
		calls++
		return 42, nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != 42 {
		t.Fatalf("value = %d, want 42", got)
	}
	if calls != 1 {
		t.Fatalf("удачный старт не должен ретраить: calls = %d", calls)
	}
}

func TestDialRetriesUntilBrokerComesUp(t *testing.T) {
	// Ровно тот сценарий, ради которого пакет и написан: брокер поднимается
	// через несколько секунд после нас.
	calls := 0
	got, err := Dial(context.Background(), "rabbitmq", fastPolicy(time.Second), func() (string, error) {
		calls++
		if calls < 4 {
			return "", errors.New("connection refused")
		}
		return "connected", nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "connected" {
		t.Fatalf("value = %q, want %q", got, "connected")
	}
	if calls != 4 {
		t.Fatalf("calls = %d, want 4", calls)
	}
}

func TestDialGivesUpWhenBudgetRunsOut(t *testing.T) {
	// Битый URL обязан падать громко, а не висеть молча.
	want := errors.New("no such host")
	calls := 0
	_, err := Dial(context.Background(), "rabbitmq", fastPolicy(time.Millisecond/2), func() (int, error) {
		calls++
		return 0, want
	})
	if err == nil {
		t.Fatal("исчерпанный бюджет обязан вернуть ошибку")
	}
	if !errors.Is(err, want) {
		t.Fatalf("причина должна быть сохранена в цепочке: %v", err)
	}
	if calls != 1 {
		t.Fatalf("бюджет меньше первой паузы — попытка должна быть одна, calls = %d", calls)
	}
}

func TestDialStopsOnContextCancel(t *testing.T) {
	// Сигнал остановки во время ожидания брокера не должен игнорироваться.
	ctx, cancel := context.WithCancel(context.Background())
	p := Policy{
		Budget: time.Minute,
		Next:   func(time.Duration) time.Duration { return 50 * time.Millisecond },
	}
	time.AfterFunc(10*time.Millisecond, cancel)

	_, err := Dial(ctx, "rabbitmq", p, func() (int, error) {
		return 0, errors.New("connection refused")
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

func TestDialThinsOutLogsButNeverSkipsAttempts(t *testing.T) {
	// ShouldLog влияет только на лог: пропуск записи не должен пропускать
	// саму попытку подключения.
	logged := 0
	calls := 0
	p := fastPolicy(time.Second)
	p.ShouldLog = func(attempt int) bool {
		logged++
		return attempt == 1
	}
	_, err := Dial(context.Background(), "rabbitmq", p, func() (int, error) {
		calls++
		if calls < 5 {
			return 0, errors.New("connection refused")
		}
		return 1, nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls != 5 {
		t.Fatalf("calls = %d, want 5", calls)
	}
	if logged != 4 {
		t.Fatalf("ShouldLog должен спрашиваться на каждой неудаче: %d", logged)
	}
}

func TestDialRejectsPolicyWithoutNext(t *testing.T) {
	// Без Next пауза была бы нулевой — это горячий цикл Dial по брокеру.
	_, err := Dial(context.Background(), "rabbitmq", Policy{Budget: time.Second}, func() (int, error) {
		return 0, errors.New("connection refused")
	})
	if err == nil {
		t.Fatal("policy без Next обязана отвергаться")
	}
}
