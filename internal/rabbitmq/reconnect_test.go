package rabbitmq

import (
	"testing"
	"time"
)

func TestNextReconnectDelayGrowsExponentiallyAndCaps(t *testing.T) {
	d := NextReconnectDelay(0)
	if d != ReconnectMinDelay {
		t.Fatalf("first delay must be ReconnectMinDelay, got %s", d)
	}

	// Задержка обязана расти: иначе лежащий брокер получает горячий цикл Dial.
	prev := d
	for i := 0; i < 20; i++ {
		next := NextReconnectDelay(prev)
		if next < prev {
			t.Fatalf("delay must never shrink: %s -> %s", prev, next)
		}
		if next > ReconnectMaxDelay {
			t.Fatalf("delay must stay under the cap %s, got %s", ReconnectMaxDelay, next)
		}
		prev = next
	}
	if prev != ReconnectMaxDelay {
		t.Fatalf("delay must saturate at %s, got %s", ReconnectMaxDelay, prev)
	}
}

func TestNextReconnectDelayNeverZero(t *testing.T) {
	for _, in := range []time.Duration{-time.Second, 0, time.Nanosecond} {
		if got := NextReconnectDelay(in); got <= 0 {
			t.Fatalf("NextReconnectDelay(%s) = %s, must be positive to avoid a hot loop", in, got)
		}
	}
}

func TestShouldLogAttemptThinsOutRepeats(t *testing.T) {
	// Первые попытки видно целиком — по ним диагностируют причину обрыва.
	for attempt := 1; attempt <= 3; attempt++ {
		if !ShouldLogAttempt(attempt) {
			t.Fatalf("attempt %d must be logged", attempt)
		}
	}
	// Дальше — прореживание, чтобы часовой простой брокера не утопил остальные логи.
	if ShouldLogAttempt(4) || ShouldLogAttempt(9) {
		t.Fatal("attempts past the first few must be thinned out")
	}
	if !ShouldLogAttempt(10) || !ShouldLogAttempt(100) {
		t.Fatal("every 10th attempt must still surface in the log")
	}
}
