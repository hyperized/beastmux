package sourcemgr

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestNextBackoffDoublesUntilCap(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		current    time.Duration
		maxBackoff time.Duration
		want       time.Duration
	}{
		{name: "doubles below cap", current: 1 * time.Second, maxBackoff: 30 * time.Second, want: 2 * time.Second},
		{name: "doubled equals cap", current: 15 * time.Second, maxBackoff: 30 * time.Second, want: 30 * time.Second},
		{name: "doubled exceeds cap", current: 20 * time.Second, maxBackoff: 30 * time.Second, want: 30 * time.Second},
		{name: "already at cap", current: 30 * time.Second, maxBackoff: 30 * time.Second, want: 30 * time.Second},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			if got := nextBackoff(testCase.current, testCase.maxBackoff); got != testCase.want {
				t.Errorf("nextBackoff(%v, %v) = %v, want %v", testCase.current, testCase.maxBackoff, got, testCase.want)
			}
		})
	}
}

func TestSleepCtxFiresAfterDelay(t *testing.T) {
	t.Parallel()

	start := time.Now()

	if err := sleepCtx(t.Context(), 10*time.Millisecond); err != nil {
		t.Fatalf("sleepCtx returned %v, want nil", err)
	}

	if elapsed := time.Since(start); elapsed < 10*time.Millisecond {
		t.Errorf("sleepCtx woke too early: %v < 10ms", elapsed)
	}
}

func TestSleepCtxReturnsImmediatelyOnZeroDelay(t *testing.T) {
	t.Parallel()

	if err := sleepCtx(t.Context(), 0); err != nil {
		t.Errorf("sleepCtx(_, 0) = %v, want nil", err)
	}

	if err := sleepCtx(t.Context(), -1*time.Second); err != nil {
		t.Errorf("sleepCtx(_, negative) = %v, want nil", err)
	}
}

func TestSleepCtxCancellationShortCircuits(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	cancel() // cancel before the call so the select picks ctx.Done first.

	err := sleepCtx(ctx, time.Hour)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("sleepCtx after cancel = %v, want context.Canceled", err)
	}
}

func TestNewAppliesDefaults(t *testing.T) {
	t.Parallel()

	source := New("upstream:30005")

	if source.Addr() != "upstream:30005" {
		t.Errorf("Addr() = %q, want %q", source.Addr(), "upstream:30005")
	}

	if source.backoffMin != DefaultBackoffMin {
		t.Errorf("backoffMin = %v, want %v", source.backoffMin, DefaultBackoffMin)
	}

	if source.backoffMax != DefaultBackoffMax {
		t.Errorf("backoffMax = %v, want %v", source.backoffMax, DefaultBackoffMax)
	}

	if source.dialer == nil {
		t.Error("dialer = nil, want default *net.Dialer")
	}

	if source.logger == nil {
		t.Error("logger = nil, want slog.Default()")
	}

	if got := source.Frames(); got != 0 {
		t.Errorf("Frames() = %d, want 0", got)
	}

	if got := source.Drops(); got != 0 {
		t.Errorf("Drops() = %d, want 0", got)
	}
}

func TestWithBackoffValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		minBackoff        time.Duration
		maxBackoff        time.Duration
		expectMinBackoff  time.Duration
		expectMaxBackoff  time.Duration
		usesDefaultsCheck bool
	}{
		{
			name:             "valid pair applied",
			minBackoff:       100 * time.Millisecond,
			maxBackoff:       5 * time.Second,
			expectMinBackoff: 100 * time.Millisecond,
			expectMaxBackoff: 5 * time.Second,
		},
		{
			name:             "zero min ignored",
			minBackoff:       0,
			maxBackoff:       5 * time.Second,
			expectMinBackoff: DefaultBackoffMin,
			expectMaxBackoff: DefaultBackoffMax,
		},
		{
			name:             "negative min ignored",
			minBackoff:       -1 * time.Second,
			maxBackoff:       5 * time.Second,
			expectMinBackoff: DefaultBackoffMin,
			expectMaxBackoff: DefaultBackoffMax,
		},
		{
			name:             "max below min ignored",
			minBackoff:       5 * time.Second,
			maxBackoff:       1 * time.Second,
			expectMinBackoff: DefaultBackoffMin,
			expectMaxBackoff: DefaultBackoffMax,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			source := New("any", WithBackoff(testCase.minBackoff, testCase.maxBackoff))

			if source.backoffMin != testCase.expectMinBackoff {
				t.Errorf("backoffMin = %v, want %v", source.backoffMin, testCase.expectMinBackoff)
			}

			if source.backoffMax != testCase.expectMaxBackoff {
				t.Errorf("backoffMax = %v, want %v", source.backoffMax, testCase.expectMaxBackoff)
			}
		})
	}
}

func TestWithDialerNilIgnored(t *testing.T) {
	t.Parallel()

	source := New("any", WithDialer(nil))

	if source.dialer == nil {
		t.Error("WithDialer(nil) wiped the default dialer; nil should be ignored")
	}
}

func TestWithLoggerNilIgnored(t *testing.T) {
	t.Parallel()

	source := New("any", WithLogger(nil))

	if source.logger == nil {
		t.Error("WithLogger(nil) wiped the default logger; nil should be ignored")
	}
}
