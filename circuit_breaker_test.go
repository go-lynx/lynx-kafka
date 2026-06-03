package kafka

import (
	"errors"
	"sync"
	"testing"
	"time"
)

// TestCircuitBreaker_ClosedToOpen verifies that the circuit breaker opens after
// reaching the failure threshold while in the Closed state.
func TestCircuitBreaker_ClosedToOpen(t *testing.T) {
	cb := NewCircuitBreaker(3, time.Minute)

	boom := errors.New("boom")
	for i := 0; i < 3; i++ {
		if err := cb.Call(func() error { return boom }); !errors.Is(err, boom) {
			t.Fatalf("attempt %d: expected boom, got %v", i, err)
		}
	}

	if got := cb.GetState(); got != CircuitBreakerOpen {
		t.Fatalf("expected CircuitBreakerOpen after threshold, got %v", got)
	}

	// Next call must be rejected without calling the operation.
	called := false
	if err := cb.Call(func() error { called = true; return nil }); !errors.Is(err, ErrCircuitBreakerOpen) {
		t.Fatalf("expected ErrCircuitBreakerOpen, got %v", err)
	}
	if called {
		t.Fatal("operation must not be called when breaker is open")
	}
}

// TestCircuitBreaker_OpenToHalfOpen verifies that the breaker transitions to
// HalfOpen after the timeout elapses and allows a probe call through.
func TestCircuitBreaker_OpenToHalfOpen(t *testing.T) {
	cb := NewCircuitBreaker(1, 10*time.Millisecond)

	// Trip the breaker.
	_ = cb.Call(func() error { return errors.New("err") })
	if cb.GetState() != CircuitBreakerOpen {
		t.Fatal("expected Open state")
	}

	// Wait for timeout to expire.
	time.Sleep(20 * time.Millisecond)

	// The first call after the timeout should be allowed (transition to HalfOpen).
	called := false
	_ = cb.Call(func() error { called = true; return nil })
	if !called {
		t.Fatal("expected probe call to be allowed after timeout")
	}
}

// TestCircuitBreaker_HalfOpenSuccessClosesBreaker verifies that a successful call
// in HalfOpen increments halfOpenCount and closes the breaker once the limit is
// reached (halfOpenLimit = 5 by default).
func TestCircuitBreaker_HalfOpenSuccessClosesBreaker(t *testing.T) {
	cb := NewCircuitBreaker(1, 10*time.Millisecond)
	_ = cb.Call(func() error { return errors.New("trip") })

	time.Sleep(20 * time.Millisecond)

	// 5 successful calls should close the breaker.
	for i := 0; i < 5; i++ {
		if err := cb.Call(func() error { return nil }); err != nil {
			t.Fatalf("call %d failed: %v", i, err)
		}
	}

	if got := cb.GetState(); got != CircuitBreakerClosed {
		t.Fatalf("expected CircuitBreakerClosed after %d successes, got %v", 5, got)
	}
}

// TestCircuitBreaker_HalfOpenFailureReopens verifies that a single failure in
// HalfOpen trips the breaker back to Open.
func TestCircuitBreaker_HalfOpenFailureReopens(t *testing.T) {
	cb := NewCircuitBreaker(1, 10*time.Millisecond)
	_ = cb.Call(func() error { return errors.New("trip") })

	time.Sleep(20 * time.Millisecond)

	// One failure in HalfOpen should reopen.
	_ = cb.Call(func() error { return errors.New("probe failure") })

	if got := cb.GetState(); got != CircuitBreakerOpen {
		t.Fatalf("expected CircuitBreakerOpen after half-open failure, got %v", got)
	}
}

// TestCircuitBreaker_ConcurrentCallsNoDeadlock verifies that concurrent
// calls do not deadlock or panic under race conditions.
func TestCircuitBreaker_ConcurrentCallsNoDeadlock(t *testing.T) {
	cb := NewCircuitBreaker(5, 10*time.Millisecond)

	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			_ = cb.Call(func() error {
				if n%3 == 0 {
					return errors.New("intentional")
				}
				return nil
			})
		}(i)
	}
	wg.Wait()
	// No assertion needed — test passes if it does not deadlock or panic.
}

// TestCircuitBreaker_GetStats returns a non-nil map with expected keys.
func TestCircuitBreaker_GetStats(t *testing.T) {
	cb := NewCircuitBreaker(5, time.Second)
	stats := cb.GetStats()
	for _, key := range []string{"state", "failure_count", "threshold", "timeout", "last_failure", "last_success", "half_open_count", "half_open_limit"} {
		if _, ok := stats[key]; !ok {
			t.Errorf("GetStats missing key %q", key)
		}
	}
}
