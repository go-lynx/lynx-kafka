package kafka

import (
	"context"
	"fmt"
	"time"
)

// RetryConfig retry configuration
type RetryConfig struct {
	MaxRetries  int           // attempts after the first try
	BackoffTime time.Duration // initial backoff, doubled each retry
	MaxBackoff  time.Duration // cap on the backoff
}

// RetryHandler retry handler
type RetryHandler struct {
	config RetryConfig
}

func NewRetryHandler(config RetryConfig) *RetryHandler {
	return &RetryHandler{
		config: config,
	}
}

// DoWithRetry runs operation, retrying up to MaxRetries times with exponential
// backoff. It aborts early if ctx is done, wrapping the context error with the
// last operation error for diagnostics.
func (rh *RetryHandler) DoWithRetry(ctx context.Context, operation func() error) error {
	var lastErr error
	backoff := rh.config.BackoffTime

	for attempt := 0; attempt <= rh.config.MaxRetries; attempt++ {
		select {
		case <-ctx.Done():
			return wrapRetryContextError(ctx.Err(), lastErr, attempt)
		default:
		}

		if err := operation(); err == nil {
			return nil
		} else {
			lastErr = err
			if attempt == rh.config.MaxRetries {
				break
			}

			select {
			case <-ctx.Done():
				return wrapRetryContextError(ctx.Err(), lastErr, attempt+1)
			case <-time.After(backoff):
			}

			backoff *= 2
			if backoff > rh.config.MaxBackoff {
				backoff = rh.config.MaxBackoff
			}
		}
	}

	return fmt.Errorf("operation failed after %d retries: %w", rh.config.MaxRetries, lastErr)
}

func wrapRetryContextError(ctxErr, lastErr error, attempts int) error {
	if ctxErr == nil {
		return lastErr
	}
	if lastErr == nil {
		return ctxErr
	}
	return fmt.Errorf("%w after %d attempts; last kafka error: %v", ctxErr, attempts, lastErr)
}

// DefaultRetryConfig returns 3 retries, 1s initial backoff, capped at 30s.
func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxRetries:  3,
		BackoffTime: time.Second,
		MaxBackoff:  30 * time.Second,
	}
}
