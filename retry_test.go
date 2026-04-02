package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDoWithRetryPreservesLastErrorOnContextTimeout(t *testing.T) {
	handler := NewRetryHandler(RetryConfig{
		MaxRetries:  3,
		BackoffTime: 20 * time.Millisecond,
		MaxBackoff:  20 * time.Millisecond,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	want := errors.New("kafka broker unavailable")
	err := handler.DoWithRetry(ctx, func() error {
		return want
	})

	if assert.Error(t, err) {
		assert.ErrorIs(t, err, context.DeadlineExceeded)
		assert.Contains(t, err.Error(), "last kafka error")
		assert.Contains(t, err.Error(), want.Error())
	}
}
