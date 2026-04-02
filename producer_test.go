package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/twmb/franz-go/pkg/kgo"
)

func newTestProducerClient() *Client {
	return &Client{
		producers:           map[string]*kgo.Client{"default": new(kgo.Client)},
		prodConnMgrs:        make(map[string]*ConnectionManager),
		defaultProducer:     "default",
		metrics:             NewMetrics(),
		retryHandlers:       map[string]*RetryHandler{"default": NewRetryHandler(RetryConfig{MaxRetries: 0, BackoffTime: time.Millisecond, MaxBackoff: time.Millisecond})},
		defaultRetryHandler: NewRetryHandler(RetryConfig{MaxRetries: 0, BackoffTime: time.Millisecond, MaxBackoff: time.Millisecond}),
	}
}

func TestIsProducerReadyRequiresConnectedManager(t *testing.T) {
	client := newTestProducerClient()
	client.prodConnMgrs["default"] = &ConnectionManager{}

	assert.False(t, client.IsProducerReady())

	client.prodConnMgrs["default"].isConnected = true
	assert.True(t, client.IsProducerReady())
}

func TestProduceFailsFastWhenProducerDisconnected(t *testing.T) {
	client := newTestProducerClient()
	client.prodConnMgrs["default"] = &ConnectionManager{
		lastErr: errors.New("tls handshake failed"),
	}

	err := client.Produce(context.Background(), "betday.user.user_login_recorded", []byte("1"), []byte(`{}`))
	if assert.Error(t, err) {
		assert.ErrorIs(t, err, ErrBrokerUnavailable)
		assert.Contains(t, err.Error(), "tls handshake failed")
	}
}

func TestGetHealthStatusReportsDisconnectedProducer(t *testing.T) {
	client := newTestProducerClient()
	client.prodConnMgrs["default"] = &ConnectionManager{
		lastErr: errors.New("dial tcp timeout"),
	}

	status := client.GetHealthStatus()
	assert.False(t, status.Healthy)
	if assert.Error(t, status.LastError) {
		assert.Contains(t, status.LastError.Error(), "producer[default]")
		assert.Contains(t, status.LastError.Error(), "dial tcp timeout")
	}
}
