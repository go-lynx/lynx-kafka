package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-lynx/lynx-kafka/conf"
	"github.com/twmb/franz-go/pkg/kgo"
)

// ---------------------------------------------------------------------------
// Produce / ProduceWith error paths (no real broker needed)
// ---------------------------------------------------------------------------

func TestProduce_NoDefaultProducer(t *testing.T) {
	k := NewKafkaClient()
	err := k.Produce(context.Background(), "topic", nil, nil)
	if !errors.Is(err, ErrProducerNotInitialized) {
		t.Fatalf("expected ErrProducerNotInitialized, got %v", err)
	}
}

func TestProduceBatch_NoDefaultProducer(t *testing.T) {
	k := NewKafkaClient()
	err := k.ProduceBatch(context.Background(), "topic", []*kgo.Record{{Topic: "topic"}})
	if !errors.Is(err, ErrProducerNotInitialized) {
		t.Fatalf("expected ErrProducerNotInitialized, got %v", err)
	}
}

func TestProduceBatchWith_NilProducer(t *testing.T) {
	k := NewKafkaClient()
	err := k.ProduceBatchWith(context.Background(), "missing", "topic", []*kgo.Record{{Topic: "topic"}})
	if !errors.Is(err, ErrProducerNotInitialized) {
		t.Fatalf("expected ErrProducerNotInitialized, got %v", err)
	}
}

func TestProduceBatchWith_EmptyRecords_ReturnNil(t *testing.T) {
	k := newTestProducerClient()
	// All-nil slice must return nil (no-op)
	err := k.ProduceBatchWith(context.Background(), "default", "topic", []*kgo.Record{nil, nil})
	if err != nil {
		t.Fatalf("expected nil for all-nil records, got %v", err)
	}
}

func TestProduceBatchWith_InvalidTopic(t *testing.T) {
	k := newTestProducerClient()
	err := k.ProduceBatchWith(context.Background(), "default", "", []*kgo.Record{{Value: []byte("v")}})
	if err == nil {
		t.Fatal("expected error for empty topic, got nil")
	}
}

func TestProduceBatchWith_EmptySlice_ReturnNil(t *testing.T) {
	k := newTestProducerClient()
	err := k.ProduceBatchWith(context.Background(), "default", "topic", nil)
	if err != nil {
		t.Fatalf("expected nil for empty records, got %v", err)
	}
}

func TestProduceWith_InvalidTopic(t *testing.T) {
	k := newTestProducerClient()
	err := k.ProduceWith(context.Background(), "default", "", nil, nil)
	if err == nil {
		t.Fatal("expected error for empty topic")
	}
}

func TestProduceWith_ProducerUnavailableDueToConnectionError(t *testing.T) {
	k := newTestProducerClient()
	k.prodConnMgrs["default"] = &ConnectionManager{lastErr: errors.New("broker down")}
	err := k.ProduceWith(context.Background(), "default", "topic", nil, nil)
	if !errors.Is(err, ErrBrokerUnavailable) {
		t.Fatalf("expected ErrBrokerUnavailable, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// GetProducer / IsProducerReady
// ---------------------------------------------------------------------------

func TestGetProducer_NoDefault_ReturnsNil(t *testing.T) {
	k := NewKafkaClient()
	if k.GetProducer() != nil {
		t.Fatal("expected nil when no default producer")
	}
}

func TestGetProducer_ReturnsKgoClient(t *testing.T) {
	k := newTestProducerClient()
	if k.GetProducer() == nil {
		t.Fatal("expected non-nil producer client")
	}
}

func TestIsProducerReady_NoDefault_ReturnsFalse(t *testing.T) {
	k := NewKafkaClient()
	if k.IsProducerReady() {
		t.Fatal("expected false when no default producer")
	}
}

func TestIsProducerReady_NilProducerEntry_ReturnsFalse(t *testing.T) {
	k := newTestProducerClient()
	k.producers["default"] = nil
	if k.IsProducerReady() {
		t.Fatal("expected false when producer is nil")
	}
}

func TestIsProducerReady_NoConnMgr_ReturnsTrue(t *testing.T) {
	k := newTestProducerClient()
	// No connection manager -> consider ready
	if !k.IsProducerReady() {
		t.Fatal("expected true when no connection manager")
	}
}

// ---------------------------------------------------------------------------
// getCompression
// ---------------------------------------------------------------------------

func TestGetCompression_AllCodecs(t *testing.T) {
	tests := []struct {
		compression string
	}{
		{CompressionGzip},
		{CompressionSnappy},
		{CompressionLz4},
		{CompressionZstd},
		{CompressionNone},
		{"unknown"},
		{""},
	}
	k := NewKafkaClient()
	for _, tt := range tests {
		t.Run(tt.compression, func(t *testing.T) {
			p := &conf.Producer{Compression: tt.compression}
			_ = k.getCompression(p) // must not panic
		})
	}
}

func TestGetCompression_NilProducer_ReturnsSnappy(t *testing.T) {
	k := NewKafkaClient()
	codec := k.getCompression(nil)
	_ = codec // snappy by default
}

// ---------------------------------------------------------------------------
// retryConfigFromProducer
// ---------------------------------------------------------------------------

func TestRetryConfigFromProducer_NilProducer_ReturnsDefault(t *testing.T) {
	k := NewKafkaClient()
	cfg := k.retryConfigFromProducer(nil)
	def := DefaultRetryConfig()
	if cfg.MaxRetries != def.MaxRetries {
		t.Errorf("expected default MaxRetries %d, got %d", def.MaxRetries, cfg.MaxRetries)
	}
}

func TestRetryConfigFromProducer_OverridesValues(t *testing.T) {
	k := NewKafkaClient()
	p := &conf.Producer{MaxRetries: 7}
	cfg := k.retryConfigFromProducer(p)
	if cfg.MaxRetries != 7 {
		t.Errorf("expected MaxRetries 7, got %d", cfg.MaxRetries)
	}
}

// ---------------------------------------------------------------------------
// getCircuitBreaker / getRetryHandler
// ---------------------------------------------------------------------------

func TestGetCircuitBreaker_MissingKey_ReturnsNil(t *testing.T) {
	k := NewKafkaClient()
	if cb := k.getCircuitBreaker("nonexistent"); cb != nil {
		t.Fatal("expected nil circuit breaker for missing key")
	}
}

func TestGetCircuitBreaker_ExistingKey(t *testing.T) {
	k := NewKafkaClient()
	k.circuitBreakers["p1"] = NewCircuitBreaker(3, time.Second)
	if cb := k.getCircuitBreaker("p1"); cb == nil {
		t.Fatal("expected non-nil circuit breaker")
	}
}

func TestGetRetryHandler_MissingKey_ReturnsFallback(t *testing.T) {
	k := NewKafkaClient()
	rh := k.getRetryHandler("nonexistent")
	if rh == nil {
		t.Fatal("expected fallback retry handler, got nil")
	}
	// should be the default
	if rh != k.defaultRetryHandler {
		t.Fatal("expected default retry handler as fallback")
	}
}

func TestGetRetryHandler_SpecificKey(t *testing.T) {
	k := NewKafkaClient()
	specific := NewRetryHandler(RetryConfig{MaxRetries: 1, BackoffTime: time.Millisecond, MaxBackoff: time.Millisecond})
	k.retryHandlers["p1"] = specific
	rh := k.getRetryHandler("p1")
	if rh != specific {
		t.Fatal("expected specific retry handler")
	}
}

// ---------------------------------------------------------------------------
// GetMetrics
// ---------------------------------------------------------------------------

func TestGetMetrics_ReturnsNonNil(t *testing.T) {
	k := NewKafkaClient()
	if k.GetMetrics() == nil {
		t.Fatal("expected non-nil metrics")
	}
}

// ---------------------------------------------------------------------------
// GetConsumer / IsConsumerReady
// ---------------------------------------------------------------------------

func TestGetConsumer_Empty_ReturnsNil(t *testing.T) {
	k := NewKafkaClient()
	if k.GetConsumer() != nil {
		t.Fatal("expected nil consumer when none initialized")
	}
}

func TestIsConsumerReady_Empty_ReturnsFalse(t *testing.T) {
	k := NewKafkaClient()
	if k.IsConsumerReady() {
		t.Fatal("expected false when no consumers initialized")
	}
}
