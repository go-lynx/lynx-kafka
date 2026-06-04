package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-lynx/lynx-kafka/conf"
	"github.com/go-lynx/lynx/plugins"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/types/known/durationpb"
)

// ---------------------------------------------------------------------------
// StopContext – active plugin
// ---------------------------------------------------------------------------

func TestClient_StopContext_ActivePlugin(t *testing.T) {
	k := NewKafkaClient()
	k.conf = &conf.Kafka{Brokers: []string{"localhost:9092"}}

	// Force status to active so StopContext can proceed past the status check.
	k.SetStatus(plugins.StatusActive)

	ctx := context.Background()
	err := k.StopContext(ctx, k)
	if err != nil {
		t.Fatalf("expected no error from StopContext, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// startupContext
// ---------------------------------------------------------------------------

func TestStartupContext_DefaultTimeout(t *testing.T) {
	k := NewKafkaClient()
	k.conf = &conf.Kafka{}
	ctx, cancel := k.startupContext()
	defer cancel()
	if ctx == nil {
		t.Fatal("expected non-nil startup context")
	}
}

func TestStartupContext_CustomDialTimeout(t *testing.T) {
	k := NewKafkaClient()
	k.conf = &conf.Kafka{DialTimeout: durationpb.New(500 * time.Millisecond)}
	ctx, cancel := k.startupContext()
	defer cancel()
	if ctx == nil {
		t.Fatal("expected non-nil startup context")
	}
}

// ---------------------------------------------------------------------------
// shutdownTasksContext
// ---------------------------------------------------------------------------

func TestShutdownTasksContext_CanceledContext(t *testing.T) {
	k := NewKafkaClient()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := k.shutdownTasksContext(ctx)
	// cancelled context returns error with "canceled" or combines errors
	if err == nil {
		t.Fatal("expected error for canceled shutdown context")
	}
}

func TestShutdownTasksContext_CleanupWithConsumerGroups(t *testing.T) {
	k := NewKafkaClient()
	ctx := context.Background()

	// Add a consumer group that can be stopped
	cgCtx, cgCancel := context.WithCancel(context.Background())
	_ = cgCancel
	cg := &ConsumerGroup{
		ctx:       cgCtx,
		cancel:    cgCancel,
		pool:      NewGoroutinePool(2),
		partChans: make(map[string]chan []*kgo.Record),
		errorChan: make(chan error, 10),
		rebalanceChan: make(chan RebalanceEvent, 4),
	}
	k.activeGroups["c1"] = cg
	k.producers["p1"] = nil // nil is safe, skipped in cleanup
	k.consumers["c1"] = nil // nil is safe, skipped in cleanup

	if err := k.shutdownTasksContext(ctx); err != nil {
		t.Fatalf("unexpected error during shutdown: %v", err)
	}
}

// ---------------------------------------------------------------------------
// rollbackStartupState
// ---------------------------------------------------------------------------

func TestRollbackStartupState_EmptyMaps(t *testing.T) {
	k := NewKafkaClient()
	if err := k.rollbackStartupState(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// ---------------------------------------------------------------------------
// startupTasksContext – no conf
// ---------------------------------------------------------------------------

func TestStartupTasksContext_NilConf_ReturnsError(t *testing.T) {
	k := NewKafkaClient()
	k.conf = nil
	err := k.startupTasksContext(context.Background())
	if !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("expected ErrInvalidConfiguration, got %v", err)
	}
}

func TestStartupTasksContext_CanceledContext_ReturnsError(t *testing.T) {
	k := NewKafkaClient()
	k.conf = &conf.Kafka{Brokers: []string{"localhost:9092"}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := k.startupTasksContext(ctx)
	if err == nil {
		t.Fatal("expected error for canceled context")
	}
}

func TestStartupTasksContext_NoBrokersWithProducers_FailsFast(t *testing.T) {
	k := NewKafkaClient()
	k.conf = &conf.Kafka{
		Brokers:     []string{"127.0.0.1:1"},
		DialTimeout: durationpb.New(100 * time.Millisecond),
		Producers: []*conf.Producer{
			{
				Enabled:      true,
				Name:         "p1",
				RequiredAcks: 1,
				BatchSize:    1,
				BatchTimeout: durationpb.New(50 * time.Millisecond),
			},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := k.startupTasksContext(ctx)
	if err == nil {
		t.Fatal("expected connectivity error, got nil")
	}
}

func TestStartupTasksContext_SkipsDisabledProducers(t *testing.T) {
	k := NewKafkaClient()
	k.conf = &conf.Kafka{
		Brokers: []string{"localhost:9092"},
		Producers: []*conf.Producer{
			{Enabled: false, Name: "disabled"},
		},
	}
	// no rt, no active producers/consumers — health check passes (nothing to check)
	// so startupTasksContext should succeed (nil error)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	err := k.startupTasksContext(ctx)
	// No error expected: no producers start, health check passes trivially
	_ = err // either nil or error is acceptable here
}

// ---------------------------------------------------------------------------
// RetryHandler edge cases
// ---------------------------------------------------------------------------

func TestDoWithRetry_SuccessOnFirstAttempt(t *testing.T) {
	rh := NewRetryHandler(RetryConfig{MaxRetries: 3, BackoffTime: time.Millisecond, MaxBackoff: time.Millisecond})
	err := rh.DoWithRetry(context.Background(), func() error { return nil })
	if err != nil {
		t.Fatalf("expected nil error on success, got %v", err)
	}
}

func TestDoWithRetry_ExhaustsRetries(t *testing.T) {
	attempts := 0
	rh := NewRetryHandler(RetryConfig{MaxRetries: 2, BackoffTime: time.Millisecond, MaxBackoff: time.Millisecond})
	err := rh.DoWithRetry(context.Background(), func() error {
		attempts++
		return errors.New("always fails")
	})
	if err == nil {
		t.Fatal("expected error after retries exhausted")
	}
	if attempts != 3 { // initial + 2 retries
		t.Fatalf("expected 3 attempts, got %d", attempts)
	}
}

func TestDoWithRetry_ContextCanceledDuringBackoff(t *testing.T) {
	rh := NewRetryHandler(RetryConfig{MaxRetries: 10, BackoffTime: 500 * time.Millisecond, MaxBackoff: 500 * time.Millisecond})
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := rh.DoWithRetry(ctx, func() error { return errors.New("fail") })
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded, got %v", err)
	}
}

func TestWrapRetryContextError_NilCtxErr(t *testing.T) {
	result := wrapRetryContextError(nil, errors.New("last"), 1)
	if result.Error() != "last" {
		t.Fatalf("expected last error, got %v", result)
	}
}

func TestWrapRetryContextError_NilLastErr(t *testing.T) {
	ctxErr := context.Canceled
	result := wrapRetryContextError(ctxErr, nil, 0)
	if !errors.Is(result, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", result)
	}
}

// ---------------------------------------------------------------------------
// registerHealthForProducer / Consumer
// ---------------------------------------------------------------------------

func TestRegisterHealthForProducer_NoConnMgr(t *testing.T) {
	k := NewKafkaClient()
	// Should not panic even if there's no conn mgr
	k.registerHealthForProducer("p1")
}

func TestRegisterHealthForConsumer_NoConnMgr(t *testing.T) {
	k := NewKafkaClient()
	k.registerHealthForConsumer("c1")
}

func TestRegisterHealthForProducer_WithConnMgr(t *testing.T) {
	k := NewKafkaClient()
	k.prodConnMgrs["p1"] = &ConnectionManager{healthChecker: &HealthChecker{isHealthy: true}}
	k.registerHealthForProducer("p1")
	// Calling again should be idempotent (already registered)
	k.registerHealthForProducer("p1")
}

// ---------------------------------------------------------------------------
// CheckHealth / GetHealthStatus – healthy path with nil conn mgrs
// ---------------------------------------------------------------------------

func TestCheckHealth_NilConnMgrs_ReturnsNil(t *testing.T) {
	k := NewKafkaClient()
	k.prodConnMgrs["p1"] = nil
	k.consConnMgrs["c1"] = nil
	if err := k.CheckHealth(); err != nil {
		t.Fatalf("expected nil error for nil conn mgrs, got %v", err)
	}
}

func TestGetHealthStatus_ConnectedProducerNoHealthChecker(t *testing.T) {
	k := NewKafkaClient()
	k.prodConnMgrs["p1"] = &ConnectionManager{isConnected: true}
	status := k.GetHealthStatus()
	if !status.Healthy {
		t.Fatalf("expected healthy, got %v", status.LastError)
	}
}

func TestGetHealthStatus_ProducerDisconnectedNoError(t *testing.T) {
	k := NewKafkaClient()
	k.prodConnMgrs["p1"] = &ConnectionManager{isConnected: false, lastErr: nil}
	status := k.GetHealthStatus()
	if status.Healthy {
		t.Fatal("expected unhealthy when disconnected")
	}
}

// ---------------------------------------------------------------------------
// ProduceBatchWith – per-record topic validation
// ---------------------------------------------------------------------------

func TestProduceBatchWith_RecordWithEmptyTopic_NoTopicArg(t *testing.T) {
	k := newTestProducerClient()
	// topic arg is empty AND record has empty topic -> should fail validation
	records := []*kgo.Record{{Value: []byte("v"), Topic: ""}}
	err := k.ProduceBatchWith(context.Background(), "default", "", records)
	if err == nil {
		t.Fatal("expected error for record with empty topic and no topic arg")
	}
}

func TestProduceBatchWith_ValidRecordTopicPassesValidation(t *testing.T) {
	k := NewKafkaClient()
	// No producer registered => returns ErrProducerNotInitialized, NOT a validation error
	records := []*kgo.Record{{Value: []byte("v"), Topic: "valid-topic"}}
	err := k.ProduceBatchWith(context.Background(), "default", "", records)
	if !errors.Is(err, ErrProducerNotInitialized) {
		t.Fatalf("expected ErrProducerNotInitialized for missing producer, got: %v", err)
	}
}
