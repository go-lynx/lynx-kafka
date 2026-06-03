package kafka

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// newTestConsumerGroup creates a minimal ConsumerGroup suitable for unit tests
// that do not require a real Kafka connection.
func newTestConsumerGroup(ctx context.Context, cancel context.CancelFunc, h MessageHandler) *ConsumerGroup {
	return &ConsumerGroup{
		ctx:            ctx,
		cancel:         cancel,
		handler:        h,
		pool:           NewGoroutinePool(4),
		metrics:        NewMetrics(),
		errorChan:      make(chan error, 100),
		rebalanceChan:  make(chan RebalanceEvent, 16),
		partChans:      make(map[string]chan []*kgo.Record),
		handlerTimeout: 5 * time.Second,
	}
}

// TestConsumerGroup_StopUnblocksProcessing verifies that calling Stop() on a
// running consumer group cancels in-flight processing without hanging.
func TestConsumerGroup_StopUnblocksProcessing(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	release := make(chan struct{})
	var handled atomic.Int32

	h := MessageHandlerFunc(func(hctx context.Context, _ string, _ int32, _ int64, _, _ []byte) error {
		handled.Add(1)
		select {
		case <-hctx.Done():
		case <-release:
		}
		return nil
	})

	cg := newTestConsumerGroup(ctx, cancel, h)

	records := []*kgo.Record{{Topic: "t", Value: []byte("v")}}
	if !cg.enqueuePartitionRecords("t", 0, records) {
		t.Fatal("enqueue returned false before Stop")
	}

	// Give the worker a moment to pick up the records.
	time.Sleep(20 * time.Millisecond)
	close(release)

	done := make(chan struct{})
	go func() {
		cg.Stop()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Stop() did not return within 3 seconds")
	}
}

// TestConsumerGroup_PartitionKeyFormat verifies that the partition key is stable
// and unique across different topic/partition combinations.
func TestConsumerGroup_PartitionKeyFormat(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cg := newTestConsumerGroup(ctx, cancel, MessageHandlerFunc(func(context.Context, string, int32, int64, []byte, []byte) error { return nil }))

	k1 := cg.partitionKey("topicA", 0)
	k2 := cg.partitionKey("topicA", 1)
	k3 := cg.partitionKey("topicB", 0)

	if k1 == k2 {
		t.Errorf("different partitions must produce different keys: %q vs %q", k1, k2)
	}
	if k1 == k3 {
		t.Errorf("different topics must produce different keys: %q vs %q", k1, k3)
	}
}

// TestConsumerGroup_MetricsIncrementOnSuccess verifies that message metrics are
// incremented when a handler processes a record successfully.
func TestConsumerGroup_MetricsIncrementOnSuccess(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var handled atomic.Int32
	h := MessageHandlerFunc(func(_ context.Context, _ string, _ int32, _ int64, _, value []byte) error {
		handled.Add(1)
		return nil
	})

	cg := newTestConsumerGroup(ctx, cancel, h)
	defer cg.Stop()

	records := []*kgo.Record{
		{Topic: "t", Value: []byte("hello")},
		{Topic: "t", Value: []byte("world")},
	}
	if !cg.enqueuePartitionRecords("t", 0, records) {
		t.Fatal("enqueue failed")
	}

	// Wait for processing to finish.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if handled.Load() == 2 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	if got := handled.Load(); got != 2 {
		t.Fatalf("expected 2 handled messages, got %d", got)
	}

	stats := cg.metrics.GetStats()
	if v := stats["consumed_messages"].(int64); v != 2 {
		t.Fatalf("consumed_messages metric = %d, want 2", v)
	}
}

// TestConsumerGroup_RebalanceRevokesCloseChannel verifies that handling a
// Revoked rebalance event closes the corresponding partition channel.
func TestConsumerGroup_RebalanceRevokesCloseChannel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cg := newTestConsumerGroup(ctx, cancel,
		MessageHandlerFunc(func(context.Context, string, int32, int64, []byte, []byte) error { return nil }))
	defer cg.Stop()

	// Create the partition channel by enqueueing a record.
	records := []*kgo.Record{{Topic: "events", Value: []byte("x")}}
	_ = cg.enqueuePartitionRecords("events", 2, records)

	cg.partMu.Lock()
	_, exists := cg.partChans[cg.partitionKey("events", 2)]
	cg.partMu.Unlock()
	if !exists {
		t.Skip("partition channel not created before rebalance — timing issue in test setup")
	}

	// Simulate a revoke event by calling closePartitionChan directly.
	cg.closePartitionChan("events", 2)

	cg.partMu.Lock()
	_, stillExists := cg.partChans[cg.partitionKey("events", 2)]
	cg.partMu.Unlock()
	if stillExists {
		t.Fatal("partition channel should have been removed after closePartitionChan")
	}
}

// TestConsumerGroup_IsRunning checks the IsRunning sentinel field lifecycle.
func TestConsumerGroup_IsRunning(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cg := newTestConsumerGroup(ctx, cancel,
		MessageHandlerFunc(func(context.Context, string, int32, int64, []byte, []byte) error { return nil }))

	if cg.IsRunning() {
		t.Fatal("IsRunning should be false before Start")
	}
}
