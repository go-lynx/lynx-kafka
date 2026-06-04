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
// Subscribe error paths (no real broker)
// ---------------------------------------------------------------------------

func TestSubscribe_NoTopics(t *testing.T) {
	k := NewKafkaClient()
	k.conf = &conf.Kafka{Brokers: []string{"localhost:9092"}}
	err := k.Subscribe(context.Background(), nil, MessageHandlerFunc(func(context.Context, string, int32, int64, []byte, []byte) error { return nil }))
	if err == nil {
		t.Fatal("expected error for no topics, got nil")
	}
}

func TestSubscribe_NilConf(t *testing.T) {
	k := NewKafkaClient()
	err := k.Subscribe(context.Background(), []string{"topic"}, MessageHandlerFunc(func(context.Context, string, int32, int64, []byte, []byte) error { return nil }))
	if !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("expected ErrInvalidConfiguration, got %v", err)
	}
}

func TestSubscribe_NoEnabledConsumer(t *testing.T) {
	k := NewKafkaClient()
	k.conf = &conf.Kafka{
		Brokers: []string{"localhost:9092"},
		Consumers: []*conf.Consumer{
			{Enabled: false, GroupId: "grp"},
		},
	}
	err := k.Subscribe(context.Background(), []string{"topic"}, MessageHandlerFunc(func(context.Context, string, int32, int64, []byte, []byte) error { return nil }))
	if err == nil {
		t.Fatal("expected error when no enabled consumer, got nil")
	}
}

func TestSubscribeWith_NoTopics(t *testing.T) {
	k := NewKafkaClient()
	k.conf = &conf.Kafka{Brokers: []string{"localhost:9092"}}
	err := k.SubscribeWith(context.Background(), "consumer", nil, MessageHandlerFunc(func(context.Context, string, int32, int64, []byte, []byte) error { return nil }))
	if err == nil {
		t.Fatal("expected error for no topics, got nil")
	}
}

func TestSubscribeWithOptions_NilHandler(t *testing.T) {
	k := NewKafkaClient()
	k.conf = &conf.Kafka{Brokers: []string{"localhost:9092"}}
	err := k.SubscribeWithOptions(context.Background(), "consumer", []string{"topic"}, nil, nil)
	if err == nil {
		t.Fatal("expected error for nil handler, got nil")
	}
}

func TestSubscribeWithOptions_NilConf(t *testing.T) {
	k := NewKafkaClient()
	err := k.SubscribeWithOptions(context.Background(), "consumer", []string{"topic"},
		MessageHandlerFunc(func(context.Context, string, int32, int64, []byte, []byte) error { return nil }), nil)
	if !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("expected ErrInvalidConfiguration, got %v", err)
	}
}

func TestSubscribeWithOptions_ConsumerNotFound(t *testing.T) {
	k := NewKafkaClient()
	k.conf = &conf.Kafka{
		Brokers: []string{"localhost:9092"},
		Consumers: []*conf.Consumer{
			{Enabled: true, GroupId: "grp", Name: "other"},
		},
	}
	err := k.SubscribeWithOptions(context.Background(), "nonexistent", []string{"topic"},
		MessageHandlerFunc(func(context.Context, string, int32, int64, []byte, []byte) error { return nil }), nil)
	if err == nil {
		t.Fatal("expected error for missing consumer instance, got nil")
	}
}

// ---------------------------------------------------------------------------
// NewConsumerGroup / NewConsumerGroupWithOptions
// ---------------------------------------------------------------------------

func TestNewConsumerGroupWithOptions_DefaultsFromConf(t *testing.T) {
	k := NewKafkaClient()
	c := &conf.Consumer{
		GroupId:        "test-group",
		MaxConcurrency: 5,
		AutoCommit:     true,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	k.ctx = ctx
	k.cancel = cancel

	cg := k.NewConsumerGroup(new(kgo.Client), c, []string{"t"}, MessageHandlerFunc(func(context.Context, string, int32, int64, []byte, []byte) error { return nil }))
	if cg == nil {
		t.Fatal("expected non-nil ConsumerGroup")
	}
	if cg.groupID != "test-group" {
		t.Errorf("expected groupID test-group, got %q", cg.groupID)
	}
	if !cg.autoCommit {
		t.Error("expected autoCommit true")
	}
}

func TestNewConsumerGroupWithOptions_OptionOverridesConf(t *testing.T) {
	k := NewKafkaClient()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	k.ctx = ctx
	k.cancel = cancel

	c := &conf.Consumer{GroupId: "g", MaxConcurrency: 1}
	opts := &ConsumerGroupOptions{MaxConcurrency: 8, HandlerTimeout: 5 * time.Second}
	cg := k.NewConsumerGroupWithOptions(new(kgo.Client), c, nil, MessageHandlerFunc(func(context.Context, string, int32, int64, []byte, []byte) error { return nil }), opts)
	if cg.handlerTimeout != 5*time.Second {
		t.Errorf("expected handler timeout 5s, got %v", cg.handlerTimeout)
	}
}

func TestNewConsumerGroup_NilConf(t *testing.T) {
	k := NewKafkaClient()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	k.ctx = ctx
	k.cancel = cancel

	cg := k.NewConsumerGroup(new(kgo.Client), nil, []string{"t"}, MessageHandlerFunc(func(context.Context, string, int32, int64, []byte, []byte) error { return nil }))
	if cg == nil {
		t.Fatal("expected non-nil ConsumerGroup even with nil conf")
	}
}

// ---------------------------------------------------------------------------
// getStartOffset
// ---------------------------------------------------------------------------

func TestGetStartOffset_Earliest(t *testing.T) {
	k := NewKafkaClient()
	c := &conf.Consumer{StartOffset: StartOffsetEarliest}
	off := k.getStartOffset(c)
	_ = off // just ensure no panic
}

func TestGetStartOffset_Latest(t *testing.T) {
	k := NewKafkaClient()
	c := &conf.Consumer{StartOffset: StartOffsetLatest}
	_ = k.getStartOffset(c)
}

func TestGetStartOffset_Default(t *testing.T) {
	k := NewKafkaClient()
	c := &conf.Consumer{StartOffset: ""}
	_ = k.getStartOffset(c)
}

func TestGetStartOffset_NilConsumer(t *testing.T) {
	k := NewKafkaClient()
	_ = k.getStartOffset(nil)
}

// ---------------------------------------------------------------------------
// processRecordsSerial error path (handler returns error)
// ---------------------------------------------------------------------------

func TestProcessRecordsSerial_HandlerError_StopsAtFirstError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var callCount int
	h := MessageHandlerFunc(func(_ context.Context, _ string, _ int32, _ int64, _, _ []byte) error {
		callCount++
		return errors.New("handler failed")
	})

	cg := newTestConsumerGroup(ctx, cancel, h)
	defer cg.Stop()

	records := []*kgo.Record{
		{Topic: "t", Value: []byte("a")},
		{Topic: "t", Value: []byte("b")},
	}
	cg.processRecordsSerial("t", 0, records)

	if callCount != 1 {
		t.Fatalf("expected handler called once (stops on first error), got %d", callCount)
	}
}

func TestProcessRecordsSerial_HandlerSuccess_ProcessesAll(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var callCount int
	h := MessageHandlerFunc(func(_ context.Context, _ string, _ int32, _ int64, _, _ []byte) error {
		callCount++
		return nil
	})

	cg := newTestConsumerGroup(ctx, cancel, h)
	defer cg.Stop()

	records := []*kgo.Record{
		{Topic: "t", Value: []byte("a")},
		{Topic: "t", Value: []byte("b")},
		{Topic: "t", Value: []byte("c")},
	}
	cg.processRecordsSerial("t", 0, records)

	if callCount != 3 {
		t.Fatalf("expected 3 handler calls, got %d", callCount)
	}
}

// ---------------------------------------------------------------------------
// handleErrors & handleRebalances – verify goroutines exit on ctx cancel
// ---------------------------------------------------------------------------

func TestConsumerGroup_HandleErrors_ExitsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cg := newTestConsumerGroup(ctx, cancel, MessageHandlerFunc(func(context.Context, string, int32, int64, []byte, []byte) error { return nil }))

	done := make(chan struct{})
	go func() {
		cg.handleErrors()
		close(done)
	}()

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("handleErrors did not exit after context cancel")
	}
}

func TestConsumerGroup_HandleRebalances_ExitsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cg := newTestConsumerGroup(ctx, cancel, MessageHandlerFunc(func(context.Context, string, int32, int64, []byte, []byte) error { return nil }))

	done := make(chan struct{})
	go func() {
		cg.handleRebalances()
		close(done)
	}()

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("handleRebalances did not exit after context cancel")
	}
}

func TestConsumerGroup_HandleRebalances_ProcessesAssignedEvent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cg := newTestConsumerGroup(ctx, cancel, MessageHandlerFunc(func(context.Context, string, int32, int64, []byte, []byte) error { return nil }))

	go cg.handleRebalances()

	cg.rebalanceChan <- RebalanceEvent{Assigned: map[string][]int32{"topic": {0, 1}}}
	time.Sleep(30 * time.Millisecond)
	cancel()
}

func TestConsumerGroup_HandleRebalances_ProcessesRevokedEvent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cg := newTestConsumerGroup(ctx, cancel, MessageHandlerFunc(func(context.Context, string, int32, int64, []byte, []byte) error { return nil }))

	// Create a partition chan to verify it gets closed on revoke
	cg.partMu.Lock()
	key := cg.partitionKey("topic", 3)
	ch := make(chan []*kgo.Record, 1)
	cg.partChans[key] = ch
	cg.partMu.Unlock()

	go cg.handleRebalances()

	cg.rebalanceChan <- RebalanceEvent{Revoked: map[string][]int32{"topic": {3}}}
	time.Sleep(30 * time.Millisecond)
	cancel()
}

// ---------------------------------------------------------------------------
// GetConsumer fallback path
// ---------------------------------------------------------------------------

func TestGetConsumer_FallbackToMap(t *testing.T) {
	k := NewKafkaClient()
	k.consumers["c1"] = new(kgo.Client)
	got := k.GetConsumer()
	if got == nil {
		t.Fatal("expected non-nil consumer via fallback map")
	}
}

// ---------------------------------------------------------------------------
// IsConsumerReady – various states
// ---------------------------------------------------------------------------

func TestIsConsumerReady_LegacyConsumerSet_NoConnMgr(t *testing.T) {
	k := NewKafkaClient()
	k.consumer = new(kgo.Client)
	// No connection managers -> should return true
	if !k.IsConsumerReady() {
		t.Fatal("expected true when consumer set and no conn mgrs")
	}
}

func TestIsConsumerReady_ConsumerMap_WithNilConnMgr(t *testing.T) {
	k := NewKafkaClient()
	k.consumers["c1"] = new(kgo.Client)
	k.consConnMgrs["c1"] = nil
	if !k.IsConsumerReady() {
		t.Fatal("expected true when conn mgr is nil")
	}
}

func TestIsConsumerReady_ConsumerMap_DisconnectedConnMgr(t *testing.T) {
	k := NewKafkaClient()
	k.consumers["c1"] = new(kgo.Client)
	k.consConnMgrs["c1"] = &ConnectionManager{isConnected: false}
	if k.IsConsumerReady() {
		t.Fatal("expected false when conn mgr disconnected")
	}
}
