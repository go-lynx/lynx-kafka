package kafka

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-lynx/lynx-kafka/conf"
	"github.com/twmb/franz-go/pkg/kgo"
)

// A batch must still be flushed after the enqueuing caller's ctx is cancelled,
// both on the size-triggered path and the timer-triggered path.
func TestBatchProcessor_FlushSurvivesCallerContextCancel(t *testing.T) {
	var flushed int32
	handlerCtxErr := make(chan error, 2)
	bp := NewBatchProcessor(2, 20*time.Millisecond, func(ctx context.Context, recs []*kgo.Record) error {
		// Give the caller time to cancel before we observe the ctx.
		time.Sleep(10 * time.Millisecond)
		handlerCtxErr <- ctx.Err()
		atomic.AddInt32(&flushed, int32(len(recs)))
		return nil
	})
	defer bp.Close()

	// Size-triggered flush: second record fills the batch.
	ctx, cancel := context.WithCancel(context.Background())
	if err := bp.AddRecord(ctx, &kgo.Record{Topic: "t"}); err != nil {
		t.Fatalf("AddRecord: %v", err)
	}
	if err := bp.AddRecord(ctx, &kgo.Record{Topic: "t"}); err != nil {
		t.Fatalf("AddRecord: %v", err)
	}
	cancel() // caller goes away immediately after Produce returned

	select {
	case err := <-handlerCtxErr:
		if err != nil {
			t.Fatalf("flush ctx was cancelled by caller ctx: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("size-triggered batch was not flushed")
	}

	// Timer-triggered flush: single record, caller cancels before timer fires.
	ctx2, cancel2 := context.WithCancel(context.Background())
	if err := bp.AddRecord(ctx2, &kgo.Record{Topic: "t"}); err != nil {
		t.Fatalf("AddRecord: %v", err)
	}
	cancel2()
	select {
	case err := <-handlerCtxErr:
		if err != nil {
			t.Fatalf("timer flush ctx was cancelled by caller ctx: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timer-triggered batch was not flushed")
	}

	if got := atomic.LoadInt32(&flushed); got != 3 {
		t.Fatalf("expected 3 records flushed, got %d", got)
	}
}

func TestBatchProcessor_AddRecordRejectsCancelledCallerCtx(t *testing.T) {
	bp := NewBatchProcessor(10, time.Hour, func(context.Context, []*kgo.Record) error { return nil })
	defer bp.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := bp.AddRecord(ctx, &kgo.Record{Topic: "t"}); err != context.Canceled {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	bp.mu.Lock()
	n := len(bp.records)
	bp.mu.Unlock()
	if n != 0 {
		t.Fatalf("record enqueued despite cancelled ctx")
	}
}

func TestBatchProcessor_CloseCancelsFlushContext(t *testing.T) {
	bp := NewBatchProcessor(1, time.Hour, func(context.Context, []*kgo.Record) error { return nil })
	bp.Close()
	if bp.ctx.Err() == nil {
		t.Fatal("expected processor ctx cancelled after Close")
	}
}

// Batch-size semantics: 0/1 (and the default) mean no batching; >1 enables it.
func TestBatchSizeSemantics_DefaultAndExplicit(t *testing.T) {
	k := NewKafkaClient()
	k.conf = &conf.Kafka{
		Brokers: []string{"localhost:9092"},
		Producers: []*conf.Producer{
			{Enabled: true, Name: "unset"},
			{Enabled: true, Name: "zero", BatchSize: 0},
			{Enabled: true, Name: "one", BatchSize: 1},
			{Enabled: true, Name: "many", BatchSize: 50},
		},
	}
	k.setDefaultValues()
	want := map[string]int32{"unset": 0, "zero": 0, "one": 1, "many": 50}
	for _, p := range k.conf.Producers {
		if p.BatchSize != want[p.Name] {
			t.Errorf("producer %s: BatchSize=%d, want %d", p.Name, p.BatchSize, want[p.Name])
		}
		if p.BatchTimeout == nil {
			t.Errorf("producer %s: expected BatchTimeout default", p.Name)
		}
		enabled := batchingEnabled(p)
		if enabled != (want[p.Name] > 1) {
			t.Errorf("producer %s: batching enabled=%v, want %v", p.Name, enabled, want[p.Name] > 1)
		}
	}
}
