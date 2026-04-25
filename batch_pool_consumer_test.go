package kafka

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestBatchProcessorFlushWaitsForInFlightAndCloseRejects(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	bp := NewBatchProcessor(1, time.Hour, func(context.Context, []*kgo.Record) error {
		close(started)
		<-release
		return nil
	})

	if err := bp.AddRecord(context.Background(), &kgo.Record{Topic: "topic"}); err != nil {
		t.Fatalf("AddRecord: %v", err)
	}
	<-started

	flushDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		flushDone <- bp.Flush(ctx)
	}()

	select {
	case err := <-flushDone:
		t.Fatalf("Flush returned before in-flight batch finished: %v", err)
	case <-time.After(25 * time.Millisecond):
	}

	close(release)
	if err := <-flushDone; err != nil {
		t.Fatalf("Flush: %v", err)
	}

	bp.Close()
	bp.Close()
	err := bp.AddRecord(context.Background(), &kgo.Record{Topic: "topic"})
	if !errors.Is(err, ErrBatchProcessorClosed) {
		t.Fatalf("expected ErrBatchProcessorClosed, got %v", err)
	}
}

func TestGoroutinePoolCloseRejectsNewWorkWithoutPanics(t *testing.T) {
	pool := NewGoroutinePool(2)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pool.Submit(func() {
				time.Sleep(time.Millisecond)
			})
		}()
	}
	pool.Close()
	wg.Wait()
	pool.Submit(func() {
		t.Fatal("closed pool must reject new work")
	})
}

func TestConsumerGroupPartitionQueueCloseRace(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cg := &ConsumerGroup{
		ctx:       ctx,
		cancel:    cancel,
		handler:   MessageHandlerFunc(func(context.Context, string, int32, int64, []byte, []byte) error { return nil }),
		pool:      NewGoroutinePool(4),
		metrics:   NewMetrics(),
		partChans: make(map[string]chan []*kgo.Record),
	}
	defer cg.Stop()

	records := []*kgo.Record{{Topic: "topic"}}
	for i := 0; i < 100; i++ {
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			_ = cg.enqueuePartitionRecords("topic", 0, records)
		}()
		go func() {
			defer wg.Done()
			cg.closePartitionChan("topic", 0)
		}()
		wg.Wait()
	}
}
