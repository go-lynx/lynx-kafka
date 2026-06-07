package kafka

import (
	"context"
	"sync"
	"time"

	"github.com/go-lynx/lynx/log"
	"github.com/twmb/franz-go/pkg/kgo"
)

// BatchProcessor accumulates records and flushes them either when maxBatchSize
// is reached or after maxWaitTime elapses, whichever comes first. Flushes run
// asynchronously except for the explicit Flush call.
type BatchProcessor struct {
	maxBatchSize int
	maxWaitTime  time.Duration
	handler      func(context.Context, []*kgo.Record) error
	records      []*kgo.Record
	mu           sync.Mutex
	wg           sync.WaitGroup
	timer        *time.Timer
	closed       bool
}

func NewBatchProcessor(maxBatchSize int, maxWaitTime time.Duration, handler func(context.Context, []*kgo.Record) error) *BatchProcessor {
	bp := &BatchProcessor{
		maxBatchSize: maxBatchSize,
		maxWaitTime:  maxWaitTime,
		handler:      handler,
		records:      make([]*kgo.Record, 0, maxBatchSize),
	}
	return bp
}

// AddRecord enqueues a record, triggering a flush if the batch is now full or
// arming the wait-time timer for the first record of a new batch. A nil record
// is ignored.
func (bp *BatchProcessor) AddRecord(ctx context.Context, record *kgo.Record) error {
	if record == nil {
		return nil
	}
	bp.mu.Lock()
	defer bp.mu.Unlock()

	if bp.closed {
		return ErrBatchProcessorClosed
	}

	bp.records = append(bp.records, record)

	if len(bp.records) >= bp.maxBatchSize {
		return bp.processBatchLocked(ctx, false)
	}

	// Set timer to process after maximum wait time.
	// Use context.Background() in timer callback instead of captured ctx,
	// since the original ctx may be canceled before the timer fires.
	if bp.timer == nil {
		bp.timer = time.AfterFunc(bp.maxWaitTime, func() {
			bp.mu.Lock()
			defer bp.mu.Unlock()
			if !bp.closed && len(bp.records) > 0 {
				_ = bp.processBatchLocked(context.Background(), false)
			}
		})
	}

	return nil
}

// processBatchLocked processes batch records. Caller must hold bp.mu.
// When sync is true, waits for handler to complete before returning (for Flush).
func (bp *BatchProcessor) processBatchLocked(ctx context.Context, sync bool) error {
	if len(bp.records) == 0 {
		return nil
	}

	// Stop timer
	if bp.timer != nil {
		bp.timer.Stop()
		bp.timer = nil
	}

	// Hand the records off to the flush and reset the buffer so callers can
	// keep appending while the flush runs.
	records := make([]*kgo.Record, len(bp.records))
	copy(records, bp.records)
	bp.records = bp.records[:0]

	if sync {
		// Flush: process synchronously so caller can wait for completion
		if err := bp.handler(ctx, records); err != nil {
			log.ErrorfCtx(ctx, "Batch processing failed: %v", err)
			return err
		}
		return nil
	}

	// Async: process in background
	bp.wg.Add(1)
	go func() {
		defer bp.wg.Done()
		defer func() {
			if r := recover(); r != nil {
				log.ErrorfCtx(ctx, "Batch processing panic recovered: %v", r)
			}
		}()
		if err := bp.handler(ctx, records); err != nil {
			log.ErrorfCtx(ctx, "Batch processing failed: %v", err)
		}
	}()

	return nil
}

// Flush forces processing of all pending records and waits for completion.
func (bp *BatchProcessor) Flush(ctx context.Context) error {
	bp.mu.Lock()
	err := bp.processBatchLocked(ctx, true)
	bp.mu.Unlock()
	if err != nil {
		return err
	}
	return bp.wait(ctx)
}

// Close marks the processor closed and drops any unflushed records. Callers
// wanting those records delivered must Flush first.
func (bp *BatchProcessor) Close() {
	bp.mu.Lock()
	if bp.closed {
		bp.mu.Unlock()
		return
	}
	bp.closed = true
	if bp.timer != nil {
		bp.timer.Stop()
		bp.timer = nil
	}
	bp.records = bp.records[:0]
	bp.mu.Unlock()
	// Wait for any async goroutines still executing the handler so Close()
	// does not return while background processing is in progress.
	bp.wg.Wait()
}

func (bp *BatchProcessor) wait(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	done := make(chan struct{})
	go func() {
		bp.wg.Wait()
		close(done)
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return nil
	}
}

// BatchConfig batch processing configuration
type BatchConfig struct {
	MaxBatchSize int
	MaxWaitTime  time.Duration
	Compression  string
	RetryCount   int
}

// DefaultBatchConfig returns batch defaults: 1000 records or 100ms wait, no
// compression, 3 retries.
func DefaultBatchConfig() *BatchConfig {
	return &BatchConfig{
		MaxBatchSize: 1000,
		MaxWaitTime:  100 * time.Millisecond,
		Compression:  "none",
		RetryCount:   3,
	}
}
