package kafka

import (
	"context"
	"sync"
	"time"

	"github.com/go-lynx/lynx/log"
	"github.com/twmb/franz-go/pkg/kgo"
)

// DefaultBatchFlushTimeout bounds a single asynchronous batch flush. It is
// derived from the processor's own lifecycle context, never from the context
// of the caller that enqueued a record.
const DefaultBatchFlushTimeout = 30 * time.Second

// BatchProcessor accumulates records and flushes them either when maxBatchSize
// is reached or after maxWaitTime elapses, whichever comes first. Flushes run
// asynchronously except for the explicit Flush call.
//
// Asynchronous flushes run under a context owned by the processor (cancelled
// only by Close) bounded by flushTimeout. A caller's context passed to
// AddRecord governs only that caller's enqueue; cancelling it afterwards does
// not affect delivery of the batch its record joined.
type BatchProcessor struct {
	maxBatchSize int
	maxWaitTime  time.Duration
	flushTimeout time.Duration
	handler      func(context.Context, []*kgo.Record) error
	records      []*kgo.Record
	mu           sync.Mutex
	wg           sync.WaitGroup
	timer        *time.Timer
	closed       bool
	ctx          context.Context
	cancel       context.CancelFunc
}

func NewBatchProcessor(maxBatchSize int, maxWaitTime time.Duration, handler func(context.Context, []*kgo.Record) error) *BatchProcessor {
	if maxBatchSize < 1 {
		maxBatchSize = 1
	}
	ctx, cancel := context.WithCancel(context.Background())
	bp := &BatchProcessor{
		maxBatchSize: maxBatchSize,
		maxWaitTime:  maxWaitTime,
		flushTimeout: DefaultBatchFlushTimeout,
		handler:      handler,
		records:      make([]*kgo.Record, 0, maxBatchSize),
		ctx:          ctx,
		cancel:       cancel,
	}
	return bp
}

// SetFlushTimeout overrides the per-flush timeout for asynchronous flushes.
// Non-positive values leave the default in place.
func (bp *BatchProcessor) SetFlushTimeout(d time.Duration) {
	if d <= 0 {
		return
	}
	bp.mu.Lock()
	bp.flushTimeout = d
	bp.mu.Unlock()
}

// AddRecord enqueues a record, triggering a flush if the batch is now full or
// arming the wait-time timer for the first record of a new batch. A nil record
// is ignored.
func (bp *BatchProcessor) AddRecord(ctx context.Context, record *kgo.Record) error {
	if record == nil {
		return nil
	}
	// The caller's ctx only governs the enqueue itself; it is never used for
	// the flush, which runs under the processor-owned context.
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	bp.mu.Lock()
	defer bp.mu.Unlock()

	if bp.closed {
		return ErrBatchProcessorClosed
	}

	bp.records = append(bp.records, record)

	if len(bp.records) >= bp.maxBatchSize {
		return bp.processBatchLocked(nil, false)
	}

	// Set timer to process after maximum wait time.
	if bp.timer == nil {
		bp.timer = time.AfterFunc(bp.maxWaitTime, func() {
			bp.mu.Lock()
			defer bp.mu.Unlock()
			if !bp.closed && len(bp.records) > 0 {
				_ = bp.processBatchLocked(nil, false)
			}
		})
	}

	return nil
}

// processBatchLocked processes batch records. Caller must hold bp.mu.
// When sync is true, waits for handler to complete before returning (for Flush)
// using ctx. When sync is false, ctx is ignored and the flush runs under the
// processor-owned context bounded by flushTimeout.
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

	// Async: process in background under the processor-owned context so a
	// cancelled caller context cannot abort the whole batch.
	flushCtx, cancel := context.WithTimeout(bp.ctx, bp.flushTimeout)
	bp.wg.Add(1)
	go func() {
		defer bp.wg.Done()
		defer cancel()
		defer func() {
			if r := recover(); r != nil {
				log.ErrorfCtx(flushCtx, "Batch processing panic recovered: %v", r)
			}
		}()
		if err := bp.handler(flushCtx, records); err != nil {
			log.ErrorfCtx(flushCtx, "Batch processing failed (%d records dropped): %v", len(records), err)
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
	if bp.cancel != nil {
		bp.cancel()
	}
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

// DefaultBatchConfig returns batch defaults: batching disabled (MaxBatchSize 1),
// 100ms wait, no compression, 3 retries.
func DefaultBatchConfig() *BatchConfig {
	return &BatchConfig{
		MaxBatchSize: 1,
		MaxWaitTime:  100 * time.Millisecond,
		Compression:  "none",
		RetryCount:   3,
	}
}
