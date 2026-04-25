package kafka

import (
	"context"
	"sync"
	"time"

	"github.com/go-lynx/lynx/log"
	"github.com/twmb/franz-go/pkg/kgo"
)

// BatchProcessor batch processor
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

// NewBatchProcessor creates a new batch processor
func NewBatchProcessor(maxBatchSize int, maxWaitTime time.Duration, handler func(context.Context, []*kgo.Record) error) *BatchProcessor {
	bp := &BatchProcessor{
		maxBatchSize: maxBatchSize,
		maxWaitTime:  maxWaitTime,
		handler:      handler,
		records:      make([]*kgo.Record, 0, maxBatchSize),
	}
	return bp
}

// AddRecord adds a record to the batch processor
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

	// If maximum batch size is reached, process immediately
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

	// Copy records and clear original slice
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

// Close closes the batch processor
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
	MaxBatchSize int           // Maximum batch size
	MaxWaitTime  time.Duration // Maximum wait time
	Compression  string        // Compression type
	RetryCount   int           // Retry count
}

// DefaultBatchConfig default batch processing configuration
func DefaultBatchConfig() *BatchConfig {
	return &BatchConfig{
		MaxBatchSize: 1000,
		MaxWaitTime:  100 * time.Millisecond,
		Compression:  "none",
		RetryCount:   3,
	}
}
