package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/go-lynx/lynx-kafka/conf"
	"github.com/go-lynx/lynx/log"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// MessageHandler processes a single consumed record. Returning an error stops
// the partition's batch at that record so the offset is not advanced past it.
type MessageHandler interface {
	Handle(ctx context.Context, topic string, partition int32, offset int64, key, value []byte) error
}

// MessageHandlerFunc adapts a plain function to MessageHandler.
type MessageHandlerFunc func(ctx context.Context, topic string, partition int32, offset int64, key, value []byte) error

func (f MessageHandlerFunc) Handle(ctx context.Context, topic string, partition int32, offset int64, key, value []byte) error {
	return f(ctx, topic, partition, offset, key, value)
}

// DefaultHandlerTimeout bounds each handler call when no override is supplied.
const DefaultHandlerTimeout = 30 * time.Second

// ConsumerGroup drives one group subscription: it polls fetches and dispatches
// records to per-partition serial workers, preserving in-partition order while
// processing different partitions concurrently via the goroutine pool.
type ConsumerGroup struct {
	client         *kgo.Client
	groupID        string
	topics         []string
	handler        MessageHandler
	pool           *GoroutinePool
	metrics        *Metrics
	ctx            context.Context
	cancel         context.CancelFunc
	mu             sync.RWMutex
	isRunning      bool
	errorChan      chan error
	rebalanceChan  chan RebalanceEvent
	autoCommit     bool
	handlerTimeout time.Duration
	// per-partition serial processing channels, ensuring strict order for the same partition
	partChans map[string]chan []*kgo.Record
	partMu    sync.Mutex

	// internal wait group for background goroutines (errors/rebalances)
	wg sync.WaitGroup
}

// ConsumerGroupOptions optional parameters for creating consumer group
type ConsumerGroupOptions struct {
	// MaxConcurrency concurrency limit. Priority: Options.MaxConcurrency > conf.Consumer.MaxConcurrency > DefaultPoolConfig().Size
	MaxConcurrency int
	// HandlerTimeout timeout per message handler call (default 30s)
	HandlerTimeout time.Duration
}

// RebalanceEvent carries the partitions assigned or revoked in a single
// rebalance, delivered to the group's rebalance handler.
type RebalanceEvent struct {
	Assigned map[string][]int32
	Revoked  map[string][]int32
}

// NewConsumerGroup creates a consumer group using config-derived defaults.
func (k *Client) NewConsumerGroup(client *kgo.Client, c *conf.Consumer, topics []string, handler MessageHandler) *ConsumerGroup {
	return k.NewConsumerGroupWithOptions(client, c, topics, handler, nil)
}

// NewConsumerGroupWithOptions creates a new consumer group with optional overrides (e.g., concurrency)
func (k *Client) NewConsumerGroupWithOptions(client *kgo.Client, c *conf.Consumer, topics []string, handler MessageHandler, opts *ConsumerGroupOptions) *ConsumerGroup {
	ctx, cancel := context.WithCancel(k.ctx)
	// Concurrency precedence: options > config > pool default.
	maxConc := DefaultPoolConfig().Size
	if c != nil && c.MaxConcurrency > 0 {
		maxConc = int(c.MaxConcurrency)
	}
	if opts != nil && opts.MaxConcurrency > 0 {
		maxConc = opts.MaxConcurrency
	}
	autoCommit := false
	groupID := ""
	if c != nil {
		autoCommit = c.AutoCommit
		groupID = c.GetGroupId()
	}
	handlerTimeout := DefaultHandlerTimeout
	if opts != nil && opts.HandlerTimeout > 0 {
		handlerTimeout = opts.HandlerTimeout
	}
	return &ConsumerGroup{
		client:         client,
		groupID:        groupID,
		topics:         topics,
		handler:        handler,
		pool:           NewGoroutinePool(maxConc),
		metrics:        k.metrics,
		ctx:            ctx,
		cancel:         cancel,
		errorChan:      make(chan error, 100),
		rebalanceChan:  make(chan RebalanceEvent, 16),
		autoCommit:     autoCommit,
		handlerTimeout: handlerTimeout,
		partChans:      make(map[string]chan []*kgo.Record),
	}
}

// initConsumerInstance builds a kgo.Client for the named consumer, wiring group
// membership, offset reset, auto-commit (marks mode), rebalance callbacks, and
// TLS/SASL. Rebalance callbacks forward events to the instance's active group.
func (k *Client) initConsumerInstance(name string, cconf *conf.Consumer) (*kgo.Client, error) {
	if cconf == nil {
		return nil, fmt.Errorf("consumer config is nil for %s", name)
	}
	if k.conf == nil {
		return nil, ErrInvalidConfiguration
	}

	dialTimeout := 10 * time.Second
	if k.conf.DialTimeout != nil {
		dialTimeout = k.conf.DialTimeout.AsDuration()
	}
	opts := []kgo.Opt{
		kgo.SeedBrokers(k.conf.Brokers...),
		kgo.ConsumerGroup(cconf.GroupId),
		kgo.ConsumeResetOffset(k.getStartOffset(cconf)),
		kgo.DialTimeout(dialTimeout),
	}
	if cconf.RebalanceTimeout != nil && cconf.RebalanceTimeout.AsDuration() > 0 {
		opts = append(opts, kgo.RebalanceTimeout(cconf.RebalanceTimeout.AsDuration()))
	}

	if cconf.AutoCommit {
		interval := 5 * time.Second
		if cconf.AutoCommitInterval != nil {
			interval = cconf.AutoCommitInterval.AsDuration()
		}
		opts = append(opts,
			kgo.AutoCommitInterval(interval),
			kgo.AutoCommitMarks(),
		)
	}

	// Dispatch rebalance events to the active group of the corresponding instance
	opts = append(opts,
		kgo.OnPartitionsAssigned(func(ctx context.Context, c *kgo.Client, assigned map[string][]int32) {
			var target *ConsumerGroup
			k.mu.RLock()
			// Map the callback's client back to its instance to find the active group.
			for in, cli := range k.consumers {
				if cli == c {
					target = k.activeGroups[in]
					break
				}
			}
			k.mu.RUnlock()
			if target != nil {
				select {
				case <-target.ctx.Done():
				case target.rebalanceChan <- RebalanceEvent{Assigned: assigned}:
				default:
					log.WarnfCtx(ctx, "rebalance channel full, dropping assigned event")
				}
			} else {
				log.InfofCtx(ctx, "Partitions assigned: %+v", assigned)
			}
		}),
		kgo.OnPartitionsRevoked(func(ctx context.Context, c *kgo.Client, revoked map[string][]int32) {
			var target *ConsumerGroup
			k.mu.RLock()
			for in, cli := range k.consumers {
				if cli == c {
					target = k.activeGroups[in]
					break
				}
			}
			k.mu.RUnlock()
			if target != nil {
				select {
				case <-target.ctx.Done():
				case target.rebalanceChan <- RebalanceEvent{Revoked: revoked}:
				default:
					log.WarnfCtx(ctx, "rebalance channel full, dropping revoked event")
				}
			} else {
				log.InfofCtx(ctx, "Partitions revoked: %+v", revoked)
			}
		}),
	)

	if k.conf.Tls != nil && k.conf.Tls.Enabled {
		tlsCfg, err := buildTLSConfig(k.conf.Tls)
		if err != nil {
			return nil, fmt.Errorf("buildTLSConfig failed: %w", err)
		}
		opts = append(opts, kgo.DialTLSConfig(tlsCfg))
	}
	if saslMech := k.getSASLMechanism(); saslMech != nil {
		opts = append(opts, kgo.SASL(saslMech))
	}

	consumer, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrConnectionFailed, err)
	}
	return consumer, nil
}

// getStartOffset maps the configured start offset to a kgo.Offset, defaulting
// to the latest (end) offset.
func (k *Client) getStartOffset(c *conf.Consumer) kgo.Offset {
	if c == nil {
		return kgo.NewOffset().AtEnd()
	}
	switch c.StartOffset {
	case StartOffsetEarliest:
		return kgo.NewOffset().AtStart()
	case StartOffsetLatest:
		fallthrough
	default:
		return kgo.NewOffset().AtEnd()
	}
}

// Subscribe subscribes the given topics on the first enabled consumer instance.
//
// Subscribe blocks the calling goroutine: it runs the consumer poll loop until
// the group is stopped or its context is cancelled, and only returns early on a
// setup error. Callers that need to continue should invoke it in its own
// goroutine.
func (k *Client) Subscribe(ctx context.Context, topics []string, handler MessageHandler) error {
	if len(topics) == 0 {
		return fmt.Errorf("no topics provided")
	}
	if k.conf == nil {
		return ErrInvalidConfiguration
	}
	var chosen *conf.Consumer
	var name string
	for _, c := range k.conf.Consumers {
		if c != nil && c.Enabled {
			chosen = c
			name = c.GetName()
			break
		}
	}
	if chosen == nil {
		return fmt.Errorf("no enabled consumer configured")
	}
	return k.SubscribeWith(ctx, name, topics, handler)
}

// SubscribeWith subscribes on the named consumer instance. Like Subscribe, it
// blocks the calling goroutine while the poll loop runs.
func (k *Client) SubscribeWith(ctx context.Context, consumerName string, topics []string, handler MessageHandler) error {
	return k.SubscribeWithOptions(ctx, consumerName, topics, handler, nil)
}

// SubscribeWithOptions subscribes on the named consumer instance, lazily
// creating the client and connection manager, and replacing any group already
// running on that instance. It blocks the calling goroutine while the poll loop
// runs (see Subscribe).
func (k *Client) SubscribeWithOptions(ctx context.Context, consumerName string, topics []string, handler MessageHandler, opts *ConsumerGroupOptions) error {
	if len(topics) == 0 {
		return fmt.Errorf("no topics provided")
	}
	if handler == nil {
		return fmt.Errorf("message handler is nil")
	}
	if k.conf == nil {
		return ErrInvalidConfiguration
	}
	var cconf *conf.Consumer
	for _, c := range k.conf.Consumers {
		if c != nil && c.Enabled && c.GetName() == consumerName {
			cconf = c
			break
		}
	}
	if cconf == nil {
		return fmt.Errorf("consumer instance %s not found or disabled", consumerName)
	}

	k.mu.RLock()
	consumer := k.consumers[consumerName]
	k.mu.RUnlock()
	if consumer == nil {
		cli, err := k.initConsumerInstance(consumerName, cconf)
		if err != nil {
			return err
		}
		cm := NewConnectionManager(cli, k.conf.GetBrokers())
		if err := cm.StartWithContext(ctx); err != nil {
			cm.Stop()
			cli.Close()
			return fmt.Errorf("consumer connection manager start failed: %w", err)
		}

		k.mu.Lock()
		// Re-check under the write lock: a concurrent Subscribe may have won the
		// race, in which case discard our freshly built client.
		if existing := k.consumers[consumerName]; existing != nil {
			consumer = existing
			k.mu.Unlock()
			cm.Stop()
			cli.Close()
		} else {
			k.consumers[consumerName] = cli
			consumer = cli
			k.consConnMgrs[consumerName] = cm
			log.Infof("Kafka consumer[%s] connection manager started", consumerName)
			k.mu.Unlock()
			k.registerHealthForConsumer(consumerName)
		}
	}

	// Replace any group already running on this instance.
	// The old group is stopped AFTER releasing the lock so a slow Stop()
	// (waiting on in-flight handlers) does not block other goroutines that
	// need a read-lock on k.mu.
	k.mu.Lock()
	old := k.activeGroups[consumerName]
	cg := k.NewConsumerGroupWithOptions(consumer, cconf, topics, handler, opts)
	k.activeGroups[consumerName] = cg
	// Populate legacy single-instance fields for backward compatibility.
	if k.consumer == nil {
		k.consumer = consumer
		k.activeConsumerGroup = cg
	}
	k.mu.Unlock()

	if old != nil {
		old.Stop()
	}

	if err := cg.Start(); err != nil {
		// Roll back: remove the non-running group so the map stays consistent.
		k.mu.Lock()
		if k.activeGroups[consumerName] == cg {
			delete(k.activeGroups, consumerName)
		}
		k.mu.Unlock()
		return fmt.Errorf("consumer group start failed: %w", err)
	}
	return nil
}

// Start launches the error and rebalance handlers, then runs the poll loop in
// the calling goroutine until the group context is canceled. It is not
// re-entrant: a second concurrent call returns an error.
func (cg *ConsumerGroup) Start() error {
	cg.mu.Lock()
	if cg.isRunning {
		cg.mu.Unlock()
		return fmt.Errorf("consumer group is already running")
	}
	cg.isRunning = true
	cg.mu.Unlock()

	defer func() {
		cg.mu.Lock()
		cg.isRunning = false
		cg.mu.Unlock()
	}()

	cg.wg.Add(1)
	go func() {
		defer cg.wg.Done()
		cg.handleErrors()
	}()

	cg.wg.Add(1)
	go func() {
		defer cg.wg.Done()
		cg.handleRebalances()
	}()

	for {
		select {
		case <-cg.ctx.Done():
			// Graceful shutdown is not treated as an error to reduce log noise
			return nil
		default:
		}

		fetches := cg.client.PollFetches(cg.ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, fe := range errs {
				cg.metrics.IncrementConsumerErrors()
				e := fe.Err
				// Context cancel/deadline are expected; retriable errors warn;
				// non-retriable errors are logged and forwarded to errorChan.
				switch {
				case errors.Is(e, context.Canceled):
					log.InfofCtx(cg.ctx, "Consumer fetch canceled: %s[%d]: %v", fe.Topic, fe.Partition, e)
				case errors.Is(e, context.DeadlineExceeded):
					log.WarnfCtx(cg.ctx, "Consumer fetch timeout: %s[%d]: %v", fe.Topic, fe.Partition, e)
				case kerr.IsRetriable(e):
					log.WarnfCtx(cg.ctx, "Consumer fetch retriable error: %s[%d]: %v", fe.Topic, fe.Partition, e)
				default:
					log.ErrorfCtx(cg.ctx, "Consumer fetch non-retriable error: %s[%d]: %v", fe.Topic, fe.Partition, e)
					select {
					case cg.errorChan <- e:
					default:
						log.WarnfCtx(cg.ctx, "Error channel full, dropping non-retriable fetch error: %v", e)
					}
				}
			}
			continue
		}

		cg.processFetches(fetches)
	}
}

// processFetches routes each non-empty partition's records to its serial worker.
func (cg *ConsumerGroup) processFetches(fetches kgo.Fetches) {
	for _, fetch := range fetches {
		for _, topic := range fetch.Topics {
			for _, partition := range topic.Partitions {
				if len(partition.Records) == 0 {
					continue
				}

				if !cg.enqueuePartitionRecords(topic.Topic, partition.Partition, partition.Records) {
					return
				}
			}
		}
	}
}

func (cg *ConsumerGroup) enqueuePartitionRecords(topic string, partition int32, records []*kgo.Record) bool {
	cg.partMu.Lock()
	defer cg.partMu.Unlock()

	ch := cg.getPartitionChanLocked(topic, partition)
	select {
	case <-cg.ctx.Done():
		return false
	case ch <- records:
		return true
	}
}

func (cg *ConsumerGroup) getPartitionChanLocked(topic string, partition int32) chan []*kgo.Record {
	key := cg.partitionKey(topic, partition)
	ch, ok := cg.partChans[key]
	if !ok {
		ch = make(chan []*kgo.Record, 1)
		cg.partChans[key] = ch
		// One worker per partition: each batch is submitted to the pool and
		// awaited before the next batch is taken, so a partition is never
		// processed concurrently with itself.
		go func(t string, p int32, c chan []*kgo.Record) {
			defer func() {
				if r := recover(); r != nil {
					log.ErrorfCtx(cg.ctx, "partition worker panic recovered [%s:%d]: %v", t, p, r)
				}
			}()
			for {
				select {
				case <-cg.ctx.Done():
					return
				case recs, ok := <-c:
					if !ok {
						return
					}
					done := make(chan struct{})
					cg.pool.Submit(func() {
						cg.processRecordsSerial(t, p, recs)
						close(done)
					})
					select {
					case <-cg.ctx.Done():
						return
					case <-done:
					}
				}
			}
		}(topic, partition, ch)
	}
	return ch
}

// closePartitionChan closes the partition's worker channel so its goroutine
// exits; called on revocation and shutdown.
func (cg *ConsumerGroup) closePartitionChan(topic string, partition int32) {
	key := cg.partitionKey(topic, partition)
	cg.partMu.Lock()
	if ch, ok := cg.partChans[key]; ok {
		delete(cg.partChans, key)
		close(ch)
	}
	cg.partMu.Unlock()
}

func (cg *ConsumerGroup) partitionKey(topic string, partition int32) string {
	return fmt.Sprintf("%s:%d", topic, partition)
}

// processRecordsSerial synchronously processes a batch of records for the given partition; stops on first error and only commits the last consecutive success
func (cg *ConsumerGroup) processRecordsSerial(topic string, partition int32, records []*kgo.Record) {
	var lastSuccess *kgo.Record
	timeout := cg.handlerTimeout
	if timeout <= 0 {
		timeout = DefaultHandlerTimeout
	}
	for _, record := range records {
		start := time.Now()
		ctx, cancel := context.WithTimeout(cg.ctx, timeout)
		err := cg.handler.Handle(ctx, topic, partition, record.Offset, record.Key, record.Value)
		cancel()

		if err != nil {
			cg.metrics.IncrementConsumerErrors()
			log.ErrorfCtx(cg.ctx, "Failed to process message from topic %s partition %d: %v", topic, partition, err)
			select {
			case cg.errorChan <- err:
			default:
				log.ErrorfCtx(cg.ctx, "Error channel is full, dropping error: %v", err)
			}
			break // Stop on first error to avoid advancing offset
		}

		cg.metrics.IncrementConsumedMessages(1)
		cg.metrics.IncrementConsumedBytes(int64(len(record.Value)))
		cg.metrics.SetConsumerLatency(time.Since(start))
		lastSuccess = record
	}

	// Commit only up to the last consecutively-successful record: auto-commit
	// marks it for the background committer, otherwise commit synchronously.
	if lastSuccess != nil && cg.client != nil {
		if cg.autoCommit {
			cg.client.MarkCommitRecords(lastSuccess)
		} else {
			if err := cg.client.CommitRecords(cg.ctx, lastSuccess); err != nil {
				cg.metrics.IncrementOffsetCommitErrors()
				log.ErrorfCtx(cg.ctx, "CommitRecords failed for %s[%d]@%d: %v", topic, partition, lastSuccess.Offset, err)
			} else {
				cg.metrics.IncrementOffsetCommits()
			}
		}
	}
}

// handleErrors drains the error channel until the group context is canceled.
// This is the hook point for dead-letter handling.
func (cg *ConsumerGroup) handleErrors() {
	defer func() {
		if r := recover(); r != nil {
			log.ErrorfCtx(cg.ctx, "handleErrors panic recovered: %v", r)
		}
	}()
	for {
		select {
		case <-cg.ctx.Done():
			return
		case err := <-cg.errorChan:
			log.ErrorfCtx(cg.ctx, "Consumer error: %v", err)
		}
	}
}

// handleRebalances handles partition rebalances
func (cg *ConsumerGroup) handleRebalances() {
	defer func() {
		if r := recover(); r != nil {
			log.ErrorfCtx(cg.ctx, "handleRebalances panic recovered: %v", r)
		}
	}()
	for {
		select {
		case <-cg.ctx.Done():
			return
		case ev := <-cg.rebalanceChan:
			if len(ev.Revoked) > 0 {
				log.InfofCtx(cg.ctx, "Partitions revoked: %+v", ev.Revoked)
				// Flush marked offsets before losing the partitions. Only needed
				// in auto-commit mode; sync mode already committed inline.
				if cg.autoCommit {
					if err := cg.client.CommitUncommittedOffsets(cg.ctx); err != nil {
						cg.metrics.IncrementOffsetCommitErrors()
						log.WarnfCtx(cg.ctx, "CommitUncommittedOffsets warn: %v", err)
					} else {
						cg.metrics.IncrementOffsetCommits()
					}
				}
				// Stop workers for revoked partitions so they don't leak.
				for topic, parts := range ev.Revoked {
					for _, p := range parts {
						cg.closePartitionChan(topic, p)
					}
				}
			}
			if len(ev.Assigned) > 0 {
				log.InfofCtx(cg.ctx, "Partitions assigned: %+v", ev.Assigned)
			}
		}
	}
}

// Stop cancels the group and drains it in order: cancel the context, close the
// partition channels so workers stop taking new batches, wait for in-flight
// pool tasks, then wait for the error/rebalance goroutines.
func (cg *ConsumerGroup) Stop() {
	cg.cancel()
	cg.partMu.Lock()
	for key, ch := range cg.partChans {
		delete(cg.partChans, key)
		close(ch)
	}
	cg.partMu.Unlock()
	cg.pool.Close()
	cg.wg.Wait()
}

func (cg *ConsumerGroup) IsRunning() bool {
	cg.mu.RLock()
	defer cg.mu.RUnlock()
	return cg.isRunning
}

// GetConsumer returns the underlying consumer client (for advanced operations),
// preferring the legacy default and falling back to any initialized instance.
func (k *Client) GetConsumer() *kgo.Client {
	k.mu.RLock()
	defer k.mu.RUnlock()
	if k.consumer != nil {
		return k.consumer
	}
	for _, c := range k.consumers {
		if c != nil {
			return c
		}
	}
	return nil
}

// IsConsumerReady reports whether at least one consumer connection is up.
func (k *Client) IsConsumerReady() bool {
	k.mu.RLock()
	defer k.mu.RUnlock()
	if k.consumer != nil {
		for _, cm := range k.consConnMgrs {
			if cm == nil || cm.IsConnected() {
				return true
			}
		}
		return len(k.consConnMgrs) == 0
	}
	for name, c := range k.consumers {
		if c != nil {
			cm := k.consConnMgrs[name]
			if cm == nil || cm.IsConnected() {
				return true
			}
		}
	}
	return false
}
