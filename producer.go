package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/go-lynx/lynx-kafka/conf"
	"github.com/go-lynx/lynx/log"
	"github.com/twmb/franz-go/pkg/kgo"
)

// initProducerInstance builds a kgo.Client for the named producer, wiring
// linger, compression, acks/idempotency, dial timeout, and TLS/SASL.
func (k *Client) initProducerInstance(name string, p *conf.Producer) (*kgo.Client, error) {
	if p == nil {
		return nil, fmt.Errorf("producer config is nil for %s", name)
	}

	// Use BatchTimeout as the linger so batching matches the configured window;
	// default 5ms.
	linger := 5 * time.Millisecond
	if p.BatchTimeout != nil {
		if d := p.BatchTimeout.AsDuration(); d > 0 {
			linger = d
		}
	}

	dialTimeout := 10 * time.Second
	brokers := []string{}
	if k.conf != nil {
		if k.conf.DialTimeout != nil {
			dialTimeout = k.conf.DialTimeout.AsDuration()
		}
		brokers = k.conf.Brokers
	}
	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ProducerLinger(linger),
		kgo.DialTimeout(dialTimeout),
	}

	if k.conf != nil && k.conf.Tls != nil && k.conf.Tls.Enabled {
		tlsCfg, err := buildTLSConfig(k.conf.Tls)
		if err != nil {
			return nil, fmt.Errorf("buildTLSConfig failed: %w", err)
		}
		opts = append(opts, kgo.DialTLSConfig(tlsCfg))
	}

	if saslMech := k.getSASLMechanism(); saslMech != nil {
		opts = append(opts, kgo.SASL(saslMech))
	}

	if comp := k.getCompression(p); comp != kgo.NoCompression() {
		opts = append(opts, kgo.ProducerBatchCompression(comp))
	}

	// RequiredAcks value mapping: -1=AllISRAcks, 1=LeaderAck (default), 0=NoAck
	switch p.RequiredAcks {
	case -1:
		opts = append(opts, kgo.RequiredAcks(kgo.AllISRAcks()))
	case 0:
		opts = append(opts, kgo.RequiredAcks(kgo.NoAck()))
	case 1:
		fallthrough
	default:
		opts = append(opts, kgo.RequiredAcks(kgo.LeaderAck()))
	}

	// franz-go enables idempotent writes by default, which requires acks=all (-1).
	// Disable idempotency for leader-ack or no-ack modes.
	if p.RequiredAcks != -1 {
		opts = append(opts, kgo.DisableIdempotentWrite())
	}

	producer, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrConnectionFailed, err)
	}
	return producer, nil
}

// Produce sends a message via the default producer.
//
// Delivery semantics depend on the producer's batch_size:
//   - batch_size <= 1 (the default): the message is sent synchronously and the
//     returned error reflects broker acknowledgment.
//   - batch_size > 1: the message is enqueued into a producer-owned batch and
//     Produce returns nil immediately (fire-and-forget). The batch is flushed
//     when it reaches batch_size or batch_timeout elapses, whichever comes first,
//     under the producer's own context, so cancelling ctx after Produce returns
//     does not drop the batch. Flush failures are logged and counted in
//     producer error metrics but cannot be returned to the caller.
//
// ctx governs only this call's enqueue/synchronous send.
func (k *Client) Produce(ctx context.Context, topic string, key, value []byte) error {
	k.mu.RLock()
	name := k.defaultProducer
	k.mu.RUnlock()
	if name == "" {
		return ErrProducerNotInitialized
	}
	return k.ProduceWith(ctx, name, topic, key, value)
}

// ProduceWith sends a message through the named producer. When that producer has
// a batch processor (batch_size > 1), the record is enqueued for async batched
// delivery and the call returns immediately; if enqueue fails it falls back to a
// synchronous send. See Produce for the full semantics.
func (k *Client) ProduceWith(ctx context.Context, producerName, topic string, key, value []byte) error {
	if err := k.validateTopic(topic); err != nil {
		return fmt.Errorf("invalid topic %s: %w", topic, err)
	}

	if err := k.getProducerAvailabilityError(producerName); err != nil {
		return err
	}

	k.mu.RLock()
	bp := k.batchProcessors[producerName]
	k.mu.RUnlock()
	if bp != nil {
		record := &kgo.Record{Topic: topic, Key: key, Value: value}
		if err := bp.AddRecord(ctx, record); err != nil {
			log.WarnfCtx(ctx, "Batch enqueue failed, fallback to sync produce: %v", err)
		} else {
			return nil
		}
	}

	k.mu.RLock()
	producer := k.producers[producerName]
	k.mu.RUnlock()

	if producer == nil {
		return ErrProducerNotInitialized
	}

	record := &kgo.Record{
		Topic: topic,
		Key:   key,
		Value: value,
	}

	start := time.Now()
	var err error
	if cb := k.getCircuitBreaker(producerName); cb != nil {
		err = cb.Call(func() error {
			return k.getRetryHandler(producerName).DoWithRetry(ctx, func() error {
				return producer.ProduceSync(ctx, record).FirstErr()
			})
		})
	} else {
		err = k.getRetryHandler(producerName).DoWithRetry(ctx, func() error {
			return producer.ProduceSync(ctx, record).FirstErr()
		})
	}

	if err != nil {
		k.metrics.IncrementProducerErrors()
		if connErr := k.getProducerConnectionError(producerName); connErr != nil {
			log.ErrorfCtx(ctx, "Failed to produce message to topic %s: %v (producer health: %v)", topic, err, connErr)
		} else {
			log.ErrorfCtx(ctx, "Failed to produce message to topic %s: %v", topic, err)
		}
		return fmt.Errorf("failed to produce message: %w", err)
	}

	k.metrics.IncrementProducedMessages(1)
	k.metrics.IncrementProducedBytes(int64(len(value)))
	k.metrics.SetProducerLatency(time.Since(start))

	return nil
}

// ProduceBatch sends a batch of records via the default producer.
func (k *Client) ProduceBatch(ctx context.Context, topic string, records []*kgo.Record) error {
	k.mu.RLock()
	name := k.defaultProducer
	k.mu.RUnlock()
	if name == "" {
		return ErrProducerNotInitialized
	}
	return k.ProduceBatchWith(ctx, name, topic, records)
}

// ProduceBatchWith sends a batch through the named producer. A non-empty topic
// overrides every record's topic; an empty topic requires each record to carry
// its own valid topic. Nil records are dropped.
func (k *Client) ProduceBatchWith(ctx context.Context, producerName string, topic string, records []*kgo.Record) error {
	k.mu.RLock()
	producer := k.producers[producerName]
	k.mu.RUnlock()

	if producer == nil {
		return ErrProducerNotInitialized
	}

	if topic != "" {
		if err := k.validateTopic(topic); err != nil {
			return fmt.Errorf("invalid topic %s: %w", topic, err)
		}
	}

	nonNil := make([]*kgo.Record, 0, len(records))
	for _, r := range records {
		if r == nil {
			continue
		}
		if topic != "" {
			r.Topic = topic
		} else {
			if err := k.validateTopic(r.Topic); err != nil {
				return fmt.Errorf("invalid topic %s: %w", r.Topic, err)
			}
		}
		nonNil = append(nonNil, r)
	}

	if len(nonNil) == 0 {
		return nil
	}

	start := time.Now()
	var err error
	if cb := k.getCircuitBreaker(producerName); cb != nil {
		err = cb.Call(func() error {
			return k.getRetryHandler(producerName).DoWithRetry(ctx, func() error {
				return producer.ProduceSync(ctx, nonNil...).FirstErr()
			})
		})
	} else {
		err = k.getRetryHandler(producerName).DoWithRetry(ctx, func() error {
			return producer.ProduceSync(ctx, nonNil...).FirstErr()
		})
	}

	if err != nil {
		k.metrics.IncrementProducerErrors()
		// With a per-record topic, log the distinct topics (capped at 5) to aid triage.
		if topic == "" {
			topicSet := make(map[string]struct{})
			for _, r := range nonNil {
				topicSet[r.Topic] = struct{}{}
			}
			topics := make([]string, 0, len(topicSet))
			for tp := range topicSet {
				topics = append(topics, tp)
			}
			if len(topics) > 5 {
				topics = topics[:5]
				topics = append(topics, "...")
			}
			log.ErrorfCtx(ctx, "Failed to produce batch messages to topics %v: %v", topics, err)
		} else {
			log.ErrorfCtx(ctx, "Failed to produce batch messages to topic %s: %v", topic, err)
		}
		return fmt.Errorf("failed to produce batch messages: %w", err)
	}

	totalBytes := int64(0)
	for _, record := range nonNil {
		totalBytes += int64(len(record.Value))
	}
	k.metrics.IncrementProducedMessages(int64(len(nonNil)))
	k.metrics.IncrementProducedBytes(totalBytes)
	k.metrics.SetProducerLatency(time.Since(start))

	return nil
}

// getCompression maps the configured codec name to a kgo codec, defaulting to snappy.
func (k *Client) getCompression(p *conf.Producer) kgo.CompressionCodec {
	if p == nil {
		return kgo.SnappyCompression()
	}
	switch p.Compression {
	case CompressionGzip:
		return kgo.GzipCompression()
	case CompressionSnappy:
		return kgo.SnappyCompression()
	case CompressionLz4:
		return kgo.Lz4Compression()
	case CompressionZstd:
		return kgo.ZstdCompression()
	case CompressionNone:
		return kgo.NoCompression()
	default:
		return kgo.SnappyCompression()
	}
}

// GetProducer returns the default producer client, or nil if none is configured.
func (k *Client) GetProducer() *kgo.Client {
	k.mu.RLock()
	defer k.mu.RUnlock()
	if k.defaultProducer == "" {
		return nil
	}
	return k.producers[k.defaultProducer]
}

// IsProducerReady reports whether the default producer exists and is connected.
func (k *Client) IsProducerReady() bool {
	k.mu.RLock()
	defer k.mu.RUnlock()
	if k.defaultProducer == "" {
		return false
	}
	if k.producers[k.defaultProducer] == nil {
		return false
	}
	cm := k.prodConnMgrs[k.defaultProducer]
	if cm == nil {
		return true
	}
	return cm.IsConnected()
}

func (k *Client) getProducerAvailabilityError(producerName string) error {
	k.mu.RLock()
	producer := k.producers[producerName]
	k.mu.RUnlock()
	if producer == nil {
		return ErrProducerNotInitialized
	}
	return k.getProducerConnectionError(producerName)
}

func (k *Client) getProducerConnectionError(producerName string) error {
	k.mu.RLock()
	cm := k.prodConnMgrs[producerName]
	k.mu.RUnlock()
	if cm == nil || cm.IsConnected() {
		return nil
	}
	if err := cm.LastError(); err != nil {
		return fmt.Errorf("%w: producer[%s] unavailable: %v", ErrBrokerUnavailable, producerName, err)
	}
	return fmt.Errorf("%w: producer[%s] not connected", ErrBrokerUnavailable, producerName)
}
