package kafka

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/go-lynx/lynx-kafka/conf"
	"github.com/go-lynx/lynx/log"
	"github.com/go-lynx/lynx/plugins"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Client Kafka client plugin
type Client struct {
	*plugins.BasePlugin
	conf *conf.Kafka
	// Multi-instance producers/consumers
	producers       map[string]*kgo.Client
	batchProcessors map[string]*BatchProcessor
	defaultProducer string
	consumers       map[string]*kgo.Client
	activeGroups    map[string]*ConsumerGroup // Maintain active groups by consumer instance name
	// Connection managers
	prodConnMgrs map[string]*ConnectionManager
	consConnMgrs map[string]*ConnectionManager
	// Compatible with old fields, to be removed after consumer refactoring is complete
	producer            *kgo.Client
	consumer            *kgo.Client
	activeConsumerGroup *ConsumerGroup
	batchProcessor      *BatchProcessor
	mu                  sync.RWMutex
	ctx                 context.Context
	cancel              context.CancelFunc
	metrics             *Metrics
	retryHandlers       map[string]*RetryHandler   // per-producer retry config
	defaultRetryHandler *RetryHandler              // fallback when producer has no retry config
	circuitBreakers     map[string]*CircuitBreaker // per-producer circuit breaker
}

// Ensure Client implements all interfaces
var _ ClientInterface = (*Client)(nil)
var _ Producer = (*Client)(nil)
var _ Consumer = (*Client)(nil)

// NewKafkaClient creates a new Kafka client plugin instance
func NewKafkaClient() *Client {
	ctx, cancel := context.WithCancel(context.Background())
	return &Client{
		BasePlugin: plugins.NewBasePlugin(
			plugins.GeneratePluginID("", pluginName, pluginVersion),
			pluginName,
			pluginDescription,
			pluginVersion,
			confPrefix,
			100,
		),
		ctx:                 ctx,
		cancel:              cancel,
		metrics:             NewMetrics(),
		retryHandlers:       make(map[string]*RetryHandler),
		defaultRetryHandler: NewRetryHandler(DefaultRetryConfig()),
		circuitBreakers:     make(map[string]*CircuitBreaker),
		producers:           make(map[string]*kgo.Client),
		batchProcessors:     make(map[string]*BatchProcessor),
		consumers:           make(map[string]*kgo.Client),
		activeGroups:        make(map[string]*ConsumerGroup),
		prodConnMgrs:        make(map[string]*ConnectionManager),
		consConnMgrs:        make(map[string]*ConnectionManager),
	}
}

// InitializeResources initializes Kafka resources
func (k *Client) InitializeResources(rt plugins.Runtime) error {
	k.conf = &conf.Kafka{}

	// Load configuration
	err := rt.GetConfig().Value(confPrefix).Scan(k.conf)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidConfiguration, err)
	}

	// Validate configuration
	if err := k.validateConfiguration(); err != nil {
		return err
	}

	// Set default values
	k.setDefaultValues()

	return nil
}

// StartupTasks startup tasks
func (k *Client) StartupTasks() error {
	// Initialize all enabled producer instances
	var firstProducerName string
	for idx, p := range k.conf.Producers {
		if p == nil || !p.Enabled {
			continue
		}
		name := p.Name
		if name == "" {
			name = fmt.Sprintf("producer-%d", idx)
		}
		if _, exists := k.producers[name]; exists {
			log.Warnf("duplicate producer name: %s, skip", name)
			continue
		}
		client, err := k.initProducerInstance(name, p)
		if err != nil {
			return fmt.Errorf("failed to initialize kafka producer %s: %w", name, err)
		}
		k.mu.Lock()
		k.producers[name] = client
		// Create per-producer retry handler from config
		k.retryHandlers[name] = NewRetryHandler(k.retryConfigFromProducer(p))
		// Create per-producer circuit breaker (threshold 5 failures, 30s timeout)
		k.circuitBreakers[name] = NewCircuitBreaker(5, 30*time.Second)
		// Start producer connection manager
		if _, ok := k.prodConnMgrs[name]; !ok {
			cm := NewConnectionManager(client, k.conf.GetBrokers())
			k.prodConnMgrs[name] = cm
			cm.Start()
			log.Infof("Kafka producer[%s] connection manager started", name)
			// Register health metrics
			k.registerHealthForProducer(name)
		}
		if firstProducerName == "" {
			firstProducerName = name
		}
		// Each producer decides whether to enable async batch processing based on configuration
		batchSize := int(p.BatchSize)
		batchTimeout := time.Second
		if p.BatchTimeout != nil {
			batchTimeout = p.BatchTimeout.AsDuration()
		}
		if batchSize > 1 && batchTimeout > 0 {
			bp := NewBatchProcessor(batchSize, batchTimeout, func(ctx context.Context, recs []*kgo.Record) error {
				return k.ProduceBatchWith(ctx, name, "", recs)
			})
			k.batchProcessors[name] = bp
			log.Infof("Kafka producer[%s] batch processor started: size=%d, timeout=%s", name, batchSize, batchTimeout)
		} else {
			log.Infof("Kafka producer[%s] batch processor disabled (batch_size=%d, batch_timeout=%s)", name, batchSize, batchTimeout)
		}
		k.mu.Unlock()
		log.Infof("Kafka producer[%s] initialized successfully", name)
	}
	if firstProducerName != "" {
		k.defaultProducer = firstProducerName
	}

	// Consumers are initialized during Subscribe/SubscribeWith
	return nil
}

// ShutdownTasks shutdown tasks
func (k *Client) ShutdownTasks() error {
	k.cancel() // Cancel all contexts

	// Stop active consumer groups first (before closing clients) to avoid goroutine leaks and use-after-close.
	// Copy refs and release lock before Stop() to avoid deadlock (Stop blocks on pool.Wait).
	k.mu.Lock()
	groups := make(map[string]*ConsumerGroup, len(k.activeGroups))
	for name, cg := range k.activeGroups {
		groups[name] = cg
		delete(k.activeGroups, name)
	}
	k.mu.Unlock()
	for name, cg := range groups {
		if cg != nil {
			cg.Stop()
			log.Infof("Kafka consumer group[%s] stopped", name)
		}
	}

	k.mu.Lock()
	defer k.mu.Unlock()

	// Stop connection managers (before closing clients)
	for name, cm := range k.prodConnMgrs {
		if cm != nil {
			cm.Stop()
			log.Infof("Kafka producer connection manager[%s] stopped", name)
		}
		delete(k.prodConnMgrs, name)
	}
	for name, cm := range k.consConnMgrs {
		if cm != nil {
			cm.Stop()
			log.Infof("Kafka consumer connection manager[%s] stopped", name)
		}
		delete(k.consConnMgrs, name)
	}

	// First gracefully flush all batch processors
	for name, bp := range k.batchProcessors {
		if bp == nil {
			continue
		}
		flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = bp.Flush(flushCtx)
		cancel()
		bp.Close()
		delete(k.batchProcessors, name)
		log.Infof("Kafka producer batch processor[%s] closed", name)
	}

	for name, p := range k.producers {
		if p != nil {
			p.Close()
			log.Infof("Kafka producer[%s] closed", name)
		}
		delete(k.producers, name)
		delete(k.retryHandlers, name)
		delete(k.circuitBreakers, name)
	}
	for name, c := range k.consumers {
		if c != nil {
			c.Close()
			log.Infof("Kafka consumer[%s] closed", name)
		}
		delete(k.consumers, name)
	}

	return nil
}

// GetMetrics gets monitoring metrics
func (k *Client) GetMetrics() *Metrics {
	return k.metrics
}

// retryConfigFromProducer builds RetryConfig from producer config
func (k *Client) retryConfigFromProducer(p *conf.Producer) RetryConfig {
	cfg := DefaultRetryConfig()
	if p == nil {
		return cfg
	}
	if p.MaxRetries > 0 {
		cfg.MaxRetries = int(p.MaxRetries)
	}
	if p.RetryBackoff != nil {
		if d := p.RetryBackoff.AsDuration(); d > 0 {
			cfg.BackoffTime = d
		}
	}
	return cfg
}

// getCircuitBreaker returns circuit breaker for producer
func (k *Client) getCircuitBreaker(producerName string) *CircuitBreaker {
	k.mu.RLock()
	cb := k.circuitBreakers[producerName]
	k.mu.RUnlock()
	return cb
}

// getRetryHandler returns retry handler for producer, or default fallback
func (k *Client) getRetryHandler(producerName string) *RetryHandler {
	k.mu.RLock()
	rh := k.retryHandlers[producerName]
	k.mu.RUnlock()
	if rh != nil {
		return rh
	}
	return k.defaultRetryHandler
}

// HealthStatus represents the aggregated health status of Kafka clients
type HealthStatus struct {
	Healthy   bool
	LastError error
}

// CheckHealth performs health check on all producers and consumers.
// Returns nil if all are healthy, otherwise returns the first error encountered.
func (k *Client) CheckHealth() error {
	status := k.GetHealthStatus()
	if status.Healthy {
		return nil
	}
	return status.LastError
}

// GetHealthStatus returns the aggregated health status of all Kafka connections.
func (k *Client) GetHealthStatus() *HealthStatus {
	k.mu.RLock()
	defer k.mu.RUnlock()

	status := &HealthStatus{Healthy: true}
	for name, cm := range k.prodConnMgrs {
		if cm != nil && cm.healthChecker != nil && !cm.healthChecker.IsHealthy() {
			status.Healthy = false
			if err := cm.healthChecker.GetLastError(); err != nil {
				status.LastError = fmt.Errorf("producer[%s]: %w", name, err)
			} else {
				status.LastError = fmt.Errorf("producer[%s] unhealthy", name)
			}
			return status
		}
	}
	for name, cm := range k.consConnMgrs {
		if cm != nil && cm.healthChecker != nil && !cm.healthChecker.IsHealthy() {
			status.Healthy = false
			if err := cm.healthChecker.GetLastError(); err != nil {
				status.LastError = fmt.Errorf("consumer[%s]: %w", name, err)
			} else {
				status.LastError = fmt.Errorf("consumer[%s] unhealthy", name)
			}
			return status
		}
	}
	return status
}
