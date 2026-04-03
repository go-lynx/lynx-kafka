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
	rt   plugins.Runtime
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
	if err := k.BasePlugin.InitializeResources(rt); err != nil {
		return err
	}
	k.rt = rt
	k.conf = &conf.Kafka{}

	// Load configuration
	err := rt.GetConfig().Value(confPrefix).Scan(k.conf)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidConfiguration, err)
	}

	// 先补全默认值再校验（否则未填 max_concurrency 等字段时为 0 会误报失败）
	k.setDefaultValues()

	if err := k.validateConfiguration(); err != nil {
		return err
	}

	k.logTLSClientCertHint()

	return nil
}

// logTLSClientCertHint 在启用 TLS 但未配置客户端证书时给出一次性提示，避免仅依赖健康检查里的 tls: bad certificate 才排查
func (k *Client) logTLSClientCertHint() {
	if k.conf == nil || k.conf.Tls == nil || !k.conf.Tls.Enabled {
		return
	}
	if k.conf.Tls.CertFile != "" && k.conf.Tls.KeyFile != "" {
		return
	}
	log.Warnf("lynx.kafka.tls: 未设置 cert_file/key_file。若连接 Aiven 等集群并在健康检查中出现 remote error: tls: bad certificate，请在控制台下载 Access certificate 与 Access key（或 service.cert / service.key），写入 certs/ 并在配置中填写 cert_file、key_file（YAML 须为下划线命名）")
}

// StartupTasks startup tasks
func (k *Client) StartupTasks() error {
	ctx, cancel := k.startupContext()
	defer cancel()
	return k.startupTasksContext(ctx)
}

// ShutdownTasks shutdown tasks
func (k *Client) ShutdownTasks() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return k.shutdownTasksContext(ctx)
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
		if cm == nil {
			continue
		}
		if !cm.IsConnected() {
			status.Healthy = false
			if err := cm.LastError(); err != nil {
				status.LastError = fmt.Errorf("producer[%s]: %w", name, err)
			} else {
				status.LastError = fmt.Errorf("producer[%s] not connected", name)
			}
			return status
		}
		if cm.healthChecker != nil && !cm.healthChecker.IsHealthy() {
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
		if cm == nil {
			continue
		}
		if !cm.IsConnected() {
			status.Healthy = false
			if err := cm.LastError(); err != nil {
				status.LastError = fmt.Errorf("consumer[%s]: %w", name, err)
			} else {
				status.LastError = fmt.Errorf("consumer[%s] not connected", name)
			}
			return status
		}
		if cm.healthChecker != nil && !cm.healthChecker.IsHealthy() {
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
