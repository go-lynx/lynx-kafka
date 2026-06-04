package kafka

import (
	"context"
	"time"

	"github.com/go-lynx/lynx/plugins"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Producer is the message-sending surface. The plain methods target the default
// producer; the *With variants target a named instance.
type Producer interface {
	Produce(ctx context.Context, topic string, key, value []byte) error
	ProduceBatch(ctx context.Context, topic string, records []*kgo.Record) error
	ProduceWith(ctx context.Context, producerName, topic string, key, value []byte) error
	ProduceBatchWith(ctx context.Context, producerName string, topic string, records []*kgo.Record) error
	GetProducer() *kgo.Client
	IsProducerReady() bool
}

// Consumer is the subscription surface. Subscribe uses the first enabled
// instance; SubscribeWith targets a named instance.
type Consumer interface {
	Subscribe(ctx context.Context, topics []string, handler MessageHandler) error
	SubscribeWith(ctx context.Context, consumerName string, topics []string, handler MessageHandler) error
	GetConsumer() *kgo.Client
	IsConsumerReady() bool
}

// ClientInterface is the full plugin contract: producing, consuming, lifecycle,
// metrics, and health.
type ClientInterface interface {
	Producer
	Consumer

	InitializeResources(rt plugins.Runtime) error
	StartupTasks() error
	ShutdownTasks() error
	GetMetrics() *Metrics
	CheckHealth() error
	GetHealthStatus() *HealthStatus
}

// MetricsProvider exposes counters and a reset hook.
type MetricsProvider interface {
	GetStats() map[string]any
	Reset()
}

// HealthCheckerInterface is the health-probe contract.
type HealthCheckerInterface interface {
	Start()
	Stop()
	IsHealthy() bool
	GetLastCheck() time.Time
	GetErrorCount() int
}

// ConnectionManagerInterface is the connection-lifecycle contract.
type ConnectionManagerInterface interface {
	Start()
	Stop()
	IsConnected() bool
	GetHealthChecker() HealthCheckerInterface
	ForceReconnect()
}

// BatchProcessorInterface is the producer-side batching contract.
type BatchProcessorInterface interface {
	AddRecord(ctx context.Context, record *kgo.Record) error
	Flush(ctx context.Context) error
	Close()
}

// RetryHandlerInterface runs an operation with the configured retry policy.
type RetryHandlerInterface interface {
	DoWithRetry(ctx context.Context, operation func() error) error
}

// GoroutinePoolInterface is the bounded worker-pool contract.
type GoroutinePoolInterface interface {
	Submit(task func())
	Wait()
}
