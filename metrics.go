package kafka

import (
	"fmt"
	"strings"
	"sync/atomic"
	"time"
)

// Metrics Kafka monitoring metrics (thread-safe via atomic operations)
type Metrics struct {
	// Producer metrics
	producedMessages atomic.Int64
	producedBytes    atomic.Int64
	producerErrors   atomic.Int64
	producerLatency  atomic.Int64 // nanoseconds

	// Consumer metrics
	consumedMessages   atomic.Int64
	consumedBytes      atomic.Int64
	consumerErrors     atomic.Int64
	consumerLatency    atomic.Int64 // nanoseconds
	offsetCommits      atomic.Int64
	offsetCommitErrors atomic.Int64

	// Connection metrics
	connectionErrors atomic.Int64
	reconnections    atomic.Int64
}

func NewMetrics() *Metrics {
	return &Metrics{}
}

func (m *Metrics) IncrementProducedMessages(count int64) {
	m.producedMessages.Add(count)
}

func (m *Metrics) IncrementProducedBytes(bytes int64) {
	m.producedBytes.Add(bytes)
}

func (m *Metrics) IncrementProducerErrors() {
	m.producerErrors.Add(1)
}

func (m *Metrics) IncrementConsumedMessages(count int64) {
	m.consumedMessages.Add(count)
}

func (m *Metrics) IncrementConsumedBytes(bytes int64) {
	m.consumedBytes.Add(bytes)
}

func (m *Metrics) IncrementConsumerErrors() {
	m.consumerErrors.Add(1)
}

func (m *Metrics) IncrementOffsetCommits() {
	m.offsetCommits.Add(1)
}

func (m *Metrics) IncrementOffsetCommitErrors() {
	m.offsetCommitErrors.Add(1)
}

func (m *Metrics) IncrementConnectionErrors() {
	m.connectionErrors.Add(1)
}

func (m *Metrics) IncrementReconnections() {
	m.reconnections.Add(1)
}

func (m *Metrics) SetProducerLatency(latency time.Duration) {
	m.producerLatency.Store(latency.Nanoseconds())
}

func (m *Metrics) SetConsumerLatency(latency time.Duration) {
	m.consumerLatency.Store(latency.Nanoseconds())
}

// GetStats returns a snapshot of all counters as a map.
func (m *Metrics) GetStats() map[string]any {
	return map[string]any{
		"produced_messages":    m.producedMessages.Load(),
		"produced_bytes":       m.producedBytes.Load(),
		"producer_errors":      m.producerErrors.Load(),
		"producer_latency":     time.Duration(m.producerLatency.Load()).String(),
		"consumed_messages":    m.consumedMessages.Load(),
		"consumed_bytes":       m.consumedBytes.Load(),
		"consumer_errors":      m.consumerErrors.Load(),
		"consumer_latency":     time.Duration(m.consumerLatency.Load()).String(),
		"offset_commits":       m.offsetCommits.Load(),
		"offset_commit_errors": m.offsetCommitErrors.Load(),
		"connection_errors":    m.connectionErrors.Load(),
		"reconnections":        m.reconnections.Load(),
	}
}

// Reset resets all metrics
func (m *Metrics) Reset() {
	m.producedMessages.Store(0)
	m.producedBytes.Store(0)
	m.producerErrors.Store(0)
	m.producerLatency.Store(0)
	m.consumedMessages.Store(0)
	m.consumedBytes.Store(0)
	m.consumerErrors.Store(0)
	m.consumerLatency.Store(0)
	m.offsetCommits.Store(0)
	m.offsetCommitErrors.Store(0)
	m.connectionErrors.Store(0)
	m.reconnections.Store(0)
}

// GetPrometheusMetrics renders the counters in Prometheus text exposition format.
func (m *Metrics) GetPrometheusMetrics() string {
	var metrics []string

	metrics = append(metrics,
		"# HELP lynx_kafka_producer_messages_total Total number of messages produced to Kafka",
		"# TYPE lynx_kafka_producer_messages_total counter",
		fmt.Sprintf("lynx_kafka_producer_messages_total %d", m.producedMessages.Load()),
		"",
		"# HELP lynx_kafka_producer_bytes_total Total number of bytes produced to Kafka",
		"# TYPE lynx_kafka_producer_bytes_total counter",
		fmt.Sprintf("lynx_kafka_producer_bytes_total %d", m.producedBytes.Load()),
		"",
		"# HELP lynx_kafka_producer_errors_total Total number of producer errors",
		"# TYPE lynx_kafka_producer_errors_total counter",
		fmt.Sprintf("lynx_kafka_producer_errors_total %d", m.producerErrors.Load()),
		"",
		"# HELP lynx_kafka_producer_latency_seconds Producer latency in seconds",
		"# TYPE lynx_kafka_producer_latency_seconds gauge",
		fmt.Sprintf("lynx_kafka_producer_latency_seconds %f", time.Duration(m.producerLatency.Load()).Seconds()),
		"",
	)

	// Consumer metrics
	metrics = append(metrics,
		"# HELP lynx_kafka_consumer_messages_total Total number of messages consumed from Kafka",
		"# TYPE lynx_kafka_consumer_messages_total counter",
		fmt.Sprintf("lynx_kafka_consumer_messages_total %d", m.consumedMessages.Load()),
		"",
		"# HELP lynx_kafka_consumer_bytes_total Total number of bytes consumed from Kafka",
		"# TYPE lynx_kafka_consumer_bytes_total counter",
		fmt.Sprintf("lynx_kafka_consumer_bytes_total %d", m.consumedBytes.Load()),
		"",
		"# HELP lynx_kafka_consumer_errors_total Total number of consumer errors",
		"# TYPE lynx_kafka_consumer_errors_total counter",
		fmt.Sprintf("lynx_kafka_consumer_errors_total %d", m.consumerErrors.Load()),
		"",
		"# HELP lynx_kafka_consumer_latency_seconds Consumer latency in seconds",
		"# TYPE lynx_kafka_consumer_latency_seconds gauge",
		fmt.Sprintf("lynx_kafka_consumer_latency_seconds %f", time.Duration(m.consumerLatency.Load()).Seconds()),
		"",
	)

	// Offset metrics
	metrics = append(metrics,
		"# HELP lynx_kafka_offset_commits_total Total number of offset commits",
		"# TYPE lynx_kafka_offset_commits_total counter",
		fmt.Sprintf("lynx_kafka_offset_commits_total %d", m.offsetCommits.Load()),
		"",
		"# HELP lynx_kafka_offset_commit_errors_total Total number of offset commit errors",
		"# TYPE lynx_kafka_offset_commit_errors_total counter",
		fmt.Sprintf("lynx_kafka_offset_commit_errors_total %d", m.offsetCommitErrors.Load()),
		"",
	)

	// Connection metrics
	metrics = append(metrics,
		"# HELP lynx_kafka_connection_errors_total Total number of connection errors",
		"# TYPE lynx_kafka_connection_errors_total counter",
		fmt.Sprintf("lynx_kafka_connection_errors_total %d", m.connectionErrors.Load()),
		"",
		"# HELP lynx_kafka_reconnections_total Total number of reconnections",
		"# TYPE lynx_kafka_reconnections_total counter",
		fmt.Sprintf("lynx_kafka_reconnections_total %d", m.reconnections.Load()),
	)

	return strings.Join(metrics, "\n")
}
