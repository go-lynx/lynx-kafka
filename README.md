# Kafka Plugin for Lynx Framework

The Kafka Plugin integrates Apache Kafka into the Lynx framework: message production and consumption backed by franz-go, with batch processing, retries, per-producer circuit breakers, and Prometheus monitoring.

## Version & Migration Notes

- Module path: `github.com/go-lynx/lynx-kafka`
- Plugin name: `kafka.client`
- Local release audit note: the current [`go.mod`](./go.mod) still requires `github.com/go-lynx/lynx v1.6.0-beta`; this README reflects the landed API shape, not a completed stable-version sweep across every module.
- Multi-instance producer/consumer routing is already the primary API surface. `Produce()` targets the default producer, while `ProduceWith()` / `ProduceBatchWith()` target named producer instances configured under `lynx.kafka.producers`.
- Startup now fails fast when the initial broker connectivity check cannot establish a usable connection, so callers should handle `StartupTasks()` errors as real readiness failures instead of assuming deferred background recovery.

## Features

### Core Messaging Support
- **Producer/Consumer Pattern**: Full support for Kafka's producer and consumer APIs
- **Batch Processing**: Configurable message batching for high throughput
- **Retry Mechanisms**: Intelligent retry logic with exponential backoff
- **Connection Pooling**: Efficient connection and resource management
- **Graceful Shutdown**: Proper cleanup and resource management

### Advanced Features
- **SASL Authentication**: Support for SASL/PLAIN, SASL/SCRAM-SHA-256, and SASL/SCRAM-SHA-512
- **TLS Encryption**: End-to-end encryption support
- **Compression**: Support for gzip, snappy, lz4, and zstd compression
- **Circuit Breaker**: Per-producer circuit breaker for fault isolation
- **Message Routing**: Multi-producer/consumer instance support

### Performance & Monitoring
- **Prometheus Metrics**: Comprehensive monitoring and alerting
- **Health Checks**: Real-time health monitoring
- **Performance Analytics**: Throughput and latency measurements
- **Error Tracking**: Detailed error categorization and reporting
- **Connection Monitoring**: Real-time connection status tracking

## Architecture

The plugin follows the Lynx framework's layered architecture:

```
┌─────────────────────────────────────────────────────────────┐
│                    Application Layer                        │
├─────────────────────────────────────────────────────────────┤
│                    Kafka Plugin Layer                       │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐ │
│  │   Client    │  │   Metrics   │  │   Configuration    │ │
│  └─────────────┘  └─────────────┘  └─────────────────────┘ │
├─────────────────────────────────────────────────────────────┤
│                    Core Kafka Layer                         │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐ │
│  │  Producer   │  │  Consumer   │  │   Connection       │ │
│  │  Manager    │  │  Manager    │  │   Manager          │ │
│  └─────────────┘  └─────────────┘  └─────────────────────┘ │
├─────────────────────────────────────────────────────────────┤
│                    Kafka Client Layer                      │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐ │
│  │   Kafka     │  │   franz-go  │  │   Authentication   │ │
│  │   Client    │  │   Client    │  │   (SASL/TLS)        │ │
│  └─────────────┘  └─────────────┘  └─────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

## Configuration

### Configuration Fields Reference

All Kafka plugin configuration is delivered through protobuf (`conf/kafka.proto`). The following table lists all available fields:

#### Top-Level Fields (`lynx.kafka`)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `brokers` | `repeated string` | — (required) | Kafka cluster broker address list. Example: `["localhost:9092"]` |
| `producers` | `repeated Producer` | `[]` | Multiple producer instance configurations. |
| `consumers` | `repeated Consumer` | `[]` | Multiple consumer instance configurations. |
| `sasl` | `SASL` | `nil` | SASL authentication configuration. |
| `tls` | `TLS` | `nil` | TLS encryption configuration. |
| `dial_timeout` | `duration` | `10s` | Connection timeout when dialing brokers. Example: `{ seconds: 10 }` |

#### Producer Configuration

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | `bool` | `false` | Whether to enable this producer instance. |
| `name` | `string` | `""` | Instance name for differentiation and routing. |
| `required_acks` | `int32` | `1` | Acknowledgment level: `-1`=all ISR, `1`=leader only, `0`=no ack. |
| `max_retries` | `int32` | `0` | Maximum number of retry attempts. |
| `retry_backoff` | `duration` | `100ms` | Wait time between retries. Example: `{ nanos: 100000000 }` |
| `batch_size` | `int32` | `0` | Batch send size. `0`/`1` = no batching (synchronous `Produce`); `>1` = async fire-and-forget batching flushed at `batch_size` records or `batch_timeout`. |
| `batch_timeout` | `duration` | `1s` | Batch send waiting time. Example: `{ seconds: 1 }` |
| `compression` | `string` | `"snappy"` | Compression: `none`, `gzip`, `snappy`, `lz4`, `zstd`. |
| `topics` | `repeated string` | `[]` | Optional allow-list of topics for routing/permissions. |

#### Consumer Configuration

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | `bool` | `false` | Whether to enable this consumer instance. |
| `name` | `string` | `""` | Instance name for differentiation and routing. |
| `group_id` | `string` | — (required) | Consumer group ID. |
| `topics` | `repeated string` | — (required) | Subscribed topic list. |
| `auto_commit` | `bool` | `true` | Whether to enable auto offset commit. |
| `auto_commit_interval` | `duration` | `5s` | Auto commit interval. Example: `{ seconds: 5 }` |
| `start_offset` | `string` | `"latest"` | Consumption start position: `latest` or `earliest`. |
| `max_concurrency` | `int32` | `1` | Maximum processing concurrency. |
| `min_batch_size` | `int32` | `0` | Minimum records per fetch batch. |
| `max_batch_size` | `int32` | `0` | Maximum records per fetch batch. |
| `max_wait_time` | `duration` | `5s` | Maximum waiting time for fetch requests. Example: `{ seconds: 5 }` |
| `rebalance_timeout` | `duration` | `60s` | Rebalance timeout duration. Example: `{ seconds: 60 }` |

#### SASL Configuration

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | `bool` | `false` | Whether to enable SASL authentication. |
| `mechanism` | `string` | `"PLAIN"` | Auth mechanism: `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`. |
| `username` | `string` | `""` | SASL username. |
| `password` | `string` | `""` | SASL password. |

#### TLS Configuration

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | `bool` | `false` | Whether to enable TLS encryption. |
| `ca_file` | `string` | `""` | Root CA certificate file path. |
| `cert_file` | `string` | `""` | Client certificate file path (optional, for mutual TLS). |
| `key_file` | `string` | `""` | Client private key file path (optional, for mutual TLS). |
| `insecure_skip_verify` | `bool` | `false` | Skip server cert verification (test env only). |
| `server_name` | `string` | `""` | SNI / certificate verification domain override. |

### Basic Configuration Example

```yaml
lynx:
  kafka:
    brokers:
      - "localhost:9092"
      - "localhost:9093"

    producers:
      - name: "default-producer"
        enabled: true
        topics: ["default-topic"]
        max_retries: 3
        retry_backoff: { seconds: 0, nanos: 100000000 }  # 100ms
        batch_size: 16384
        batch_timeout: { seconds: 0, nanos: 10000000 }   # 10ms
        compression: "gzip"

    consumers:
      - name: "default-consumer"
        enabled: true
        topics:
          - "default-topic"
        group_id: "lynx-consumer-group"
        start_offset: "earliest"
        auto_commit: true
        auto_commit_interval: { seconds: 5 }
        max_concurrency: 10
```

### Advanced Configuration

```yaml
lynx:
  kafka:
    brokers:
      - "kafka1:9092"
      - "kafka2:9092"
      - "kafka3:9092"

    # Global dial timeout
    dial_timeout: { seconds: 10 }

    # SASL authentication (top-level, applies to all producers/consumers)
    sasl:
      enabled: true
      mechanism: "PLAIN"
      username: "kafka-user"
      password: "kafka-password"

    # TLS encryption (top-level)
    tls:
      enabled: true
      ca_file: "/path/to/ca-cert.pem"
      cert_file: "/path/to/client-cert.pem"
      key_file: "/path/to/client-key.pem"
      insecure_skip_verify: false

    # Multiple producers
    producers:
      - name: "high-throughput-producer"
        enabled: true
        topics: ["high-throughput-topic"]
        required_acks: -1           # all ISR
        compression: "lz4"
        max_retries: 5
        retry_backoff: { nanos: 200000000 }   # 200ms
        batch_size: 200             # enable BatchProcessor with 200 records
        batch_timeout: { nanos: 5000000 }     # 5ms linger / flush

    # Multiple consumers
    consumers:
      - name: "batch-consumer"
        enabled: true
        topics: ["batch-topic"]
        group_id: "batch-consumer-group"
        start_offset: "earliest"
        auto_commit: false
        max_concurrency: 10
        max_batch_size: 1000
        max_wait_time: { nanos: 500000000 }   # 500ms
        rebalance_timeout: { seconds: 30 }
```

## Usage

### Basic Usage

```go
package main

import (
    "context"
    "github.com/go-lynx/lynx-kafka"
)

func main() {
    // Get Kafka client from plugin manager. The registered plugin name is "kafka.client".
    client := pluginManager.GetPlugin("kafka.client").(kafka.ClientInterface)
    
    // Send message
    err := client.Produce(ctx, "test-topic", []byte("key"), []byte("Hello Kafka"))
    if err != nil {
        log.Fatal(err)
    }
    
    // Subscribe to messages
    err = client.Subscribe(ctx, []string{"test-topic"}, kafka.MessageHandlerFunc(func(ctx context.Context, topic string, partition int32, offset int64, key, value []byte) error {
        log.Printf("Received message: %s", string(value))
        return nil
    }))
    if err != nil {
        log.Fatal(err)
    }
}
```

### Advanced Usage

```go
// Send message with specific producer
err := client.ProduceWith(ctx, "high-throughput-producer", "test-topic", []byte("key"), []byte("value"))

// Send batch messages
records := []*kgo.Record{
    {Topic: "test-topic", Key: []byte("key1"), Value: []byte("value1")},
    {Topic: "test-topic", Key: []byte("key2"), Value: []byte("value2")},
}
err = client.ProduceBatchWith(ctx, "high-throughput-producer", "test-topic", records)

// Subscribe with specific consumer
err = client.SubscribeWith(ctx, "batch-consumer", []string{"test-topic"}, messageHandler)

// Get producer/consumer instances
producer := client.GetProducer()
consumer := client.GetConsumer()
```

### Message Handlers

```go
// Define message handler (implements MessageHandler interface)
messageHandler := kafka.MessageHandlerFunc(func(ctx context.Context, topic string, partition int32, offset int64, key, value []byte) error {
    log.Printf("Received message from topic %s partition %d: %s", topic, partition, string(value))
    return processMessage(value)
})

// Subscribe with handler
err := client.Subscribe(ctx, []string{"test-topic"}, messageHandler)

// Subscribe with custom handler timeout
err = client.SubscribeWithOptions(ctx, "my-consumer", []string{"test-topic"}, messageHandler, &kafka.ConsumerGroupOptions{
    HandlerTimeout: 60 * time.Second,
})
```

## API Reference

### KafkaClientInterface

The main client interface providing access to all Kafka functionality.

#### Core Methods

- `Produce(ctx context.Context, topic string, key, value []byte) error` - Send a message
- `ProduceWith(ctx context.Context, producerName, topic string, key, value []byte) error` - Send with specific producer
- `ProduceBatch(ctx context.Context, topic string, records []*kgo.Record) error` - Send batch messages
- `ProduceBatchWith(ctx context.Context, producerName, topic string, records []*kgo.Record) error` - Send batch messages with a specific producer
- `Subscribe(ctx context.Context, topics []string, handler MessageHandler) error` - Subscribe to topics
- `SubscribeWith(ctx context.Context, consumerName string, topics []string, handler MessageHandler) error` - Subscribe with specific consumer
- `SubscribeWithOptions(ctx context.Context, consumerName string, topics []string, handler MessageHandler, opts *ConsumerGroupOptions) error` - Subscribe with options (e.g. HandlerTimeout, MaxConcurrency)

#### Management Methods

- `GetProducer() *kgo.Client` - Get the default producer instance selected during startup
- `GetConsumer() *kgo.Client` - Get the compatibility consumer handle or any initialized consumer instance
- `IsProducerReady() bool` - Check whether the default producer is ready
- `IsConsumerReady() bool` - Check whether at least one consumer is ready
- `ShutdownTasks() error` - Stop producers, consumers, and background managers during plugin shutdown
#### Monitoring Methods

- `GetMetrics() *Metrics` - Get performance metrics
- `CheckHealth() error` - Perform health check (returns nil if all connections healthy)
- `GetHealthStatus() *HealthStatus` - Get aggregated health status (Healthy, LastError)

## Monitoring and Metrics

### Health Checks

```go
// Check overall health
err := client.CheckHealth()
if err != nil {
    log.Printf("Health check failed: %v", err)
}

// Get detailed health status
health := client.GetHealthStatus()
if health.Healthy {
    log.Printf("All components healthy")
} else {
    log.Printf("Health issues detected: %v", health.LastError)
}
```

### Prometheus Metrics

The plugin exposes comprehensive Prometheus metrics:

#### Producer Metrics
- `lynx_kafka_producer_messages_total` - Total messages sent
- `lynx_kafka_producer_bytes_total` - Total bytes sent
- `lynx_kafka_producer_errors_total` - Total producer errors
- `lynx_kafka_producer_latency_seconds` - Message send latency

#### Consumer Metrics
- `lynx_kafka_consumer_messages_total` - Total messages received
- `lynx_kafka_consumer_bytes_total` - Total bytes received
- `lynx_kafka_consumer_errors_total` - Total consumer errors
- `lynx_kafka_consumer_latency_seconds` - Consumer processing latency
- `lynx_kafka_offset_commits_total` - Offset commits
- `lynx_kafka_offset_commit_errors_total` - Offset commit errors

#### Connection Metrics
- `lynx_kafka_connection_errors_total` - Connection errors
- `lynx_kafka_reconnections_total` - Reconnection count

## Performance Tuning

### Producer Optimization

```yaml
producers:
  - name: "optimized-producer"
    enabled: true
    required_acks: 1
    max_retries: 3
    retry_backoff: { nanos: 100000000 }   # 100ms
    batch_size: 200                       # Increase async batch size
    batch_timeout: { nanos: 5000000 }     # 5ms
    compression: "lz4"
```

### Consumer Optimization

```yaml
consumers:
  - name: "optimized-consumer"
    enabled: true
    group_id: "optimized-group"
    topics: ["orders"]
    start_offset: "earliest"
    max_concurrency: 10
    min_batch_size: 100
    max_batch_size: 1000
    max_wait_time: { nanos: 500000000 }   # 500ms
    rebalance_timeout: { seconds: 30 }
    auto_commit: false
```

## Troubleshooting

### Common Issues

1. **Connection Failed**
   - Check broker addresses and ports
   - Verify network connectivity
   - Check firewall settings

2. **Authentication Errors**
   - Verify SASL credentials
   - Check TLS certificates
   - Validate security configuration

3. **Performance Issues**
   - Monitor batch processing settings
   - Check compression configuration
   - Review retry settings

4. **Consumer Lag**
   - Increase consumer instances
   - Optimize processing logic
   - Check consumer group configuration

### Debug Mode

Enable framework debug logging for detailed troubleshooting:

```yaml
lynx:
  log:
    level: debug
```

## Best Practices

- **Messages**: use meaningful keys (records with the same key keep order within a partition) and design handlers to be idempotent, since retries and rebalances can redeliver.
- **Consumers**: with `auto_commit: false`, offsets advance only through the last consecutively-successful record in a partition batch, so a failing handler halts progress on that partition until it succeeds.
- **Throughput**: set `batch_size > 1` and a `batch_timeout` to enable the async batch processor; pick a `compression` codec that matches your payload.
- **Operations**: watch `lynx_kafka_consumer_*` and `lynx_kafka_*_errors_total` for lag and failures, and treat `StartupTasks()` errors as real readiness failures.

## Dependencies

- `github.com/twmb/franz-go` - High-performance Kafka client
- `github.com/go-lynx/lynx` - Lynx framework core
- `github.com/prometheus/client_golang` - Prometheus metrics

## License

This plugin is part of the Lynx framework and follows the same license terms.

## Contributing

Contributions are welcome! Please see the main Lynx framework contribution guidelines.

## Support

For support and questions:
- GitHub Issues: [Lynx Framework Issues](https://github.com/go-lynx/lynx/issues)
- Documentation: [Lynx Documentation](https://lynx.go-lynx.com)
- Community: [Lynx Community](https://community.go-lynx.com)
