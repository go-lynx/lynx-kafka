package kafka

import (
	"testing"
	"time"

	"github.com/go-lynx/lynx-kafka/conf"
)

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

func TestMetrics_AllIncrements(t *testing.T) {
	m := NewMetrics()

	m.IncrementProducedMessages(5)
	m.IncrementProducedBytes(100)
	m.IncrementProducerErrors()
	m.SetProducerLatency(10 * time.Millisecond)

	m.IncrementConsumedMessages(3)
	m.IncrementConsumedBytes(60)
	m.IncrementConsumerErrors()
	m.SetConsumerLatency(5 * time.Millisecond)

	m.IncrementOffsetCommits()
	m.IncrementOffsetCommitErrors()
	m.IncrementConnectionErrors()
	m.IncrementReconnections()

	stats := m.GetStats()
	assertInt64(t, stats, "produced_messages", 5)
	assertInt64(t, stats, "produced_bytes", 100)
	assertInt64(t, stats, "producer_errors", 1)
	assertInt64(t, stats, "consumed_messages", 3)
	assertInt64(t, stats, "consumed_bytes", 60)
	assertInt64(t, stats, "consumer_errors", 1)
	assertInt64(t, stats, "offset_commits", 1)
	assertInt64(t, stats, "offset_commit_errors", 1)
	assertInt64(t, stats, "connection_errors", 1)
	assertInt64(t, stats, "reconnections", 1)
}

func TestMetrics_Reset(t *testing.T) {
	m := NewMetrics()
	m.IncrementProducedMessages(10)
	m.IncrementProducerErrors()
	m.Reset()

	stats := m.GetStats()
	assertInt64(t, stats, "produced_messages", 0)
	assertInt64(t, stats, "producer_errors", 0)
}

func TestMetrics_GetPrometheusMetrics(t *testing.T) {
	m := NewMetrics()
	m.IncrementProducedMessages(2)
	m.IncrementConsumedMessages(1)

	s := m.GetPrometheusMetrics()
	if s == "" {
		t.Fatal("expected non-empty Prometheus metrics string")
	}
}

func assertInt64(t *testing.T, stats map[string]any, key string, want int64) {
	t.Helper()
	got, ok := stats[key].(int64)
	if !ok {
		t.Errorf("stats[%q] not int64 (got %T)", key, stats[key])
		return
	}
	if got != want {
		t.Errorf("stats[%q] = %d, want %d", key, got, want)
	}
}

// ---------------------------------------------------------------------------
// validateTopic
// ---------------------------------------------------------------------------

func TestValidateTopic_Empty(t *testing.T) {
	k := NewKafkaClient()
	if err := k.validateTopic(""); err == nil {
		t.Fatal("expected error for empty topic")
	}
}

func TestValidateTopic_TooLong(t *testing.T) {
	k := NewKafkaClient()
	// 250 chars
	long := make([]byte, 250)
	for i := range long {
		long[i] = 'a'
	}
	if err := k.validateTopic(string(long)); err == nil {
		t.Fatal("expected error for too-long topic")
	}
}

func TestValidateTopic_InvalidChars(t *testing.T) {
	k := NewKafkaClient()
	if err := k.validateTopic("topic\x01name"); err == nil {
		t.Fatal("expected error for invalid characters")
	}
}

func TestValidateTopic_Valid(t *testing.T) {
	k := NewKafkaClient()
	for _, topic := range []string{"my-topic", "test.topic.123", "topic_name"} {
		if err := k.validateTopic(topic); err != nil {
			t.Errorf("unexpected error for valid topic %q: %v", topic, err)
		}
	}
}

// ---------------------------------------------------------------------------
// SASL
// ---------------------------------------------------------------------------

func TestGetSASLMechanism_NilSASL(t *testing.T) {
	k := NewKafkaClient()
	k.conf = &conf.Kafka{Sasl: nil}
	if k.getSASLMechanism() != nil {
		t.Fatal("expected nil SASL mechanism when conf.Sasl is nil")
	}
}

func TestGetSASLMechanism_SASLDisabled(t *testing.T) {
	k := NewKafkaClient()
	k.conf = &conf.Kafka{Sasl: &conf.SASL{Enabled: false}}
	if k.getSASLMechanism() != nil {
		t.Fatal("expected nil SASL mechanism when SASL disabled")
	}
}

func TestGetSASLMechanism_PlainEnabled(t *testing.T) {
	k := NewKafkaClient()
	k.conf = &conf.Kafka{Sasl: &conf.SASL{Enabled: true, Mechanism: SASLPlain, Username: "u", Password: "p"}}
	if k.getSASLMechanism() == nil {
		t.Fatal("expected non-nil SASL mechanism for PLAIN")
	}
}

func TestGetSASLMechanism_ScramSHA256(t *testing.T) {
	k := NewKafkaClient()
	k.conf = &conf.Kafka{Sasl: &conf.SASL{Enabled: true, Mechanism: SASLScramSHA256, Username: "u", Password: "p"}}
	if k.getSASLMechanism() == nil {
		t.Fatal("expected non-nil SASL mechanism for SCRAM-SHA-256")
	}
}

func TestGetSASLMechanism_ScramSHA512(t *testing.T) {
	k := NewKafkaClient()
	k.conf = &conf.Kafka{Sasl: &conf.SASL{Enabled: true, Mechanism: SASLScramSHA512, Username: "u", Password: "p"}}
	if k.getSASLMechanism() == nil {
		t.Fatal("expected non-nil SASL mechanism for SCRAM-SHA-512")
	}
}

func TestGetSASLMechanism_UnknownMechanism_ReturnsNil(t *testing.T) {
	k := NewKafkaClient()
	k.conf = &conf.Kafka{Sasl: &conf.SASL{Enabled: true, Mechanism: "KERBEROS"}}
	if k.getSASLMechanism() != nil {
		t.Fatal("expected nil for unsupported SASL mechanism")
	}
}

// ---------------------------------------------------------------------------
// GoroutinePool
// ---------------------------------------------------------------------------

func TestGoroutinePool_Wait(t *testing.T) {
	p := NewGoroutinePool(4)
	done := make(chan struct{})
	p.Submit(func() {
		close(done)
	})
	p.Wait()
	select {
	case <-done:
	default:
		t.Fatal("expected task to run before Wait returns")
	}
}

func TestGoroutinePool_IsClosed(t *testing.T) {
	p := NewGoroutinePool(4)
	if p.IsClosed() {
		t.Fatal("expected pool to be open initially")
	}
	p.Close()
	if !p.IsClosed() {
		t.Fatal("expected pool to be closed after Close()")
	}
}

func TestGoroutinePool_SubmitAfterClose_NoOp(t *testing.T) {
	p := NewGoroutinePool(4)
	p.Close()
	var ran bool
	p.Submit(func() { ran = true })
	if ran {
		t.Fatal("expected task not to run after pool is closed")
	}
}

func TestDefaultPoolConfig(t *testing.T) {
	cfg := DefaultPoolConfig()
	if cfg.Size <= 0 {
		t.Fatalf("expected positive pool size, got %d", cfg.Size)
	}
}

// ---------------------------------------------------------------------------
// DefaultBatchConfig
// ---------------------------------------------------------------------------

func TestDefaultBatchConfig(t *testing.T) {
	cfg := DefaultBatchConfig()
	if cfg == nil {
		t.Fatal("expected non-nil DefaultBatchConfig")
	}
	if cfg.MaxBatchSize <= 0 {
		t.Errorf("expected positive MaxBatchSize, got %d", cfg.MaxBatchSize)
	}
}

// ---------------------------------------------------------------------------
// Error types
// ---------------------------------------------------------------------------

func TestError_ErrorMessage(t *testing.T) {
	e := NewError(ErrorTypeNetwork, "network failure", nil)
	if e.Error() == "" {
		t.Fatal("expected non-empty error message")
	}
	if e.Unwrap() != nil {
		t.Fatal("expected nil Unwrap when no cause")
	}
}

func TestError_WithCause(t *testing.T) {
	cause := ErrConnectionFailed
	e := NewError(ErrorTypeAuthentication, "auth failed", cause)
	if e.Unwrap() != cause {
		t.Fatalf("expected cause %v, got %v", cause, e.Unwrap())
	}
	if e.Error() == "" {
		t.Fatal("expected non-empty error message with cause")
	}
}

// ---------------------------------------------------------------------------
// CheckHealth + GetHealthStatus consumer paths
// ---------------------------------------------------------------------------

func TestCheckHealth_AllHealthy(t *testing.T) {
	k := NewKafkaClient()
	if err := k.CheckHealth(); err != nil {
		t.Fatalf("expected nil error when no connections, got %v", err)
	}
}

func TestGetHealthStatus_ConsumerUnhealthy(t *testing.T) {
	k := NewKafkaClient()
	k.consConnMgrs["c1"] = &ConnectionManager{lastErr: ErrConnectionFailed}
	status := k.GetHealthStatus()
	if status.Healthy {
		t.Fatal("expected unhealthy status when consumer disconnected")
	}
}

func TestGetHealthStatus_ProducerUnhealthyViaHealthChecker(t *testing.T) {
	k := NewKafkaClient()
	hc := &HealthChecker{isHealthy: false, lastErr: ErrConnectionFailed}
	k.prodConnMgrs["p1"] = &ConnectionManager{
		isConnected:   true,
		healthChecker: hc,
	}
	status := k.GetHealthStatus()
	if status.Healthy {
		t.Fatal("expected unhealthy status when health checker is unhealthy")
	}
}

func TestGetHealthStatus_ConsumerUnhealthyViaHealthChecker(t *testing.T) {
	k := NewKafkaClient()
	hc := &HealthChecker{isHealthy: false, lastErr: nil}
	k.consConnMgrs["c1"] = &ConnectionManager{
		isConnected:   true,
		healthChecker: hc,
	}
	status := k.GetHealthStatus()
	if status.Healthy {
		t.Fatal("expected unhealthy status when consumer health checker is unhealthy")
	}
}

// ---------------------------------------------------------------------------
// StartupTasks / logTLSClientCertHint
// ---------------------------------------------------------------------------

func TestStartupTasks_NilConf_Error(t *testing.T) {
	k := NewKafkaClient()
	k.conf = nil
	err := k.StartupTasks()
	if err == nil {
		t.Fatal("expected error from StartupTasks with nil conf")
	}
}

func TestLogTLSClientCertHint_NilConf_NoOp(t *testing.T) {
	k := NewKafkaClient()
	k.conf = nil
	k.logTLSClientCertHint() // must not panic
}

func TestLogTLSClientCertHint_TLSDisabled_NoOp(t *testing.T) {
	k := NewKafkaClient()
	k.conf = &conf.Kafka{Tls: &conf.TLS{Enabled: false}}
	k.logTLSClientCertHint() // must not panic
}

func TestLogTLSClientCertHint_TLSEnabled_WithCerts_NoWarn(t *testing.T) {
	k := NewKafkaClient()
	k.conf = &conf.Kafka{Tls: &conf.TLS{Enabled: true, CertFile: "c.pem", KeyFile: "k.pem"}}
	k.logTLSClientCertHint() // must not panic
}

func TestLogTLSClientCertHint_TLSEnabled_WithoutCerts_Warns(t *testing.T) {
	k := NewKafkaClient()
	k.conf = &conf.Kafka{Tls: &conf.TLS{Enabled: true, CertFile: "", KeyFile: ""}}
	k.logTLSClientCertHint() // must not panic, issues a warning log
}

// ---------------------------------------------------------------------------
// ensureLifecycleContext
// ---------------------------------------------------------------------------

func TestEnsureLifecycleContext_CreatesContextWhenNil(t *testing.T) {
	k := NewKafkaClient()
	// Reset to nil to force re-creation
	k.ctx = nil
	k.cancel = nil
	k.ensureLifecycleContext()
	if k.ctx == nil {
		t.Fatal("expected context to be created")
	}
	if k.cancel == nil {
		t.Fatal("expected cancel func to be created")
	}
}

func TestEnsureLifecycleContext_ReusesActiveContext(t *testing.T) {
	k := NewKafkaClient()
	original := k.ctx
	k.ensureLifecycleContext()
	// Original should still be valid (not replaced)
	if original == nil {
		t.Fatal("original context should not be nil")
	}
}
