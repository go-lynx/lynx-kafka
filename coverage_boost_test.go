package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-lynx/lynx-kafka/conf"
	"github.com/go-lynx/lynx/plugins"
	"github.com/twmb/franz-go/pkg/kgo"
)

// ---------------------------------------------------------------------------
// buildTLSConfig
// ---------------------------------------------------------------------------

func TestBuildTLSConfig_NilTLS_ReturnsNil(t *testing.T) {
	cfg, err := buildTLSConfig(nil)
	if err != nil {
		t.Fatalf("expected nil error for nil TLS config, got %v", err)
	}
	if cfg != nil {
		t.Fatal("expected nil TLS config")
	}
}

func TestBuildTLSConfig_Disabled_ReturnsNil(t *testing.T) {
	cfg, err := buildTLSConfig(&conf.TLS{Enabled: false})
	if err != nil {
		t.Fatalf("expected nil error for disabled TLS, got %v", err)
	}
	if cfg != nil {
		t.Fatal("expected nil TLS config for disabled TLS")
	}
}

func TestBuildTLSConfig_EnabledNoFiles_ReturnsConfig(t *testing.T) {
	cfg, err := buildTLSConfig(&conf.TLS{Enabled: true})
	if err != nil {
		t.Fatalf("unexpected error for TLS with no cert files: %v", err)
	}
	if cfg == nil {
		t.Fatal("expected non-nil TLS config")
	}
}

func TestBuildTLSConfig_ServerName_Set(t *testing.T) {
	cfg, err := buildTLSConfig(&conf.TLS{Enabled: true, ServerName: "kafka.example.com"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.ServerName != "kafka.example.com" {
		t.Errorf("expected ServerName kafka.example.com, got %q", cfg.ServerName)
	}
}

func TestBuildTLSConfig_CaFile_NotFound_Error(t *testing.T) {
	_, err := buildTLSConfig(&conf.TLS{Enabled: true, CaFile: "/nonexistent/ca.pem"})
	if err == nil {
		t.Fatal("expected error for missing CA file")
	}
}

func TestBuildTLSConfig_CertFile_NotFound_Error(t *testing.T) {
	_, err := buildTLSConfig(&conf.TLS{Enabled: true, CertFile: "/nonexistent/cert.pem", KeyFile: "/nonexistent/key.pem"})
	if err == nil {
		t.Fatal("expected error for missing client cert/key files")
	}
}

// ---------------------------------------------------------------------------
// ConnectionManager.Start and StartWithContext
// ---------------------------------------------------------------------------

func TestConnectionManager_Start_StartsHealthChecker(t *testing.T) {
	// Use an unreachable broker so bootstrap fails quickly
	cm := NewConnectionManager(makeQuickKgoClient(t), []string{"127.0.0.1:1"})
	done := make(chan struct{})
	go func() {
		cm.Start()
		close(done)
	}()
	// Stop after brief wait
	time.Sleep(50 * time.Millisecond)
	cm.Stop()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Start goroutine did not finish")
	}
}

func TestConnectionManager_StartWithContext_CanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cm := NewConnectionManager(makeQuickKgoClient(t), []string{"127.0.0.1:1"})
	defer cm.Stop()
	err := cm.StartWithContext(ctx)
	if err == nil {
		t.Fatal("expected error for canceled context")
	}
}

func TestConnectionManager_HandleReconnections_ExitsOnStop(t *testing.T) {
	cm := NewConnectionManager(makeQuickKgoClient(t), []string{"127.0.0.1:1"})
	done := make(chan struct{})
	go func() {
		cm.handleReconnections()
		close(done)
	}()
	cm.Stop()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handleReconnections did not exit after Stop")
	}
}

func makeQuickKgoClient(t *testing.T) *kgo.Client {
	t.Helper()
	c, err := kgo.NewClient(kgo.SeedBrokers("127.0.0.1:1"), kgo.DialTimeout(50*time.Millisecond))
	if err != nil {
		t.Fatalf("kgo.NewClient: %v", err)
	}
	return c
}

// ---------------------------------------------------------------------------
// HealthChecker.Start / run / check (direct invocation)
// ---------------------------------------------------------------------------

func TestHealthChecker_StartStop(t *testing.T) {
	c := makeQuickKgoClient(t)
	defer c.Close()
	hc := NewHealthChecker(c, 50*time.Millisecond, 50*time.Millisecond)
	hc.Start()
	time.Sleep(30 * time.Millisecond)
	hc.Stop()
}

// ---------------------------------------------------------------------------
// ProduceWith – broker unavailable path (error path)
// ---------------------------------------------------------------------------

func TestProduceWith_BrokerUnavailableViaConnMgr_Error(t *testing.T) {
	k := newTestProducerClient()
	k.prodConnMgrs["default"] = &ConnectionManager{
		isConnected: false,
		lastErr:     errors.New("tcp connect failed"),
	}
	err := k.ProduceWith(context.Background(), "default", "my-topic", []byte("k"), []byte("v"))
	if !errors.Is(err, ErrBrokerUnavailable) {
		t.Fatalf("expected ErrBrokerUnavailable, got %v", err)
	}
}

func TestProduceWith_ValidConnMgr_NilProducer_Error(t *testing.T) {
	k := NewKafkaClient()
	k.producers["default"] = nil
	k.defaultProducer = "default"
	k.prodConnMgrs["default"] = &ConnectionManager{isConnected: true}
	err := k.ProduceWith(context.Background(), "default", "my-topic", []byte("k"), []byte("v"))
	if !errors.Is(err, ErrProducerNotInitialized) {
		t.Fatalf("expected ErrProducerNotInitialized, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// ProduceBatchWith – more code paths
// ---------------------------------------------------------------------------

func TestProduceBatchWith_ValidTopic_NilProducerEntry(t *testing.T) {
	k := NewKafkaClient()
	k.producers["p1"] = nil
	records := []*kgo.Record{{Topic: "topic", Value: []byte("v")}}
	err := k.ProduceBatchWith(context.Background(), "p1", "topic", records)
	if !errors.Is(err, ErrProducerNotInitialized) {
		t.Fatalf("expected ErrProducerNotInitialized, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// runtime_contract: publishRuntimeContract with rt set
// ---------------------------------------------------------------------------

func TestPublishRuntimeContract_WithRuntime(t *testing.T) {
	k := NewKafkaClient()
	base := plugins.NewSimpleRuntime()
	k.rt = base.WithPluginContext(pluginName)
	k.publishRuntimeContract(true, true)   // must not panic
	k.publishRuntimeContract(false, false) // must not panic
}

func TestRegisterRuntimePluginAlias_WithRuntime(t *testing.T) {
	k := NewKafkaClient()
	base := plugins.NewSimpleRuntime()
	k.rt = base.WithPluginContext(pluginName)
	k.registerRuntimePluginAlias() // must not panic
}
