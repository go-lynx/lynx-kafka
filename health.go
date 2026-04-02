package kafka

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/go-lynx/lynx/log"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// HealthChecker performs health checks on Kafka connections
type HealthChecker struct {
	client      *kgo.Client
	interval    time.Duration
	timeout     time.Duration
	ctx         context.Context
	cancel      context.CancelFunc
	mu          sync.RWMutex
	isHealthy   bool
	lastCheck   time.Time
	lastErr     error
	errorCount  int
	maxErrors   int
	onHealthy   func()
	onUnhealthy func(error)
}

// NewHealthChecker creates a new health checker
func NewHealthChecker(client *kgo.Client, interval, timeout time.Duration) *HealthChecker {
	ctx, cancel := context.WithCancel(context.Background())
	return &HealthChecker{
		client:      client,
		interval:    interval,
		timeout:     timeout,
		ctx:         ctx,
		cancel:      cancel,
		isHealthy:   true,
		maxErrors:   3,
		onHealthy:   func() {},
		onUnhealthy: func(err error) {},
	}
}

// Start starts the health check
func (hc *HealthChecker) Start() {
	go hc.run()
}

// Stop stops the health check
func (hc *HealthChecker) Stop() {
	hc.cancel()
}

// run runs the health check loop
func (hc *HealthChecker) run() {
	ticker := time.NewTicker(hc.interval)
	defer ticker.Stop()

	for {
		select {
		case <-hc.ctx.Done():
			return
		case <-ticker.C:
			hc.check()
		}
	}
}

// check performs health check
func (hc *HealthChecker) check() {
	// Probe cluster health through Metadata request
	ctx, cancel := context.WithTimeout(hc.ctx, hc.timeout)
	defer cancel()

	// Send empty MetadataRequest (request metadata for all topics)
	var req kmsg.MetadataRequest
	_, err := req.RequestWith(ctx, hc.client)

	hc.mu.Lock()
	defer hc.mu.Unlock()
	hc.lastCheck = time.Now()

	if err != nil {
		hc.errorCount++
		hc.lastErr = err
		if hc.isHealthy && hc.errorCount >= hc.maxErrors {
			hc.isHealthy = false
			// Callback should not block main loop
			go hc.onUnhealthy(err)
		}
		msg := err.Error()
		if strings.Contains(msg, "bad certificate") {
			log.WarnfCtx(hc.ctx, "Kafka health check failed (%d/%d): %v (TLS 对端拒绝客户端证书：Aiven 等需在 lynx.kafka.tls 配置 cert_file + key_file，与控制台 Access certificate/key 或 service.cert/service.key 对应)",
				hc.errorCount, hc.maxErrors, err)
		} else {
			log.WarnfCtx(hc.ctx, "Kafka health check failed (%d/%d): %v", hc.errorCount, hc.maxErrors, err)
		}
		return
	}

	if !hc.isHealthy {
		// Status changed from unhealthy -> healthy
		hc.isHealthy = true
		hc.errorCount = 0
		hc.lastErr = nil
		go hc.onHealthy()
		log.InfofCtx(hc.ctx, "Kafka health recovered")
	} else {
		// Maintain health, reset error count
		hc.errorCount = 0
		hc.lastErr = nil
	}
}

// GetLastError returns the last health check error (if any)
func (hc *HealthChecker) GetLastError() error {
	hc.mu.RLock()
	defer hc.mu.RUnlock()
	return hc.lastErr
}

// IsHealthy checks if the connection is healthy
func (hc *HealthChecker) IsHealthy() bool {
	hc.mu.RLock()
	defer hc.mu.RUnlock()
	return hc.isHealthy
}

// GetLastCheck gets the last check time
func (hc *HealthChecker) GetLastCheck() time.Time {
	hc.mu.RLock()
	defer hc.mu.RUnlock()
	return hc.lastCheck
}

// GetErrorCount gets the error count
func (hc *HealthChecker) GetErrorCount() int {
	hc.mu.RLock()
	defer hc.mu.RUnlock()
	return hc.errorCount
}

// SetCallbacks sets callback functions
func (hc *HealthChecker) SetCallbacks(onHealthy func(), onUnhealthy func(error)) {
	hc.mu.Lock()
	defer hc.mu.Unlock()
	hc.onHealthy = onHealthy
	hc.onUnhealthy = onUnhealthy
}

// ConnectionManager manages Kafka connections
type ConnectionManager struct {
	client           *kgo.Client
	brokers          []string
	healthChecker    *HealthChecker
	mu               sync.RWMutex
	isConnected      bool
	lastErr          error
	reconnectChan    chan struct{}
	ctx              context.Context
	cancel           context.CancelFunc
	reconnectBackoff time.Duration // exponential backoff for reconnect
}

// Initial reconnect backoff
const initialReconnectBackoff = 2 * time.Second
const maxReconnectBackoff = 60 * time.Second
// 单次 Metadata 超时；过长会叠加超过 lynx.plugins.start_timeout（默认 5s）导致插件启动失败。
const initialConnectivityTimeout = 2 * time.Second

// 首次 Metadata 时 Broker 偶发 ILLEGAL_SASL_STATE（SASL 握手与后续请求交错，见 twmb/franz-go#249），短暂退避重试可恢复。次数与 initialConnectivityTimeout 乘积勿超过常见 start_timeout。
const bootstrapMaxAttempts = 4
const bootstrapRetryBackoff = 100 * time.Millisecond

// NewConnectionManager creates a new connection manager
func NewConnectionManager(client *kgo.Client, brokers []string) *ConnectionManager {
	ctx, cancel := context.WithCancel(context.Background())
	cm := &ConnectionManager{
		client:           client,
		brokers:          brokers,
		reconnectChan:    make(chan struct{}, 10),
		ctx:              ctx,
		cancel:           cancel,
		reconnectBackoff: initialReconnectBackoff,
	}

	// Create health checker
	cm.healthChecker = NewHealthChecker(client, 30*time.Second, 10*time.Second)
	cm.healthChecker.SetCallbacks(
		func() { cm.onHealthy() },
		func(err error) { cm.onUnhealthy(err) },
	)

	return cm
}

// Start starts the connection manager
func (cm *ConnectionManager) Start() {
	// 必须先同步完成首次 Metadata，再启动周期性健康检查：Lynx 会在插件 Start 后立即同步
	// CheckHealth()；若此处仍异步 bootstrap，会出现 isConnected==false 且 lastErr==nil，误报 producer not connected。
	cm.bootstrapConnectionState()
	cm.healthChecker.Start()
	go cm.handleReconnections()
}

// Stop stops the connection manager
func (cm *ConnectionManager) Stop() {
	cm.cancel()
	cm.healthChecker.Stop()
}

// onHealthy callback when connection is restored
func (cm *ConnectionManager) onHealthy() {
	cm.markConnected()
	log.InfofCtx(cm.ctx, "Kafka connection established")
}

// onUnhealthy callback when connection fails
func (cm *ConnectionManager) onUnhealthy(err error) {
	cm.markDisconnected(err)
	log.ErrorfCtx(cm.ctx, "Kafka connection lost: %v", err)

	// Trigger reconnection
	select {
	case cm.reconnectChan <- struct{}{}:
	default:
	}
}

// handleReconnections handles reconnection
func (cm *ConnectionManager) handleReconnections() {
	for {
		select {
		case <-cm.ctx.Done():
			return
		case <-cm.reconnectChan:
			cm.reconnect()
		}
	}
}

// reconnect reconnection logic with exponential backoff
func (cm *ConnectionManager) reconnect() {
	cm.mu.Lock()
	backoff := cm.reconnectBackoff
	cm.mu.Unlock()

	log.InfofCtx(cm.ctx, "Attempting to reconnect to Kafka (backoff %v)...", backoff)
	// franz-go has built-in connection management, trigger a Metadata request to accelerate recovery
	ctx, cancel := context.WithTimeout(cm.ctx, 10*time.Second)
	defer cancel()
	var req kmsg.MetadataRequest
	_, err := req.RequestWith(ctx, cm.client)
	if err != nil {
		log.WarnfCtx(cm.ctx, "Reconnect metadata request failed: %v", err)
	}

	select {
	case <-cm.ctx.Done():
		return
	case <-time.After(backoff):
	}
	// Exponential backoff for next attempt, cap at max
	cm.mu.Lock()
	nextBackoff := backoff * 2
	if nextBackoff > maxReconnectBackoff {
		nextBackoff = maxReconnectBackoff
	}
	cm.reconnectBackoff = nextBackoff
	cm.mu.Unlock()
}

// IsConnected checks if connected
func (cm *ConnectionManager) IsConnected() bool {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.isConnected
}

// LastError returns the last connection-related error.
func (cm *ConnectionManager) LastError() error {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.lastErr
}

// GetHealthChecker gets the health checker
func (cm *ConnectionManager) GetHealthChecker() *HealthChecker {
	return cm.healthChecker
}

// ForceReconnect forces reconnection
func (cm *ConnectionManager) ForceReconnect() {
	select {
	case cm.reconnectChan <- struct{}{}:
	default:
	}
}

func (cm *ConnectionManager) bootstrapConnectionState() {
	var lastErr error
	for attempt := 1; attempt <= bootstrapMaxAttempts; attempt++ {
		ctx, cancel := context.WithTimeout(cm.ctx, initialConnectivityTimeout)
		var req kmsg.MetadataRequest
		_, err := req.RequestWith(ctx, cm.client)
		cancel()
		if err == nil {
			cm.markConnected()
			if attempt > 1 {
				log.InfofCtx(cm.ctx, "Kafka initial connection established after %d attempts", attempt)
			} else {
				log.InfofCtx(cm.ctx, "Kafka initial connection established")
			}
			return
		}
		lastErr = err
		retry := attempt < bootstrapMaxAttempts && (errors.Is(err, kerr.IllegalSaslState) || strings.Contains(err.Error(), "ILLEGAL_SASL_STATE"))
		if retry {
			log.WarnfCtx(cm.ctx, "Kafka initial connectivity attempt %d/%d: %v (retrying after SASL state race)", attempt, bootstrapMaxAttempts, err)
			time.Sleep(bootstrapRetryBackoff * time.Duration(attempt))
			continue
		}
		break
	}
	cm.markDisconnected(lastErr)
	log.WarnfCtx(cm.ctx, "Kafka initial connectivity check failed: %v", lastErr)
	cm.ForceReconnect()
}

func (cm *ConnectionManager) markConnected() {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.isConnected = true
	cm.lastErr = nil
	cm.reconnectBackoff = initialReconnectBackoff
}

func (cm *ConnectionManager) markDisconnected(err error) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.isConnected = false
	cm.lastErr = err
}
