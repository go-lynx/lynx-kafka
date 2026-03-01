package kafka

import (
	"context"
	"sync"
	"time"

	"github.com/go-lynx/lynx/log"
	"github.com/twmb/franz-go/pkg/kgo"
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
		log.WarnfCtx(hc.ctx, "Kafka health check failed (%d/%d): %v", hc.errorCount, hc.maxErrors, err)
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
	reconnectChan    chan struct{}
	ctx              context.Context
	cancel           context.CancelFunc
	reconnectBackoff time.Duration // exponential backoff for reconnect
}

// Initial reconnect backoff
const initialReconnectBackoff = 2 * time.Second
const maxReconnectBackoff = 60 * time.Second

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
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.isConnected = true
	cm.reconnectBackoff = initialReconnectBackoff // reset for next failure
	log.InfofCtx(cm.ctx, "Kafka connection established")
}

// onUnhealthy callback when connection fails
func (cm *ConnectionManager) onUnhealthy(err error) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.isConnected = false
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
