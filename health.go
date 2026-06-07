package kafka

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/go-lynx/lynx/log"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// HealthChecker periodically probes a client with a Metadata request and flips
// to unhealthy after maxErrors consecutive failures, firing the configured
// callbacks on state transitions.
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

// NewHealthChecker starts healthy with a 3-failure tolerance and no-op callbacks.
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

// Start starts the health check goroutine.
func (hc *HealthChecker) Start() {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.WarnfCtx(hc.ctx, "health checker panic recovered: %v", r)
			}
		}()
		hc.run()
	}()
}

// Stop stops the health check
func (hc *HealthChecker) Stop() {
	hc.cancel()
}

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

func (hc *HealthChecker) check() {
	ctx, cancel := context.WithTimeout(hc.ctx, hc.timeout)
	defer cancel()

	// An empty MetadataRequest probes overall cluster reachability.
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
			// Run the callback off the check loop so it can't stall probes.
			go func(cb func(error), e error) {
				defer func() {
					if r := recover(); r != nil {
						log.WarnfCtx(hc.ctx, "health onUnhealthy callback panic recovered: %v", r)
					}
				}()
				cb(e)
			}(hc.onUnhealthy, err)
		}
		msg := err.Error()
		if strings.Contains(msg, "bad certificate") {
			log.WarnfCtx(hc.ctx, "Kafka health check failed (%d/%d): %v (TLS peer rejected the client certificate; for managed clusters such as Aiven set cert_file + key_file in lynx.kafka.tls to match the Access Certificate / Access Key from the cluster console)",
				hc.errorCount, hc.maxErrors, err)
		} else {
			log.WarnfCtx(hc.ctx, "Kafka health check failed (%d/%d): %v", hc.errorCount, hc.maxErrors, err)
		}
		return
	}

	if !hc.isHealthy {
		// Recovered: unhealthy -> healthy.
		hc.isHealthy = true
		hc.errorCount = 0
		hc.lastErr = nil
		go func(cb func()) {
			defer func() {
				if r := recover(); r != nil {
					log.WarnfCtx(hc.ctx, "health onHealthy callback panic recovered: %v", r)
				}
			}()
			cb()
		}(hc.onHealthy)
		log.InfofCtx(hc.ctx, "Kafka health recovered")
	} else {
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

func (hc *HealthChecker) IsHealthy() bool {
	hc.mu.RLock()
	defer hc.mu.RUnlock()
	return hc.isHealthy
}

func (hc *HealthChecker) GetLastCheck() time.Time {
	hc.mu.RLock()
	defer hc.mu.RUnlock()
	return hc.lastCheck
}

func (hc *HealthChecker) GetErrorCount() int {
	hc.mu.RLock()
	defer hc.mu.RUnlock()
	return hc.errorCount
}

// SetCallbacks installs the handlers invoked on health state transitions.
func (hc *HealthChecker) SetCallbacks(onHealthy func(), onUnhealthy func(error)) {
	hc.mu.Lock()
	defer hc.mu.Unlock()
	hc.onHealthy = onHealthy
	hc.onUnhealthy = onUnhealthy
}

// ConnectionManager tracks connectivity for one client: it runs the health
// checker and, on health loss, drives reconnection with exponential backoff.
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

// Reconnect backoff grows from initialReconnectBackoff up to maxReconnectBackoff.
const initialReconnectBackoff = 2 * time.Second
const maxReconnectBackoff = 60 * time.Second

// initialConnectivityTimeout is the per-attempt deadline for the bootstrap Metadata
// request.  Keep this well below lynx.plugins.start_timeout (default 5 s) so that
// multiple attempts still complete before the plugin startup deadline.
const initialConnectivityTimeout = 2 * time.Second

// bootstrapMaxAttempts is the number of times the initial Metadata probe is retried.
// Brokers occasionally return ILLEGAL_SASL_STATE when the SASL handshake and the first
// Metadata request interleave (see twmb/franz-go#249); a short back-off recovers this.
// Ensure bootstrapMaxAttempts × (initialConnectivityTimeout + bootstrapRetryBackoff) stays
// under the typical plugin start_timeout.
const bootstrapMaxAttempts = 4
const bootstrapRetryBackoff = 100 * time.Millisecond

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

	cm.healthChecker = NewHealthChecker(client, 30*time.Second, 10*time.Second)
	cm.healthChecker.SetCallbacks(
		func() { cm.onHealthy() },
		func(err error) { cm.onUnhealthy(err) },
	)

	return cm
}

// Start starts the connection manager synchronously probing initial connectivity,
// then starts the periodic health checker and reconnect handler.
// The synchronous bootstrap ensures that Lynx's post-start CheckHealth() call does
// not see isConnected==false with a nil lastError (which would be mis-reported as
// "producer not connected" instead of a real broker error).
func (cm *ConnectionManager) Start() {
	_ = cm.bootstrapConnectionStateWithContext(cm.ctx, true)
	cm.healthChecker.Start()
	go cm.handleReconnections()
}

// StartWithContext starts the connection manager and uses the caller context for
// the initial connectivity probe so plugin startup can fail fast on cancellation.
func (cm *ConnectionManager) StartWithContext(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		cm.markDisconnected(err)
		return err
	}
	if err := cm.bootstrapConnectionStateWithContext(ctx, false); err != nil {
		return err
	}
	cm.healthChecker.Start()
	go cm.handleReconnections()
	return nil
}

func (cm *ConnectionManager) Stop() {
	cm.cancel()
	cm.healthChecker.Stop()
}

func (cm *ConnectionManager) onHealthy() {
	cm.markConnected()
	log.InfofCtx(cm.ctx, "Kafka connection established")
}

func (cm *ConnectionManager) onUnhealthy(err error) {
	cm.markDisconnected(err)
	log.ErrorfCtx(cm.ctx, "Kafka connection lost: %v", err)

	select {
	case cm.reconnectChan <- struct{}{}:
	default:
	}
}

// handleReconnections serves reconnect requests until the context is canceled.
func (cm *ConnectionManager) handleReconnections() {
	defer func() {
		if r := recover(); r != nil {
			log.WarnfCtx(cm.ctx, "handleReconnections panic recovered: %v", r)
		}
	}()
	for {
		select {
		case <-cm.ctx.Done():
			return
		case <-cm.reconnectChan:
			cm.reconnect()
		}
	}
}

// reconnect nudges franz-go (which manages connections itself) with a Metadata
// request, then waits out the current backoff and doubles it for next time.
func (cm *ConnectionManager) reconnect() {
	cm.mu.Lock()
	backoff := cm.reconnectBackoff
	cm.mu.Unlock()

	log.InfofCtx(cm.ctx, "Attempting to reconnect to Kafka (backoff %v)...", backoff)
	ctx, cancel := context.WithTimeout(cm.ctx, 10*time.Second)
	defer cancel()
	var req kmsg.MetadataRequest
	_, err := req.RequestWith(ctx, cm.client)
	if err != nil {
		log.WarnfCtx(cm.ctx, "Reconnect metadata request failed: %v", err)
	}

	timer := time.NewTimer(backoff)
	select {
	case <-cm.ctx.Done():
		timer.Stop()
		return
	case <-timer.C:
	}
	cm.mu.Lock()
	nextBackoff := backoff * 2
	if nextBackoff > maxReconnectBackoff {
		nextBackoff = maxReconnectBackoff
	}
	cm.reconnectBackoff = nextBackoff
	cm.mu.Unlock()
}

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

func (cm *ConnectionManager) GetHealthChecker() *HealthChecker {
	return cm.healthChecker
}

// ForceReconnect requests a reconnect attempt; a no-op if one is already queued.
func (cm *ConnectionManager) ForceReconnect() {
	select {
	case cm.reconnectChan <- struct{}{}:
	default:
	}
}

func (cm *ConnectionManager) bootstrapConnectionStateWithContext(ctx context.Context, scheduleReconnect bool) error {
	var lastErr error
	for attempt := 1; attempt <= bootstrapMaxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			cm.markDisconnected(err)
			return err
		}
		attemptCtx, cancel := context.WithTimeout(ctx, initialConnectivityTimeout)
		var req kmsg.MetadataRequest
		_, err := req.RequestWith(attemptCtx, cm.client)
		cancel()
		if err == nil {
			cm.markConnected()
			if attempt > 1 {
				log.InfofCtx(ctx, "Kafka initial connection established after %d attempts", attempt)
			} else {
				log.InfofCtx(ctx, "Kafka initial connection established")
			}
			return nil
		}
		lastErr = err
		retry := attempt < bootstrapMaxAttempts && (errors.Is(err, kerr.IllegalSaslState) || strings.Contains(err.Error(), "ILLEGAL_SASL_STATE"))
		if retry {
			log.WarnfCtx(ctx, "Kafka initial connectivity attempt %d/%d: %v (retrying after SASL state race)", attempt, bootstrapMaxAttempts, err)
			timer := time.NewTimer(bootstrapRetryBackoff * time.Duration(attempt))
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				cm.markDisconnected(ctx.Err())
				return ctx.Err()
			}
			continue
		}
		break
	}
	cm.markDisconnected(lastErr)
	log.WarnfCtx(ctx, "Kafka initial connectivity check failed: %v", lastErr)
	if scheduleReconnect {
		cm.ForceReconnect()
	}
	return lastErr
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
