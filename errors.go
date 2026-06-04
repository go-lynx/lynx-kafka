package kafka

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	ErrProducerNotInitialized  = errors.New("kafka producer not initialized")
	ErrConsumerNotInitialized  = errors.New("kafka consumer not initialized")
	ErrInvalidConfiguration    = errors.New("invalid kafka configuration")
	ErrNoBrokersConfigured     = errors.New("no kafka brokers configured")
	ErrConsumerNotEnabled      = errors.New("kafka consumer is not enabled")
	ErrProducerNotEnabled      = errors.New("kafka producer is not enabled")
	ErrInvalidCompression      = errors.New("invalid compression type")
	ErrInvalidSASLMechanism    = errors.New("invalid SASL mechanism")
	ErrInvalidStartOffset      = errors.New("invalid start offset")
	ErrNoTopicsSpecified       = errors.New("no topics specified for subscription")
	ErrNoGroupID               = errors.New("consumer group ID is required")
	ErrConnectionFailed        = errors.New("failed to connect to kafka brokers")
	ErrMessageProcessingFailed = errors.New("failed to process message")
	ErrOffsetCommitFailed      = errors.New("failed to commit offsets")

	// New error types
	ErrCircuitBreakerOpen     = errors.New("circuit breaker is open")
	ErrMessageTooLarge        = errors.New("message size exceeds limit")
	ErrTopicNotFound          = errors.New("topic not found")
	ErrPartitionNotFound      = errors.New("partition not found")
	ErrAuthenticationFailed   = errors.New("authentication failed")
	ErrAuthorizationFailed    = errors.New("authorization failed")
	ErrNetworkTimeout         = errors.New("network timeout")
	ErrBrokerUnavailable      = errors.New("broker unavailable")
	ErrMessageSerialization   = errors.New("message serialization failed")
	ErrMessageDeserialization = errors.New("message deserialization failed")
	ErrBatchProcessorClosed   = errors.New("kafka batch processor is closed")
)

// ErrorType classifies Kafka errors by root cause so callers can apply
// different handling strategies (e.g. retry vs. alert vs. dead-letter).
type ErrorType int

const (
	ErrorTypeNetwork        ErrorType = iota // transient network or broker connectivity issue
	ErrorTypeConfiguration                   // misconfiguration (bad broker address, invalid ACKs, etc.)
	ErrorTypeAuthentication                  // SASL / TLS authentication failure
	ErrorTypeAuthorization                   // ACL / authorisation denied
	ErrorTypeSerialization                   // message serialisation or deserialisation failure
	ErrorTypeBusiness                        // application-level handler error
	ErrorTypeSystem                          // unexpected internal error
)

// Error is a classified Kafka error carrying its type, cause, timestamp, and
// arbitrary context for structured handling.
type Error struct {
	Type    ErrorType
	Message string
	Cause   error
	Time    time.Time
	Context map[string]any
}

func (e *Error) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("%s: %v", e.Message, e.Cause)
	}
	return e.Message
}

func (e *Error) Unwrap() error {
	return e.Cause
}

func NewError(errType ErrorType, message string, cause error) *Error {
	return &Error{
		Type:    errType,
		Message: message,
		Cause:   cause,
		Time:    time.Now(),
		Context: make(map[string]any),
	}
}

// CircuitBreaker trips after threshold consecutive failures, rejects calls for
// timeout, then allows up to halfOpenLimit probes before closing again.
type CircuitBreaker struct {
	mu              sync.RWMutex
	state           CircuitBreakerState
	failureCount    int
	lastFailureTime time.Time
	lastSuccessTime time.Time
	threshold       int
	timeout         time.Duration
	halfOpenLimit   int
	halfOpenCount   int
}

// CircuitBreakerState represents the current state of a CircuitBreaker.
type CircuitBreakerState int

const (
	CircuitBreakerClosed   CircuitBreakerState = iota // normal operation; requests are forwarded
	CircuitBreakerOpen                                // failure threshold exceeded; requests are rejected immediately
	CircuitBreakerHalfOpen                            // timeout elapsed; a limited probe is allowed to test recovery
)

// NewCircuitBreaker starts closed with the given failure threshold and open
// timeout; the half-open probe limit defaults to 5.
func NewCircuitBreaker(threshold int, timeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		state:         CircuitBreakerClosed,
		threshold:     threshold,
		timeout:       timeout,
		halfOpenLimit: 5,
	}
}

// Call runs operation unless the breaker is open, recording the outcome to
// drive state transitions. Returns ErrCircuitBreakerOpen when rejected.
func (cb *CircuitBreaker) Call(operation func() error) error {
	if !cb.canExecute() {
		return ErrCircuitBreakerOpen
	}

	err := operation()
	cb.recordResult(err)
	return err
}

// canExecute checks if execution is allowed and advances state when appropriate.
// It holds a full write-lock for the entire check-and-mutate to avoid the
// "unlock-relock" race that existed when upgrading from RLock to Lock.
func (cb *CircuitBreaker) canExecute() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case CircuitBreakerClosed:
		return true
	case CircuitBreakerOpen:
		if time.Since(cb.lastFailureTime) >= cb.timeout {
			cb.state = CircuitBreakerHalfOpen
			cb.halfOpenCount = 0
			return true
		}
		return false
	case CircuitBreakerHalfOpen:
		return cb.halfOpenCount < cb.halfOpenLimit
	default:
		return false
	}
}

// recordResult records the execution result and updates state accordingly.
func (cb *CircuitBreaker) recordResult(err error) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if err != nil {
		cb.failureCount++
		cb.lastFailureTime = time.Now()

		switch cb.state {
		case CircuitBreakerClosed:
			if cb.failureCount >= cb.threshold {
				cb.state = CircuitBreakerOpen
			}
		case CircuitBreakerHalfOpen:
			// A single failure in half-open trips the breaker again.
			cb.state = CircuitBreakerOpen
		}
	} else {
		cb.failureCount = 0
		cb.lastSuccessTime = time.Now()

		if cb.state == CircuitBreakerHalfOpen {
			cb.halfOpenCount++
			if cb.halfOpenCount >= cb.halfOpenLimit {
				cb.state = CircuitBreakerClosed
				cb.halfOpenCount = 0
			}
		}
	}
}

func (cb *CircuitBreaker) GetState() CircuitBreakerState {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.state
}

// GetStats returns a snapshot of breaker state and counters.
func (cb *CircuitBreaker) GetStats() map[string]any {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	return map[string]any{
		"state":           cb.state,
		"failure_count":   cb.failureCount,
		"threshold":       cb.threshold,
		"timeout":         cb.timeout.String(),
		"last_failure":    cb.lastFailureTime,
		"last_success":    cb.lastSuccessTime,
		"half_open_count": cb.halfOpenCount,
		"half_open_limit": cb.halfOpenLimit,
	}
}
