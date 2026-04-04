package kafka

import (
	"testing"
	"time"
)

func resetRegisteredHealthCollectors() {
	registeredHealthCollectors.Range(func(key, _ any) bool {
		registeredHealthCollectors.Delete(key)
		return true
	})
}

func countRegisteredHealthCollectors() int {
	count := 0
	registeredHealthCollectors.Range(func(_, _ any) bool {
		count++
		return true
	})
	return count
}

func TestRegisterHealthForProducerIsIdempotent(t *testing.T) {
	resetRegisteredHealthCollectors()
	t.Cleanup(resetRegisteredHealthCollectors)

	client := NewKafkaClient()
	client.prodConnMgrs["audit"] = &ConnectionManager{
		healthChecker: &HealthChecker{
			isHealthy: true,
			lastCheck: time.Now(),
		},
	}

	client.registerHealthForProducer("audit")
	client.registerHealthForProducer("audit")

	if got := countRegisteredHealthCollectors(); got != 1 {
		t.Fatalf("registered health collectors = %d, want 1", got)
	}
}

func TestRegisterHealthForConsumerIsIdempotent(t *testing.T) {
	resetRegisteredHealthCollectors()
	t.Cleanup(resetRegisteredHealthCollectors)

	client := NewKafkaClient()
	client.consConnMgrs["billing"] = &ConnectionManager{
		healthChecker: &HealthChecker{
			isHealthy: true,
			lastCheck: time.Now(),
		},
	}

	client.registerHealthForConsumer("billing")
	client.registerHealthForConsumer("billing")

	if got := countRegisteredHealthCollectors(); got != 1 {
		t.Fatalf("registered health collectors = %d, want 1", got)
	}
}
