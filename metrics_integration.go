package kafka

import (
	"time"

	"github.com/go-lynx/lynx/log"
	appmetrics "github.com/go-lynx/lynx/observability/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"sync"
)

// hcProvider adapts kafka's HealthChecker to platform HealthProvider
type hcProvider struct {
	get func() *HealthChecker
	// Additional labels (not yet distributed as dynamic labels, reserved only)
	labels map[string]string
}

var registeredHealthCollectors sync.Map

func (p *hcProvider) healthChecker() *HealthChecker {
	if p == nil || p.get == nil {
		return nil
	}
	return p.get()
}

func (p *hcProvider) IsHealthy() bool {
	hc := p.healthChecker()
	if hc == nil {
		return false
	}
	return hc.IsHealthy()
}

func (p *hcProvider) ErrorCount() int {
	hc := p.healthChecker()
	if hc == nil {
		return 0
	}
	return hc.GetErrorCount()
}

func (p *hcProvider) LastCheck() time.Time {
	hc := p.healthChecker()
	if hc == nil {
		return time.Time{}
	}
	return hc.GetLastCheck()
}

func (p *hcProvider) Labels() map[string]string { return p.labels }

func registerHealthCollector(instance string, provider *hcProvider) {
	if instance == "" || provider == nil {
		return
	}
	if _, loaded := registeredHealthCollectors.Load(instance); loaded {
		return
	}

	collector := appmetrics.NewHealthCollector("kafka", instance, pluginVersion, provider)
	if err := appmetrics.RegisterCollector(collector); err != nil {
		if _, ok := err.(prometheus.AlreadyRegisteredError); ok {
			registeredHealthCollectors.Store(instance, collector)
			return
		}
		log.Warnf("failed to register kafka health collector %s: %v", instance, err)
		return
	}
	registeredHealthCollectors.Store(instance, collector)
}

// registerHealthForProducer registers producer instance health metrics in global registry
func (k *Client) registerHealthForProducer(instance string) {
	provider := &hcProvider{
		get: func() *HealthChecker {
			k.mu.RLock()
			defer k.mu.RUnlock()
			cm := k.prodConnMgrs[instance]
			if cm == nil {
				return nil
			}
			return cm.healthChecker
		},
		labels: map[string]string{"role": "producer"},
	}
	registerHealthCollector("producer:"+instance, provider)
}

// registerHealthForConsumer registers consumer instance health metrics in global registry
func (k *Client) registerHealthForConsumer(instance string) {
	provider := &hcProvider{
		get: func() *HealthChecker {
			k.mu.RLock()
			defer k.mu.RUnlock()
			cm := k.consConnMgrs[instance]
			if cm == nil {
				return nil
			}
			return cm.healthChecker
		},
		labels: map[string]string{"role": "consumer"},
	}
	registerHealthCollector("consumer:"+instance, provider)
}
