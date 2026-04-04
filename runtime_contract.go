package kafka

import "github.com/go-lynx/lynx/log"

const (
	sharedPluginResourceName     = pluginName + ".plugin"
	sharedReadinessResourceName  = pluginName + ".readiness"
	sharedHealthResourceName     = pluginName + ".health"
	privateReadinessResourceName = "readiness"
	privateHealthResourceName    = "health"
)

func (k *Client) registerRuntimePluginAlias() {
	if k == nil || k.rt == nil {
		return
	}
	if err := k.rt.RegisterSharedResource(sharedPluginResourceName, k); err != nil {
		log.Warnf("failed to register Kafka shared plugin alias: %v", err)
	}
}

func (k *Client) publishRuntimeContract(ready, healthy bool) {
	if k == nil || k.rt == nil {
		return
	}
	for _, item := range []struct {
		name  string
		value any
	}{
		{name: sharedReadinessResourceName, value: ready},
		{name: sharedHealthResourceName, value: healthy},
	} {
		if err := k.rt.RegisterSharedResource(item.name, item.value); err != nil {
			log.Warnf("failed to register Kafka shared runtime contract %s: %v", item.name, err)
		}
	}
	if err := k.rt.RegisterPrivateResource(privateReadinessResourceName, ready); err != nil {
		log.Warnf("failed to register Kafka private readiness resource: %v", err)
	}
	if err := k.rt.RegisterPrivateResource(privateHealthResourceName, healthy); err != nil {
		log.Warnf("failed to register Kafka private health resource: %v", err)
	}
}
