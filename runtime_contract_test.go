package kafka

import (
	"context"
	"testing"

	"github.com/go-lynx/lynx-kafka/conf"
	"github.com/go-lynx/lynx/plugins"
)

func TestKafkaRuntimeContract_NoInstanceLifecycle(t *testing.T) {
	base := plugins.NewSimpleRuntime()
	rt := base.WithPluginContext(pluginName)

	client := NewKafkaClient()
	client.rt = rt
	client.conf = &conf.Kafka{}

	if err := client.StartContext(context.Background(), client); err != nil {
		t.Fatalf("StartContext failed: %v", err)
	}

	if alias, err := base.GetSharedResource(sharedPluginResourceName); err != nil || alias != client {
		t.Fatalf("unexpected shared plugin alias: value=%#v err=%v", alias, err)
	}
	if readiness, err := base.GetSharedResource(sharedReadinessResourceName); err != nil || readiness != true {
		t.Fatalf("unexpected shared readiness: value=%#v err=%v", readiness, err)
	}
	if health, err := base.GetSharedResource(sharedHealthResourceName); err != nil || health != true {
		t.Fatalf("unexpected shared health: value=%#v err=%v", health, err)
	}
	if _, err := rt.GetPrivateResource("config"); err != nil {
		t.Fatalf("private config resource missing: %v", err)
	}

	if err := client.CleanupTasks(); err != nil {
		t.Fatalf("CleanupTasks failed: %v", err)
	}

	if readiness, err := base.GetSharedResource(sharedReadinessResourceName); err != nil || readiness != false {
		t.Fatalf("unexpected shared readiness after cleanup: value=%#v err=%v", readiness, err)
	}
	if health, err := base.GetSharedResource(sharedHealthResourceName); err != nil || health != false {
		t.Fatalf("unexpected shared health after cleanup: value=%#v err=%v", health, err)
	}
}
