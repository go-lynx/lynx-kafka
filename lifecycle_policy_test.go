package kafka

import (
	"context"
	"testing"
	"time"

	"github.com/go-lynx/lynx-kafka/conf"
	"github.com/go-lynx/lynx/pkg/security"
	"github.com/go-lynx/lynx/plugins"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"
)

// The production lifecycle policy (lynx/internal/app/lifecycle_policy.go) rejects
// any plugin for which plugins.HasTrueContextLifecycle is false when
// security.IsProduction() is true. These tests pin the plugin to that contract.
func TestClient_HasTrueContextLifecycle(t *testing.T) {
	k := NewKafkaClient()

	caps := plugins.DescribePluginCapabilities(k)
	assert.True(t, caps.HasLifecycleWithCtx, "plugin must expose StartContext/StopContext/InitializeContext")
	assert.True(t, caps.HasContextSteps, "plugin must implement a context-aware step hook")
	assert.True(t, caps.IsTrulyContextAware)
	assert.True(t, plugins.HasTrueContextLifecycle(k))

	_, ok := plugins.GetTrueContextLifecycle(k)
	assert.True(t, ok)

	var _ plugins.ContextStartupTasker = k
	var _ plugins.ContextCleanupTasker = k
}

func TestClient_ProductionLifecyclePolicyAccepts(t *testing.T) {
	t.Setenv("LYNX_ENV", "production")
	require.True(t, security.IsProduction())

	k := NewKafkaClient()
	assert.True(t, plugins.HasTrueContextLifecycle(k),
		"plugin %s would be rejected by the production lifecycle policy", k.Name())
}

func TestClient_StartupTasksContext_ObservesCancellation(t *testing.T) {
	k := NewKafkaClient()
	k.conf = &conf.Kafka{
		Brokers:     []string{"127.0.0.1:1"},
		DialTimeout: durationpb.New(5 * time.Second),
		Producers: []*conf.Producer{{
			Enabled:      true,
			Name:         "cancel-producer",
			RequiredAcks: 1,
			BatchSize:    1,
		}},
	}
	k.setDefaultValues()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()
	err := k.StartupTasksContext(ctx)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
	assert.Less(t, time.Since(start), time.Second, "cancelled startup must return promptly without dialing")
	assert.Empty(t, k.producers)
}

func TestClient_CleanupTasksContext_ObservesCancellation(t *testing.T) {
	k := NewKafkaClient()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := k.CleanupTasksContext(ctx)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}
