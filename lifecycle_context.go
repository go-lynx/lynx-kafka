package kafka

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-lynx/lynx/log"
	"github.com/go-lynx/lynx/plugins"
	"github.com/twmb/franz-go/pkg/kgo"
)

func (k *Client) IsContextAware() bool {
	return true
}

func (k *Client) InitializeContext(ctx context.Context, plugin plugins.Plugin, rt plugins.Runtime) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("initialize canceled before start: %w", err)
	}
	return k.BasePlugin.Initialize(plugin, rt)
}

func (k *Client) StartContext(ctx context.Context, plugin plugins.Plugin) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("start canceled before execution: %w", err)
	}
	if k.Status(plugin) == plugins.StatusActive {
		return plugins.ErrPluginAlreadyActive
	}

	k.SetStatus(plugins.StatusInitializing)
	k.EmitEvent(plugins.PluginEvent{
		Type:     plugins.EventPluginStarting,
		Priority: plugins.PriorityNormal,
		Source:   "StartContext",
		Category: "lifecycle",
	})

	if err := k.startupTasksContext(ctx); err != nil {
		k.SetStatus(plugins.StatusFailed)
		return plugins.NewPluginError(k.ID(), "Start", "Failed to perform startup tasks", err)
	}

	if err := k.CheckHealth(); err != nil {
		k.SetStatus(plugins.StatusFailed)
		log.Errorf("Plugin %s health check failed: %v", plugin.Name(), err)
		return fmt.Errorf("plugin %s health check failed: %w", plugin.Name(), err)
	}

	k.SetStatus(plugins.StatusActive)
	k.EmitEvent(plugins.PluginEvent{
		Type:     plugins.EventPluginStarted,
		Priority: plugins.PriorityNormal,
		Source:   "StartContext",
		Category: "lifecycle",
	})

	return nil
}

func (k *Client) StopContext(ctx context.Context, plugin plugins.Plugin) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("stop canceled before execution: %w", err)
	}
	if k.Status(plugin) != plugins.StatusActive {
		return plugins.NewPluginError(k.ID(), "Stop", "Plugin must be active to stop", plugins.ErrPluginNotActive)
	}

	k.SetStatus(plugins.StatusStopping)
	k.EmitEvent(plugins.PluginEvent{
		Type:     plugins.EventPluginStopping,
		Priority: plugins.PriorityNormal,
		Source:   "StopContext",
		Category: "lifecycle",
	})

	if err := k.shutdownTasksContext(ctx); err != nil {
		k.SetStatus(plugins.StatusFailed)
		return plugins.NewPluginError(k.ID(), "Stop", "Failed to perform cleanup tasks", err)
	}

	k.SetStatus(plugins.StatusTerminated)
	k.EmitEvent(plugins.PluginEvent{
		Type:     plugins.EventPluginStopped,
		Priority: plugins.PriorityNormal,
		Source:   "StopContext",
		Category: "lifecycle",
	})

	return nil
}

func (k *Client) CleanupTasks() error {
	return k.ShutdownTasks()
}

func (k *Client) startupTasksContext(ctx context.Context) (startErr error) {
	if k.conf == nil {
		return ErrInvalidConfiguration
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("kafka startup canceled before execution: %w", err)
	}
	k.publishRuntimeContract(false, false)

	k.ensureLifecycleContext()

	defer func() {
		if startErr == nil {
			return
		}
		if cleanupErr := k.rollbackStartupState(); cleanupErr != nil {
			startErr = errors.Join(startErr, cleanupErr)
		}
	}()

	var firstProducerName string
	for idx, producerConf := range k.conf.Producers {
		if producerConf == nil || !producerConf.Enabled {
			continue
		}

		name := producerConf.Name
		if name == "" {
			name = fmt.Sprintf("producer-%d", idx)
		}

		k.mu.RLock()
		_, exists := k.producers[name]
		k.mu.RUnlock()
		if exists {
			log.Warnf("duplicate producer name: %s, skip", name)
			continue
		}

		client, err := k.initProducerInstance(name, producerConf)
		if err != nil {
			return fmt.Errorf("failed to initialize kafka producer %s: %w", name, err)
		}

		connMgr := NewConnectionManager(client, k.conf.GetBrokers())
		if err := connMgr.StartWithContext(ctx); err != nil {
			connMgr.Stop()
			client.Close()
			return fmt.Errorf("kafka producer[%s] failed to connect to brokers: %w", name, err)
		}

		var batchProcessor *BatchProcessor
		batchSize := int(producerConf.BatchSize)
		batchTimeout := time.Second
		if producerConf.BatchTimeout != nil {
			batchTimeout = producerConf.BatchTimeout.AsDuration()
		}
		if batchSize > 1 && batchTimeout > 0 {
			batchProcessor = NewBatchProcessor(batchSize, batchTimeout, func(ctx context.Context, recs []*kgo.Record) error {
				return k.ProduceBatchWith(ctx, name, "", recs)
			})
		}

		k.mu.Lock()
		k.producers[name] = client
		k.retryHandlers[name] = NewRetryHandler(k.retryConfigFromProducer(producerConf))
		k.circuitBreakers[name] = NewCircuitBreaker(5, 30*time.Second)
		k.prodConnMgrs[name] = connMgr
		if batchProcessor != nil {
			k.batchProcessors[name] = batchProcessor
		}
		if firstProducerName == "" {
			firstProducerName = name
		}
		k.mu.Unlock()

		k.registerHealthForProducer(name)
		if batchProcessor != nil {
			log.Infof("Kafka producer[%s] batch processor started: size=%d, timeout=%s", name, batchSize, batchTimeout)
		} else {
			log.Infof("Kafka producer[%s] batch processor disabled (batch_size=%d, batch_timeout=%s)", name, batchSize, batchTimeout)
		}
		log.Infof("Kafka producer[%s] connection manager started", name)
		log.Infof("Kafka producer[%s] initialized successfully", name)
	}

	k.mu.Lock()
	k.defaultProducer = firstProducerName
	k.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return fmt.Errorf("kafka startup canceled before publishing runtime resources: %w", err)
	}
	if k.rt != nil {
		if err := k.rt.RegisterSharedResource(pluginName, k); err != nil {
			return fmt.Errorf("failed to register Kafka shared resource: %w", err)
		}
		k.registerRuntimePluginAlias()
		if err := k.rt.RegisterPrivateResource("config", k.conf); err != nil {
			log.Warnf("failed to register Kafka private config resource: %v", err)
		}
		if len(k.producers) > 0 {
			if err := k.rt.RegisterPrivateResource("producers", k.producers); err != nil {
				log.Warnf("failed to register Kafka private producers resource: %v", err)
			}
		}
		if len(k.batchProcessors) > 0 {
			if err := k.rt.RegisterPrivateResource("batch_processors", k.batchProcessors); err != nil {
				log.Warnf("failed to register Kafka private batch processors resource: %v", err)
			}
		}
		if len(k.prodConnMgrs) > 0 {
			if err := k.rt.RegisterPrivateResource("producer_connection_managers", k.prodConnMgrs); err != nil {
				log.Warnf("failed to register Kafka private producer connection managers resource: %v", err)
			}
		}
		if len(k.consConnMgrs) > 0 {
			if err := k.rt.RegisterPrivateResource("consumer_connection_managers", k.consConnMgrs); err != nil {
				log.Warnf("failed to register Kafka private consumer connection managers resource: %v", err)
			}
		}
		if len(k.retryHandlers) > 0 {
			if err := k.rt.RegisterPrivateResource("retry_handlers", k.retryHandlers); err != nil {
				log.Warnf("failed to register Kafka private retry handlers resource: %v", err)
			}
		}
		if len(k.circuitBreakers) > 0 {
			if err := k.rt.RegisterPrivateResource("circuit_breakers", k.circuitBreakers); err != nil {
				log.Warnf("failed to register Kafka private circuit breakers resource: %v", err)
			}
		}
		if k.metrics != nil {
			if err := k.rt.RegisterPrivateResource("metrics", k.metrics); err != nil {
				log.Warnf("failed to register Kafka private metrics resource: %v", err)
			}
		}
		if k.defaultRetryHandler != nil {
			if err := k.rt.RegisterPrivateResource("default_retry_handler", k.defaultRetryHandler); err != nil {
				log.Warnf("failed to register Kafka private default retry handler resource: %v", err)
			}
		}
	}

	if err := k.CheckHealth(); err != nil {
		k.publishRuntimeContract(false, false)
		return err
	}
	k.publishRuntimeContract(true, true)

	return nil
}

func (k *Client) shutdownTasksContext(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("kafka shutdown canceled before execution: %w", err)
	}
	k.publishRuntimeContract(false, false)

	k.cancel()

	k.mu.Lock()
	groups := make(map[string]*ConsumerGroup, len(k.activeGroups))
	for name, cg := range k.activeGroups {
		groups[name] = cg
		delete(k.activeGroups, name)
	}

	prodConnMgrs := k.prodConnMgrs
	consConnMgrs := k.consConnMgrs
	batchProcessors := k.batchProcessors
	producers := k.producers
	consumers := k.consumers

	k.prodConnMgrs = make(map[string]*ConnectionManager)
	k.consConnMgrs = make(map[string]*ConnectionManager)
	k.batchProcessors = make(map[string]*BatchProcessor)
	k.producers = make(map[string]*kgo.Client)
	k.consumers = make(map[string]*kgo.Client)
	k.retryHandlers = make(map[string]*RetryHandler)
	k.circuitBreakers = make(map[string]*CircuitBreaker)
	k.defaultProducer = ""
	k.producer = nil
	k.consumer = nil
	k.activeConsumerGroup = nil
	k.batchProcessor = nil
	k.mu.Unlock()

	for name, cg := range groups {
		if cg == nil {
			continue
		}
		cg.Stop()
		log.Infof("Kafka consumer group[%s] stopped", name)
	}

	for name, cm := range prodConnMgrs {
		if cm == nil {
			continue
		}
		cm.Stop()
		log.Infof("Kafka producer connection manager[%s] stopped", name)
	}
	for name, cm := range consConnMgrs {
		if cm == nil {
			continue
		}
		cm.Stop()
		log.Infof("Kafka consumer connection manager[%s] stopped", name)
	}

	var errs []error
	for name, bp := range batchProcessors {
		if bp == nil {
			continue
		}
		flushCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		if err := bp.Flush(flushCtx); err != nil {
			log.Errorf("Kafka producer batch processor[%s] flush failed: %v", name, err)
			errs = append(errs, fmt.Errorf("flush kafka producer batch processor[%s]: %w", name, err))
		}
		cancel()
		bp.Close()
		log.Infof("Kafka producer batch processor[%s] closed", name)
	}

	for name, producer := range producers {
		if producer == nil {
			continue
		}
		producer.Close()
		log.Infof("Kafka producer[%s] closed", name)
	}
	for name, consumer := range consumers {
		if consumer == nil {
			continue
		}
		consumer.Close()
		log.Infof("Kafka consumer[%s] closed", name)
	}

	if err := ctx.Err(); err != nil {
		errs = append(errs, fmt.Errorf("kafka shutdown canceled: %w", err))
	}
	return errors.Join(errs...)
}

func (k *Client) rollbackStartupState() error {
	k.mu.Lock()
	prodConnMgrs := k.prodConnMgrs
	batchProcessors := k.batchProcessors
	producers := k.producers

	k.prodConnMgrs = make(map[string]*ConnectionManager)
	k.batchProcessors = make(map[string]*BatchProcessor)
	k.producers = make(map[string]*kgo.Client)
	k.retryHandlers = make(map[string]*RetryHandler)
	k.circuitBreakers = make(map[string]*CircuitBreaker)
	k.defaultProducer = ""
	k.mu.Unlock()

	for _, cm := range prodConnMgrs {
		if cm != nil {
			cm.Stop()
		}
	}
	for _, bp := range batchProcessors {
		if bp != nil {
			bp.Close()
		}
	}
	for _, producer := range producers {
		if producer != nil {
			producer.Close()
		}
	}
	return nil
}

func (k *Client) ensureLifecycleContext() {
	if k.ctx != nil && k.ctx.Err() == nil && k.cancel != nil {
		return
	}
	k.ctx, k.cancel = context.WithCancel(context.Background())
}

func (k *Client) startupContext() (context.Context, context.CancelFunc) {
	timeout := 10 * time.Second
	if k.conf != nil && k.conf.DialTimeout != nil {
		timeout = k.conf.DialTimeout.AsDuration()
	}
	return context.WithTimeout(context.Background(), timeout)
}
