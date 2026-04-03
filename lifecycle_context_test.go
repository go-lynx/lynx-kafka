package kafka

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/go-lynx/lynx-kafka/conf"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestClient_StartContext_ReturnsProducerConnectivityError(t *testing.T) {
	client := NewKafkaClient()
	client.conf = &conf.Kafka{
		Brokers:     []string{"127.0.0.1:1"},
		DialTimeout: durationpb.New(100 * time.Millisecond),
		Producers: []*conf.Producer{
			{
				Enabled:      true,
				Name:         "ctx-producer",
				RequiredAcks: 1,
				BatchSize:    1,
				BatchTimeout: durationpb.New(time.Millisecond),
			},
		},
	}
	client.setDefaultValues()

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := client.StartContext(ctx, client)
	if err == nil {
		t.Fatal("expected connectivity error when kafka broker is unreachable, got nil")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("expected startup failure to respect caller context, took %v", elapsed)
	}
}

func TestClient_ShutdownTasks_ReturnsBatchFlushError(t *testing.T) {
	client := NewKafkaClient()
	client.batchProcessors["default"] = NewBatchProcessor(10, time.Hour, func(context.Context, []*kgo.Record) error {
		return errors.New("flush failed")
	})

	if err := client.batchProcessors["default"].AddRecord(context.Background(), &kgo.Record{Topic: "test"}); err != nil {
		t.Fatalf("AddRecord: %v", err)
	}

	err := client.ShutdownTasks()
	if err == nil {
		t.Fatal("expected shutdown to surface batch flush error, got nil")
	}
	if !strings.Contains(err.Error(), "flush failed") {
		t.Fatalf("expected flush error in shutdown result, got %v", err)
	}
}
