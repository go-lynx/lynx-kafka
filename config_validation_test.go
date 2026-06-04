package kafka

import (
	"errors"
	"testing"

	"github.com/go-lynx/lynx-kafka/conf"
)

// ---------------------------------------------------------------------------
// validateConfiguration
// ---------------------------------------------------------------------------

func TestValidateConfiguration_NilConf(t *testing.T) {
	k := NewKafkaClient()
	k.conf = nil
	err := k.validateConfiguration()
	if !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("expected ErrInvalidConfiguration, got %v", err)
	}
}

func TestValidateConfiguration_NoBrokers(t *testing.T) {
	k := NewKafkaClient()
	k.conf = &conf.Kafka{Brokers: nil}
	err := k.validateConfiguration()
	if !errors.Is(err, ErrNoBrokersConfigured) {
		t.Fatalf("expected ErrNoBrokersConfigured, got %v", err)
	}
}

func TestValidateConfiguration_ValidMinimal(t *testing.T) {
	k := NewKafkaClient()
	k.conf = &conf.Kafka{Brokers: []string{"localhost:9092"}}
	if err := k.validateConfiguration(); err != nil {
		t.Fatalf("unexpected error for minimal valid config: %v", err)
	}
}

func TestValidateConfiguration_InvalidProducer(t *testing.T) {
	k := NewKafkaClient()
	k.conf = &conf.Kafka{
		Brokers: []string{"localhost:9092"},
		Producers: []*conf.Producer{
			{Enabled: true, Compression: "invalid-codec"},
		},
	}
	err := k.validateConfiguration()
	if err == nil {
		t.Fatal("expected error for invalid compression, got nil")
	}
}

func TestValidateConfiguration_InvalidConsumerMissingGroupID(t *testing.T) {
	k := NewKafkaClient()
	k.conf = &conf.Kafka{
		Brokers: []string{"localhost:9092"},
		Consumers: []*conf.Consumer{
			{Enabled: true, GroupId: "", MaxConcurrency: 1},
		},
	}
	err := k.validateConfiguration()
	if !errors.Is(err, ErrNoGroupID) {
		t.Fatalf("expected ErrNoGroupID, got %v", err)
	}
}

func TestValidateConfiguration_InvalidStartOffset(t *testing.T) {
	k := NewKafkaClient()
	k.conf = &conf.Kafka{
		Brokers: []string{"localhost:9092"},
		Consumers: []*conf.Consumer{
			{Enabled: true, GroupId: "grp", StartOffset: "invalid", MaxConcurrency: 1},
		},
	}
	err := k.validateConfiguration()
	if !errors.Is(err, ErrInvalidStartOffset) {
		t.Fatalf("expected ErrInvalidStartOffset, got %v", err)
	}
}

func TestValidateConfiguration_InvalidMaxConcurrency(t *testing.T) {
	k := NewKafkaClient()
	k.conf = &conf.Kafka{
		Brokers: []string{"localhost:9092"},
		Consumers: []*conf.Consumer{
			{Enabled: true, GroupId: "grp", MaxConcurrency: 0},
		},
	}
	err := k.validateConfiguration()
	if err == nil {
		t.Fatal("expected error for max_concurrency<=0, got nil")
	}
}

func TestValidateConfiguration_SASLEnabled_Invalid(t *testing.T) {
	k := NewKafkaClient()
	k.conf = &conf.Kafka{
		Brokers: []string{"localhost:9092"},
		Sasl:    &conf.SASL{Enabled: true, Mechanism: "INVALID"},
	}
	err := k.validateConfiguration()
	if !errors.Is(err, ErrInvalidSASLMechanism) {
		t.Fatalf("expected ErrInvalidSASLMechanism, got %v", err)
	}
}

func TestValidateConfiguration_SASLEnabled_MissingUsername(t *testing.T) {
	k := NewKafkaClient()
	k.conf = &conf.Kafka{
		Brokers: []string{"localhost:9092"},
		Sasl:    &conf.SASL{Enabled: true, Mechanism: SASLPlain, Username: "", Password: "pass"},
	}
	err := k.validateConfiguration()
	if err == nil {
		t.Fatal("expected error for missing SASL username, got nil")
	}
}

func TestValidateConfiguration_SASLEnabled_MissingPassword(t *testing.T) {
	k := NewKafkaClient()
	k.conf = &conf.Kafka{
		Brokers: []string{"localhost:9092"},
		Sasl:    &conf.SASL{Enabled: true, Mechanism: SASLPlain, Username: "user", Password: ""},
	}
	err := k.validateConfiguration()
	if err == nil {
		t.Fatal("expected error for missing SASL password, got nil")
	}
}

func TestValidateConfiguration_TLSEnabled_MismatchedCertKey(t *testing.T) {
	k := NewKafkaClient()
	k.conf = &conf.Kafka{
		Brokers: []string{"localhost:9092"},
		Tls:     &conf.TLS{Enabled: true, CertFile: "cert.pem", KeyFile: ""},
	}
	err := k.validateConfiguration()
	if err == nil {
		t.Fatal("expected error for mismatched TLS cert/key, got nil")
	}
}

func TestValidateConfiguration_DisabledSASL_NoValidation(t *testing.T) {
	k := NewKafkaClient()
	k.conf = &conf.Kafka{
		Brokers: []string{"localhost:9092"},
		Sasl:    &conf.SASL{Enabled: false},
	}
	if err := k.validateConfiguration(); err != nil {
		t.Fatalf("unexpected error when SASL disabled: %v", err)
	}
}

func TestValidateConfiguration_DisabledTLS_NoValidation(t *testing.T) {
	k := NewKafkaClient()
	k.conf = &conf.Kafka{
		Brokers: []string{"localhost:9092"},
		Tls:     &conf.TLS{Enabled: false},
	}
	if err := k.validateConfiguration(); err != nil {
		t.Fatalf("unexpected error when TLS disabled: %v", err)
	}
}

// ---------------------------------------------------------------------------
// validateProducerConfig
// ---------------------------------------------------------------------------

func TestValidateProducerConfig_AllCompressionTypes(t *testing.T) {
	k := NewKafkaClient()
	for _, comp := range []string{CompressionNone, CompressionGzip, CompressionSnappy, CompressionLz4, CompressionZstd} {
		p := &conf.Producer{Compression: comp, RequiredAcks: 1}
		if err := k.validateProducerConfig(p); err != nil {
			t.Errorf("unexpected error for compression %q: %v", comp, err)
		}
	}
}

func TestValidateProducerConfig_InvalidAcks(t *testing.T) {
	k := NewKafkaClient()
	for _, acks := range []int32{2, -2, 99} {
		p := &conf.Producer{RequiredAcks: acks}
		if err := k.validateProducerConfig(p); err == nil {
			t.Errorf("expected error for invalid acks=%d, got nil", acks)
		}
	}
}

func TestValidateProducerConfig_ValidAcks(t *testing.T) {
	k := NewKafkaClient()
	for _, acks := range []int32{-1, 0, 1} {
		p := &conf.Producer{RequiredAcks: acks}
		if err := k.validateProducerConfig(p); err != nil {
			t.Errorf("unexpected error for acks=%d: %v", acks, err)
		}
	}
}

// ---------------------------------------------------------------------------
// setDefaultValues
// ---------------------------------------------------------------------------

func TestSetDefaultValues_NilConf(t *testing.T) {
	k := NewKafkaClient()
	k.conf = nil
	k.setDefaultValues() // must not panic
	if k.conf == nil {
		t.Fatal("conf should be initialized after setDefaultValues with nil conf")
	}
}

func TestSetDefaultValues_PopulatesProducerDefaults(t *testing.T) {
	k := NewKafkaClient()
	k.conf = &conf.Kafka{
		Brokers: []string{"localhost:9092"},
		Producers: []*conf.Producer{
			{Enabled: true, Name: "p1"},
		},
	}
	k.setDefaultValues()
	p := k.conf.Producers[0]
	if p.MaxRetries == 0 {
		t.Error("expected MaxRetries to be populated")
	}
	if p.BatchSize == 0 {
		t.Error("expected BatchSize to be populated")
	}
	if p.Compression == "" {
		t.Error("expected Compression to be populated")
	}
}

func TestSetDefaultValues_PopulatesConsumerDefaults(t *testing.T) {
	k := NewKafkaClient()
	k.conf = &conf.Kafka{
		Brokers: []string{"localhost:9092"},
		Consumers: []*conf.Consumer{
			{Enabled: true, GroupId: "grp"},
		},
	}
	k.setDefaultValues()
	c := k.conf.Consumers[0]
	if c.MaxConcurrency == 0 {
		t.Error("expected MaxConcurrency to be populated")
	}
	if c.StartOffset == "" {
		t.Error("expected StartOffset to be populated")
	}
}

// ---------------------------------------------------------------------------
// validateSASLConfig edge cases
// ---------------------------------------------------------------------------

func TestValidateSASLConfig_NilSASL(t *testing.T) {
	k := NewKafkaClient()
	k.conf = &conf.Kafka{Sasl: nil}
	err := k.validateSASLConfig()
	if err == nil {
		t.Fatal("expected error for nil SASL config, got nil")
	}
}

func TestValidateSASLConfig_ValidScram256(t *testing.T) {
	k := NewKafkaClient()
	k.conf = &conf.Kafka{
		Sasl: &conf.SASL{
			Enabled:   true,
			Mechanism: SASLScramSHA256,
			Username:  "user",
			Password:  "pass",
		},
	}
	if err := k.validateSASLConfig(); err != nil {
		t.Fatalf("unexpected error for valid SCRAM-SHA-256: %v", err)
	}
}

func TestValidateSASLConfig_ValidScram512(t *testing.T) {
	k := NewKafkaClient()
	k.conf = &conf.Kafka{
		Sasl: &conf.SASL{
			Enabled:   true,
			Mechanism: SASLScramSHA512,
			Username:  "user",
			Password:  "pass",
		},
	}
	if err := k.validateSASLConfig(); err != nil {
		t.Fatalf("unexpected error for valid SCRAM-SHA-512: %v", err)
	}
}

// ---------------------------------------------------------------------------
// validateTLSConfig
// ---------------------------------------------------------------------------

func TestValidateTLSConfig_NilTLS(t *testing.T) {
	k := NewKafkaClient()
	k.conf = &conf.Kafka{Tls: nil}
	err := k.validateTLSConfig()
	if err == nil {
		t.Fatal("expected error for nil TLS config, got nil")
	}
}

func TestValidateTLSConfig_DisabledTLS_OK(t *testing.T) {
	k := NewKafkaClient()
	k.conf = &conf.Kafka{Tls: &conf.TLS{Enabled: false}}
	if err := k.validateTLSConfig(); err != nil {
		t.Fatalf("unexpected error for disabled TLS: %v", err)
	}
}

func TestValidateTLSConfig_BothCertAndKey_OK(t *testing.T) {
	k := NewKafkaClient()
	k.conf = &conf.Kafka{
		Tls: &conf.TLS{Enabled: true, CertFile: "c.pem", KeyFile: "k.pem"},
	}
	if err := k.validateTLSConfig(); err != nil {
		t.Fatalf("unexpected error for cert+key set: %v", err)
	}
}

func TestValidateTLSConfig_NeitherCertNorKey_OK(t *testing.T) {
	k := NewKafkaClient()
	k.conf = &conf.Kafka{
		Tls: &conf.TLS{Enabled: true, CertFile: "", KeyFile: ""},
	}
	if err := k.validateTLSConfig(); err != nil {
		t.Fatalf("unexpected error for server-only TLS: %v", err)
	}
}

func TestValidateTLSConfig_OnlyKeySet_Error(t *testing.T) {
	k := NewKafkaClient()
	k.conf = &conf.Kafka{
		Tls: &conf.TLS{Enabled: true, CertFile: "", KeyFile: "k.pem"},
	}
	if err := k.validateTLSConfig(); err == nil {
		t.Fatal("expected error when only key_file set, got nil")
	}
}
