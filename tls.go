package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"github.com/go-lynx/lynx-kafka/conf"
	"github.com/go-lynx/lynx/pkg/security"
)

// buildTLSConfig builds tls.Config based on configuration
func buildTLSConfig(t *conf.TLS) (*tls.Config, error) {
	if t == nil || !t.Enabled {
		return nil, nil
	}
	if err := security.ValidateTLSProductionPolicy("kafka", true, t.InsecureSkipVerify); err != nil {
		return nil, err
	}

	cfg := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: t.InsecureSkipVerify,
	}
	if t.ServerName != "" {
		cfg.ServerName = t.ServerName
	}

	// Optional CA file: appended to the system root pool so that CA certificates
	// downloaded from managed-cluster consoles (e.g. Aiven) that are not in the
	// system trust store are accepted.
	if t.CaFile != "" {
		caPEM, err := os.ReadFile(t.CaFile)
		if err != nil {
			return nil, fmt.Errorf("read ca_file failed: %w", err)
		}
		pool, err := x509.SystemCertPool()
		if err != nil || pool == nil {
			pool = x509.NewCertPool()
		}
		if ok := pool.AppendCertsFromPEM(caPEM); !ok {
			return nil, fmt.Errorf("append ca cert failed")
		}
		cfg.RootCAs = pool
	}

	// Client certificate for mTLS: managed Kafka providers such as Aiven include a
	// CertificateRequest in the TLS handshake.  Without this the first Kafka request
	// receives a TLS "bad certificate" alert.
	if t.CertFile != "" && t.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(t.CertFile, t.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("load client cert/key failed: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}

	return cfg, nil
}
