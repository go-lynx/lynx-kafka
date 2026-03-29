package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"github.com/go-lynx/lynx-kafka/conf"
)

// buildTLSConfig builds tls.Config based on configuration
func buildTLSConfig(t *conf.TLS) (*tls.Config, error) {
	if t == nil || !t.Enabled {
		return nil, nil
	}

	cfg := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: t.InsecureSkipVerify,
	}
	if t.ServerName != "" {
		cfg.ServerName = t.ServerName
	}

	// 可选 CA 文件：追加到系统根证书池（Aiven 等控制台下载的 CA 往往不在系统信任库中）
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

	// 客户端证书（mTLS）：Aiven 等托管 Kafka 常在 CertificateRequest 中要求访问证书，否则首条 Kafka 请求会收到 TLS alert bad certificate
	if t.CertFile != "" && t.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(t.CertFile, t.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("load client cert/key failed: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}

	return cfg, nil
}
