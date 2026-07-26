package kafkabus

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

var (
	ErrIncompleteKeyPair    = errors.New("kafkabus: both a client certificate and key are required for mTLS")
	ErrIncompleteSASL       = errors.New("kafkabus: both a SASL username and password are required")
	ErrEmptyCABundle        = errors.New("kafkabus: CA bundle contained no certificates")
	ErrInsecureWithoutTLS   = errors.New("kafkabus: SASL credentials require TLS so they are not sent in the clear")
	ErrPlaintextNotPermitte = errors.New("kafkabus: plaintext brokers are not permitted when security is required")
)

type Security struct {
	Enabled            bool
	CAFile             string
	CertFile           string
	KeyFile            string
	SASLUser           string
	SASLPassword       string
	InsecureSkipVerify bool
	ServerName         string
}

func (s Security) validate() error {
	if !s.Enabled {
		if s.SASLUser != "" || s.SASLPassword != "" {
			return ErrInsecureWithoutTLS
		}
		return nil
	}
	if (s.CertFile == "") != (s.KeyFile == "") {
		return ErrIncompleteKeyPair
	}
	if (s.SASLUser == "") != (s.SASLPassword == "") {
		return ErrIncompleteSASL
	}
	return nil
}

func (s Security) options() ([]kgo.Opt, error) {
	if err := s.validate(); err != nil {
		return nil, err
	}
	if !s.Enabled {
		return nil, nil
	}

	config := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: s.InsecureSkipVerify,
		ServerName:         s.ServerName,
	}

	if s.CAFile != "" {
		pem, err := os.ReadFile(s.CAFile)
		if err != nil {
			return nil, fmt.Errorf("kafkabus: reading CA bundle: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, ErrEmptyCABundle
		}
		config.RootCAs = pool
	}

	if s.CertFile != "" {
		certificate, err := tls.LoadX509KeyPair(s.CertFile, s.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("kafkabus: loading client certificate: %w", err)
		}
		config.Certificates = []tls.Certificate{certificate}
	}

	options := []kgo.Opt{kgo.DialTLSConfig(config)}

	if s.SASLUser != "" {
		options = append(options, kgo.SASL(scram.Auth{
			User: s.SASLUser,
			Pass: s.SASLPassword,
		}.AsSha512Mechanism()))
	}

	return options, nil
}
