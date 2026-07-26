package redisstore

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	ErrIncompleteKeyPair = errors.New("redisstore: both a client certificate and key are required for mTLS")
	ErrEmptyCABundle     = errors.New("redisstore: CA bundle contained no certificates")
	ErrPasswordInClear   = errors.New("redisstore: a password requires TLS so it is not sent in the clear")
)

type Security struct {
	Enabled            bool
	CAFile             string
	CertFile           string
	KeyFile            string
	Username           string
	Password           string
	InsecureSkipVerify bool
	ServerName         string
}

func (s Security) validate() error {
	if !s.Enabled && s.Password != "" {
		return ErrPasswordInClear
	}
	if (s.CertFile == "") != (s.KeyFile == "") {
		return ErrIncompleteKeyPair
	}
	return nil
}

func (s Security) apply(options *redis.Options) error {
	if err := s.validate(); err != nil {
		return err
	}

	options.Username = s.Username
	options.Password = s.Password

	if !s.Enabled {
		return nil
	}

	config := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: s.InsecureSkipVerify,
		ServerName:         s.ServerName,
	}

	if s.CAFile != "" {
		pem, err := os.ReadFile(s.CAFile)
		if err != nil {
			return fmt.Errorf("redisstore: reading CA bundle: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return ErrEmptyCABundle
		}
		config.RootCAs = pool
	}

	if s.CertFile != "" {
		certificate, err := tls.LoadX509KeyPair(s.CertFile, s.KeyFile)
		if err != nil {
			return fmt.Errorf("redisstore: loading client certificate: %w", err)
		}
		config.Certificates = []tls.Certificate{certificate}
	}

	options.TLSConfig = config

	return nil
}

func Dial(address string, security Security) (*redis.Client, error) {
	options := &redis.Options{
		Addr:         address,
		DialTimeout:  3 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		MaxRetries:   2,
	}

	if err := security.apply(options); err != nil {
		return nil, err
	}

	return redis.NewClient(options), nil
}
