package postgresstore

import (
	"errors"
	"fmt"
	"net/url"
)

var (
	ErrEmptyDSN         = errors.New("postgresstore: dsn is empty")
	ErrPlaintextRefused = errors.New("postgresstore: sslmode disables TLS but secure connections are required")
)

var permittedSSLModes = map[string]bool{
	"require":     true,
	"verify-ca":   true,
	"verify-full": true,
}

func RequireTLS(dsn string, rootCert string) (string, error) {
	if dsn == "" {
		return "", ErrEmptyDSN
	}

	parsed, err := url.Parse(dsn)
	if err != nil {
		return "", fmt.Errorf("postgresstore: parsing dsn: %w", err)
	}

	query := parsed.Query()

	mode := query.Get("sslmode")
	if mode == "" {
		mode = "verify-full"
	}
	if !permittedSSLModes[mode] {
		return "", fmt.Errorf("%w: sslmode=%s", ErrPlaintextRefused, mode)
	}

	query.Set("sslmode", mode)
	if rootCert != "" {
		query.Set("sslrootcert", rootCert)
	}

	parsed.RawQuery = query.Encode()

	return parsed.String(), nil
}
