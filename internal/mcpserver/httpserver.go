package mcpserver

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/auth"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

var (
	ErrNoVerifier   = errors.New("mcpserver: a token verifier is required when authentication is enabled")
	ErrUnknownToken = fmt.Errorf("%w: bearer token is not recognised", auth.ErrInvalidToken)
	ErrTokenExpired = fmt.Errorf("%w: bearer token has expired", auth.ErrInvalidToken)
)

type HTTPOptions struct {
	Addr            string
	Tools           *Tools
	Verifier        auth.TokenVerifier
	RequireAuth     bool
	CertFile        string
	KeyFile         string
	ClientCAFile    string
	ResourceMetaURL string
}

func NewHTTPServer(options HTTPOptions) (*http.Server, error) {
	if options.RequireAuth && options.Verifier == nil {
		return nil, ErrNoVerifier
	}

	handler := mcp.NewStreamableHTTPHandler(
		func(*http.Request) *mcp.Server { return NewServer(options.Tools) },
		nil,
	)

	wrapped := withPrincipal(handler)
	if options.RequireAuth {
		wrapped = auth.RequireBearerToken(options.Verifier, &auth.RequireBearerTokenOptions{
			ResourceMetadataURL: options.ResourceMetaURL,
		})(wrapped)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.Handle("/mcp", wrapped)

	server := &http.Server{
		Addr:              options.Addr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	if options.CertFile != "" {
		config, err := buildTLS(options)
		if err != nil {
			return nil, err
		}
		server.TLSConfig = config
	}

	return server, nil
}

func buildTLS(options HTTPOptions) (*tls.Config, error) {
	certificate, err := tls.LoadX509KeyPair(options.CertFile, options.KeyFile)
	if err != nil {
		return nil, err
	}

	config := &tls.Config{
		Certificates: []tls.Certificate{certificate},
		MinVersion:   tls.VersionTLS13,
	}

	if options.ClientCAFile != "" {
		pem, err := os.ReadFile(options.ClientCAFile)
		if err != nil {
			return nil, err
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, errors.New("mcpserver: client CA bundle contained no certificates")
		}
		config.ClientCAs = pool
		config.ClientAuth = tls.RequireAndVerifyClientCert
	}

	return config, nil
}

type StaticToken struct {
	UserID string
	Scopes []string
	Expiry time.Time
}

func StaticTokenVerifier(tokens map[string]StaticToken) auth.TokenVerifier {
	return func(_ context.Context, token string, _ *http.Request) (*auth.TokenInfo, error) {
		entry, ok := tokens[token]
		if !ok {
			return nil, ErrUnknownToken
		}
		if !entry.Expiry.IsZero() && time.Now().After(entry.Expiry) {
			return nil, ErrTokenExpired
		}

		expiration := entry.Expiry
		if expiration.IsZero() {
			expiration = time.Now().Add(time.Hour)
		}

		return &auth.TokenInfo{
			UserID:     entry.UserID,
			Scopes:     entry.Scopes,
			Expiration: expiration,
		}, nil
	}
}

func ParseStaticTokens(raw string) map[string]StaticToken {
	tokens := map[string]StaticToken{}

	for _, entry := range strings.Split(raw, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}

		parts := strings.SplitN(entry, ":", 3)
		if len(parts) < 2 {
			continue
		}

		token := StaticToken{UserID: parts[1]}
		if len(parts) > 2 && parts[2] != "" {
			token.Scopes = strings.Split(parts[2], "|")
		}
		tokens[parts[0]] = token
	}

	return tokens
}

func withPrincipal(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if info := auth.TokenInfoFromContext(r.Context()); info != nil {
			principal := Principal{UserID: info.UserID, Scopes: info.Scopes}
			r = r.WithContext(ContextWithPrincipal(r.Context(), principal))
		}
		next.ServeHTTP(w, r)
	})
}
