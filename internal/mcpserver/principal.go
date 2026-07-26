package mcpserver

import (
	"context"
	"errors"
	"fmt"

	"github.com/modelcontextprotocol/go-sdk/auth"
)

var (
	ErrUnauthenticated = errors.New("mcpserver: request carries no authenticated principal")
	ErrForbidden       = errors.New("mcpserver: principal may not access this user's telemetry")
)

const ScopeAdmin = "ironclaw:admin"

type Principal struct {
	UserID string
	Scopes []string
}

func (p Principal) hasScope(name string) bool {
	for _, scope := range p.Scopes {
		if scope == name {
			return true
		}
	}
	return false
}

func (p Principal) mayAccess(userID string) bool {
	if p.hasScope(ScopeAdmin) {
		return true
	}
	return p.UserID != "" && p.UserID == userID
}

type principalKey struct{}

func ContextWithPrincipal(ctx context.Context, principal Principal) context.Context {
	return context.WithValue(ctx, principalKey{}, principal)
}

func PrincipalFrom(ctx context.Context) (Principal, bool) {
	if principal, ok := ctx.Value(principalKey{}).(Principal); ok {
		return principal, true
	}

	info := auth.TokenInfoFromContext(ctx)
	if info == nil {
		return Principal{}, false
	}

	return Principal{UserID: info.UserID, Scopes: info.Scopes}, true
}

func (t *Tools) authorise(ctx context.Context, userID string) error {
	principal, ok := PrincipalFrom(ctx)
	if !ok {
		if t.requireAuth {
			return ErrUnauthenticated
		}
		return nil
	}

	if !principal.mayAccess(userID) {
		return fmt.Errorf("%w: principal %q requested %q", ErrForbidden, principal.UserID, userID)
	}

	return nil
}
