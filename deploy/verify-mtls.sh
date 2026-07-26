#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PKI="${PKI_DIR:-$ROOT/deploy/pki/out}"
ROGUE="$PKI/rogue"
CONTAINER="${CONTAINER:-ironclaw-edge-verify}"
PORT="${PORT:-8443}"
IMAGE="${IMAGE:-haproxy:3.0-alpine}"
DOCKER="${DOCKER:-docker}"

failures=0

cleanup() {
    $DOCKER rm -f "$CONTAINER" >/dev/null 2>&1 || true
}
trap cleanup EXIT

if [ ! -f "$PKI/edge.pem" ]; then
    echo "missing PKI at $PKI; run: make pki" >&2
    exit 1
fi

mkdir -p "$ROGUE"
if [ ! -f "$ROGUE/rogue-device.crt" ]; then
    openssl genrsa -out "$ROGUE/rogue-ca.key" 2048 2>/dev/null
    openssl req -x509 -new -nodes -key "$ROGUE/rogue-ca.key" -sha256 -days 30 \
        -subj "/O=Rogue/CN=Rogue CA" -out "$ROGUE/rogue-ca.crt" 2>/dev/null
    openssl genrsa -out "$ROGUE/rogue-device.key" 2048 2>/dev/null
    openssl req -new -key "$ROGUE/rogue-device.key" -subj "/O=Rogue/CN=device-000" \
        -out "$ROGUE/rogue-device.csr" 2>/dev/null
    openssl x509 -req -in "$ROGUE/rogue-device.csr" -CA "$ROGUE/rogue-ca.crt" -CAkey "$ROGUE/rogue-ca.key" \
        -CAcreateserial -out "$ROGUE/rogue-device.crt" -days 30 -sha256 2>/dev/null
    rm -f "$ROGUE/rogue-device.csr"
fi

cleanup
$DOCKER run -d --name "$CONTAINER" --user root -p "$PORT:443" \
    -v "$ROOT/deploy/haproxy.cfg:/usr/local/etc/haproxy/haproxy.cfg:ro" \
    -v "$PKI:/etc/ironclaw/tls:ro" \
    "$IMAGE" >/dev/null

handshake() {
    printf 'GET /healthz HTTP/1.1\r\nHost: edge.ironclaw.internal\r\nConnection: close\r\n\r\n' \
        | openssl s_client -connect "127.0.0.1:$PORT" -servername edge.ironclaw.internal \
            -CAfile "$PKI/ca.crt" -quiet "$@" 2>&1 || true
}

ready=0
for _ in $(seq 1 80); do
    if grep -qa "HTTP/1" <<<"$(handshake -cert "$PKI/device-000.crt" -key "$PKI/device-000.key")"; then
        ready=1
        break
    fi
    sleep 0.25
done

if [ "$ready" -ne 1 ]; then
    echo "edge never completed a TLS handshake on port $PORT; an unreachable listener would look like a policy rejection, so refusing to report results" >&2
    $DOCKER logs "$CONTAINER" 2>&1 | tail -20 >&2
    exit 1
fi

assert() {
    local label="$1" expectation="$2" outcome="$3"
    if [ "$expectation" = "$outcome" ]; then
        printf 'PASS  %-46s %s\n' "$label" "$outcome"
    else
        printf 'FAIL  %-46s got %s, want %s\n' "$label" "$outcome" "$expectation"
        failures=$((failures + 1))
    fi
}

classify() {
    if grep -qai "connection refused\|connect error" <<<"$1"; then
        echo unreachable
    elif grep -qa "HTTP/1" <<<"$1"; then
        echo accepted
    elif grep -qai "certificate required\|unknown ca\|bad certificate\|alert" <<<"$1"; then
        echo rejected
    else
        echo inconclusive
    fi
}

valid=$(handshake -cert "$PKI/device-000.crt" -key "$PKI/device-000.key")
assert "valid device certificate" accepted "$(classify "$valid")"

none=$(handshake)
assert "no client certificate" rejected "$(classify "$none")"

rogue=$(handshake -cert "$ROGUE/rogue-device.crt" -key "$ROGUE/rogue-device.key")
assert "client certificate from an untrusted CA" rejected "$(classify "$rogue")"

cn=$(openssl x509 -in "$PKI/device-000.crt" -noout -subject | sed 's/.*CN *= *//')
assert "device identity carried in the certificate CN" "device-000" "$cn"

if [ "$failures" -ne 0 ]; then
    echo "$failures mTLS expectation(s) not met" >&2
    exit 1
fi

echo "all mTLS expectations met"
