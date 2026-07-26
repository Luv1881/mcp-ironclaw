#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PKI="${PKI_DIR:-$ROOT/deploy/pki/out}"
NETWORK="${NETWORK:-ironclaw_default}"
PORT="${PORT:-9444}"
INGEST_PORT="${INGEST_PORT:-9445}"
DOCKER="${DOCKER:-docker}"
INGEST_IMAGE="${INGEST_IMAGE:-ironclaw/ingest:dev}"
EDGE_IMAGE="${EDGE_IMAGE:-haproxy:3.0-alpine}"
TOPIC="${TOPIC:-ironclaw.edgecheck}"
DEVICE="${DEVICE:-device-005}"

failures=0

cleanup() {
    $DOCKER rm -f ironclaw-edgecheck-ingest ironclaw-edgecheck-edge >/dev/null 2>&1 || true
}
trap cleanup EXIT

if [ ! -f "$PKI/edge.pem" ]; then
    echo "missing PKI at $PKI; run: make pki" >&2
    exit 1
fi

cleanup

$DOCKER run -d --name ironclaw-edgecheck-ingest --network "$NETWORK" --user "$(id -u):$(id -g)" \
    --network-alias ingest-a.ironclaw.internal \
    --network-alias ingest.ironclaw.internal \
    -p "$INGEST_PORT:8443" \
    -v "$PKI:/etc/ironclaw/tls:ro" \
    "$INGEST_IMAGE" \
    -addr :8443 -cert /etc/ironclaw/tls/ingest.crt -key /etc/ironclaw/tls/ingest.key \
    -client-ca /etc/ironclaw/tls/ca.crt -kafka kafka:9092 -topic "$TOPIC" -redis redis:6379 >/dev/null

$DOCKER run -d --name ironclaw-edgecheck-edge --user root --network "$NETWORK" -p "$PORT:443" \
    -v "$ROOT/deploy/haproxy.cfg:/usr/local/etc/haproxy/haproxy.cfg:ro" \
    -v "$PKI:/etc/ironclaw/tls:ro" \
    "$EDGE_IMAGE" >/dev/null

post() {
    local device="$1" body="$2"
    shift 2
    curl -sS -o /dev/null -w '%{http_code}' \
        --cert "$PKI/$device.crt" --key "$PKI/$device.key" --cacert "$PKI/ca.crt" \
        --resolve "edge.ironclaw.internal:$PORT:127.0.0.1" \
        -X POST -H 'Content-Type: application/json' "$@" \
        -d "$body" "https://edge.ironclaw.internal:$PORT/v1/batches" 2>/dev/null || echo 000
}

event() {
    printf '{"user_id":"edge-user","process_id":%s,"pod_id":"pod-edge","kind":1,"observed_at_unix_nanos":1785000000000000000,"latency_nanos":1500000,"bytes":256,"failed":false}' "$1"
}

ready=0
for _ in $(seq 1 80); do
    if [ "$(post "$DEVICE" "{\"events\":[$(event 1)]}")" = "202" ]; then
        ready=1
        break
    fi
    sleep 0.5
done

if [ "$ready" -ne 1 ]; then
    echo "edge path never accepted a batch; refusing to report results" >&2
    $DOCKER logs ironclaw-edgecheck-edge 2>&1 | tail -10 >&2
    $DOCKER logs ironclaw-edgecheck-ingest 2>&1 | tail -10 >&2
    exit 1
fi

assert() {
    local label="$1" want="$2" got="$3"
    if [ "$want" = "$got" ]; then
        printf 'PASS  %-56s %s\n' "$label" "$got"
    else
        printf 'FAIL  %-56s got %s, want %s\n' "$label" "$got" "$want"
        failures=$((failures + 1))
    fi
}

assert "valid device certificate is accepted" 202 \
    "$(post "$DEVICE" "{\"events\":[$(event 2)]}")"

assert "body device_id claiming another device is refused" 403 \
    "$(post "$DEVICE" "{\"device_id\":\"device-000\",\"events\":[$(event 3)]}")"

assert "client supplied X-Device-Id does not survive the edge" 202 \
    "$(post "$DEVICE" "{\"device_id\":\"$DEVICE\",\"events\":[$(event 4)]}" -H 'X-Device-Id: device-000')"

anonymous=$(curl -sS -o /dev/null -w '%{http_code}' --cacert "$PKI/ca.crt" \
    --resolve "edge.ironclaw.internal:$PORT:127.0.0.1" -X POST -d '{}' \
    "https://edge.ironclaw.internal:$PORT/v1/batches" 2>/dev/null) || true

assert "connection without a client certificate is refused" 000 "${anonymous:-000}"

bypass=$(curl -sS -o /dev/null -w '%{http_code}' \
    --cert "$PKI/device-000.crt" --key "$PKI/device-000.key" --cacert "$PKI/ca.crt" \
    --resolve "ingest.ironclaw.internal:$INGEST_PORT:127.0.0.1" \
    -X POST -H 'Content-Type: application/json' -H "X-Device-Id: device-017" \
    -d "{\"events\":[$(event 9)]}" \
    "https://ingest.ironclaw.internal:$INGEST_PORT/v1/batches" 2>/dev/null) || true

assert "device certificate cannot bypass the edge and impersonate" 000 "${bypass:-000}"

observed=$($DOCKER exec ironclaw-kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 --topic "$TOPIC" --from-beginning --timeout-ms 8000 2>/dev/null \
    | grep -o '"device_id":"[^"]*"' | sed 's/.*:"//; s/"$//' | sort -u | tr '\n' ' ' || true)

assert "kafka only ever saw the certificate identity" "$DEVICE" "$(echo "$observed" | xargs)"

if [ "$failures" -ne 0 ]; then
    echo "$failures edge path expectation(s) not met" >&2
    exit 1
fi

echo "all edge path expectations met"
