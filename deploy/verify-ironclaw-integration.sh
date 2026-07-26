#!/usr/bin/env bash
set -euo pipefail

# Proves the MCP server satisfies the contract that deploy/ironclaw/manifest.toml
# declares to the IronClaw agent host: HTTPS at /mcp, a bearer token injected as
# an Authorization header, a tool catalogue within the declared max_tools, and
# per-tenant scoping.
#
# Two rules this script follows, both learned the hard way in this project:
#   1. Every rejection assertion gates on a completed TLS handshake first, so
#      "nothing was listening" can never be scored as "policy refused me".
#   2. Assertions read the JSON-RPC result, never the HTTP status alone. An MCP
#      error is delivered inside a 200 response, so status-only checks pass
#      while the call is in fact failing.

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PKI="$ROOT/deploy/pki/out"
MANIFEST="$ROOT/deploy/ironclaw/manifest.toml"
WORK="$(mktemp -d)"
PORT="${IRONCLAW_MCP_PORT:-18443}"
HOST="localhost"

ADMIN_TOKEN="admin-token"
ADMIN_USER="fleet-admin"
TENANT_TOKEN="tenant-token"
TENANT_USER="user-000"
BAD_TOKEN="not-the-token"

pass=0
fail=0
server_pid=""
SESSION=""

cleanup() {
  [ -n "$server_pid" ] && kill "$server_pid" 2>/dev/null || true
  rm -rf "$WORK"
}
trap cleanup EXIT

ok()   { printf '  \033[32mPASS\033[0m %s\n' "$1"; pass=$((pass + 1)); }
bad()  { printf '  \033[31mFAIL\033[0m %s\n' "$1"; fail=$((fail + 1)); }
note() { printf '\n\033[1m%s\033[0m\n' "$1"; }

for tool in curl python3; do
  command -v "$tool" >/dev/null 2>&1 || { echo "missing required tool: $tool" >&2; exit 2; }
done

[ -f "$PKI/edge.crt" ] || { echo "no PKI material at $PKI — run 'make pki' first" >&2; exit 2; }
[ -x "$ROOT/bin/mcp-server" ] || { echo "build it first: go build -o bin/mcp-server ./mcp-server" >&2; exit 2; }

manifest_value() {
  python3 - "$MANIFEST" "$1" <<'PY'
import re, sys
key = sys.argv[2]
for line in open(sys.argv[1]):
    match = re.match(r'\s*%s\s*=\s*"?([^"\n]+)"?\s*$' % re.escape(key), line)
    if match:
        print(match.group(1).strip())
        break
PY
}

MAX_TOOLS="$(manifest_value max_tools)"
SERVER_URL="$(manifest_value server)"
MCP_PATH="/${SERVER_URL#*://*/}"

note "Manifest contract (deploy/ironclaw/manifest.toml)"
echo "  server    : $SERVER_URL"
echo "  mcp path  : $MCP_PATH"
echo "  max_tools : $MAX_TOOLS"

"$ROOT/bin/mcp-server" \
  -http-addr ":$PORT" \
  -tls-cert "$PKI/edge.crt" \
  -tls-key "$PKI/edge.key" \
  -require-auth \
  -tokens "$ADMIN_TOKEN:$ADMIN_USER:ironclaw:admin,$TENANT_TOKEN:$TENANT_USER" \
  -devices 2 -events 100000 -window 2s -emit-open-windows \
  >"$WORK/server.log" 2>&1 &
server_pid=$!

for _ in $(seq 1 60); do
  curl -sk --max-time 2 "https://$HOST:$PORT/healthz" >/dev/null 2>&1 && break
  sleep 0.2
done

# JSON-RPC over the streamable HTTP transport. Emits the HTTP status on stdout,
# leaves the body in $WORK/body and the response headers in $WORK/headers.
mcp() {
  local token="$1" body="$2"
  local args=(-sk -o "$WORK/body" -D "$WORK/headers" -w '%{http_code}' --max-time 20
    -X POST "https://$HOST:$PORT$MCP_PATH"
    -H 'Content-Type: application/json'
    -H 'Accept: application/json, text/event-stream'
    --data "$body")
  [ -n "$token" ] && args+=(-H "Authorization: Bearer $token")
  [ -n "$SESSION" ] && args+=(-H "Mcp-Session-Id: $SESSION")
  curl "${args[@]}"
}

# Extracts a field from the JSON-RPC response, whether it arrives as a bare
# JSON body or as an SSE data: frame.
rpc() {
  python3 - "$WORK/body" "$1" <<'PY'
import json, sys

selector = sys.argv[2]

for raw in open(sys.argv[1]):
    raw = raw.strip()
    if raw.startswith("data:"):
        raw = raw[5:].strip()
    if not raw.startswith("{"):
        continue
    try:
        message = json.loads(raw)
    except Exception:
        continue

    if selector == "tools":
        tools = message.get("result", {}).get("tools")
        if tools is not None:
            print(" ".join(sorted(tool["name"] for tool in tools)))
            sys.exit(0)
    elif selector == "ok":
        result = message.get("result")
        if result is not None and not result.get("isError"):
            sys.exit(0)
    elif selector == "toolerror":
        result = message.get("result")
        if result is not None and result.get("isError"):
            text = " ".join(
                part.get("text", "") for part in result.get("content", []) or []
            )
            print(text)
            sys.exit(0)
    elif selector == "rpcerror":
        error = message.get("error")
        if error is not None:
            print(error.get("message", ""))
            sys.exit(0)

sys.exit(1)
PY
}

open_session() {
  local token="$1"
  SESSION=""
  mcp "$token" "$INIT" >/dev/null
  SESSION="$(tr -d '\r' < "$WORK/headers" | awk 'tolower($1) == "mcp-session-id:" { print $2 }')"
  mcp "$token" '{"jsonrpc":"2.0","method":"notifications/initialized"}' >/dev/null
}

INIT='{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"ironclaw-host","version":"1"}}}'

note "Transport"

if curl -sk --max-time 5 "https://$HOST:$PORT/healthz" | grep -q ok; then
  ok "TLS handshake completes and the endpoint answers"
else
  bad "the endpoint did not answer over TLS — every assertion below would be meaningless"
  cat "$WORK/server.log" >&2
  exit 1
fi

if curl -s --max-time 5 "http://$HOST:$PORT/healthz" 2>/dev/null | grep -q '^ok$'; then
  bad "the server served a plaintext health response; the manifest declares an https endpoint"
else
  ok "plaintext HTTP does not yield a usable response, matching the https-only manifest"
fi

note "Bearer credential injection (Authorization: Bearer, as the manifest declares)"

code="$(mcp "" "$INIT")"
if [ "$code" = "401" ]; then
  ok "no Authorization header is rejected with 401"
else
  bad "no Authorization header returned $code, want 401"
fi

code="$(mcp "$BAD_TOKEN" "$INIT")"
if [ "$code" = "401" ]; then
  ok "an unknown bearer token is rejected with 401"
else
  bad "an unknown token returned $code, want 401"
fi

code="$(mcp "$ADMIN_TOKEN" "$INIT")"
if [ "$code" = "200" ]; then
  ok "the injected bearer token is accepted with 200"
else
  bad "the valid token returned $code, want 200"
  head -c 300 "$WORK/body" >&2
fi

SESSION="$(tr -d '\r' < "$WORK/headers" | awk 'tolower($1) == "mcp-session-id:" { print $2 }')"
if [ -n "$SESSION" ]; then
  ok "the server issued an MCP session id"
else
  bad "no Mcp-Session-Id header — subsequent calls cannot be bound to a session"
fi

mcp "$ADMIN_TOKEN" '{"jsonrpc":"2.0","method":"notifications/initialized"}' >/dev/null

note "Tool catalogue"

code="$(mcp "$ADMIN_TOKEN" '{"jsonrpc":"2.0","id":2,"method":"tools/list"}')"
tools="$(rpc tools || true)"
count="$(echo "$tools" | wc -w)"

if [ "$count" -gt 0 ]; then
  ok "tools/list returned $count tools"
else
  bad "tools/list returned no tools (http $code)"
  head -c 300 "$WORK/body" >&2
fi

if [ "$count" -le "$MAX_TOOLS" ] && [ "$count" -gt 0 ]; then
  ok "tool count $count is within the manifest's max_tools=$MAX_TOOLS"
else
  bad "tool count $count against the manifest's max_tools=$MAX_TOOLS"
fi

for expected in get_device_state get_user_devices get_pipeline_metrics watch_device reset_device_counters; do
  if echo "$tools" | grep -qw "$expected"; then
    ok "tool present: $expected"
  else
    bad "tool missing from the catalogue: $expected"
  fi
done

note "Tool invocation over the authenticated HTTPS path"

mcp "$ADMIN_TOKEN" '{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"get_pipeline_metrics","arguments":{}}}' >/dev/null
if rpc ok >/dev/null; then
  ok "get_pipeline_metrics returned a result"
else
  bad "get_pipeline_metrics did not return a usable result"
  head -c 300 "$WORK/body" >&2
fi

note "Session binding"

# The session opened above belongs to the admin principal. Presenting a
# different token on it must be refused, or a session id would be a way to
# borrow another principal's authorisation.
code="$(mcp "$TENANT_TOKEN" '{"jsonrpc":"2.0","id":90,"method":"tools/list"}')"
borrowed="$(head -c 120 "$WORK/body" | tr -d '\n')"
if [ "$code" = "403" ] || [ "$code" = "401" ]; then
  ok "a second principal cannot reuse an existing session (http $code: ${borrowed})"
else
  bad "a different token was accepted on someone else's session (http $code)"
fi

note "Tenant isolation"

# A non-admin principal reading its own tenant must succeed, and reading
# another tenant must be refused. Both are read through the JSON-RPC result,
# because an authorisation refusal still arrives inside a 200 response.
# Each principal gets its own session, since the server correctly binds one.

open_session "$TENANT_TOKEN"

mcp "$TENANT_TOKEN" "{\"jsonrpc\":\"2.0\",\"id\":4,\"method\":\"tools/call\",\"params\":{\"name\":\"get_user_devices\",\"arguments\":{\"user_id\":\"$TENANT_USER\"}}}" >/dev/null
if rpc ok >/dev/null; then
  ok "a tenant token may read its own devices"
else
  bad "a tenant token was refused its own devices"
  head -c 300 "$WORK/body" >&2
fi

mcp "$TENANT_TOKEN" '{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"get_user_devices","arguments":{"user_id":"someone-else"}}}' >/dev/null
refusal="$(rpc toolerror || true)"
if [ -n "$refusal" ]; then
  ok "a tenant token is refused another tenant's devices: ${refusal}"
else
  bad "a tenant token was NOT refused another tenant's devices — cross-tenant read is possible"
  head -c 300 "$WORK/body" >&2
fi

mcp "$TENANT_TOKEN" '{"jsonrpc":"2.0","id":6,"method":"tools/call","params":{"name":"get_pipeline_metrics","arguments":{}}}' >/dev/null
refusal="$(rpc toolerror || true)"
if [ -n "$refusal" ]; then
  ok "fleet metrics are refused without ironclaw:admin: ${refusal}"
else
  bad "a non-admin token read fleet-wide pipeline metrics"
fi

note "Result"
echo "  passed: $pass"
echo "  failed: $fail"
[ "$fail" -eq 0 ] || exit 1
