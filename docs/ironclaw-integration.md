# Registering with the IronClaw Agent OS

[IronClaw](https://github.com/nearai/ironclaw) is a Rust agent runtime that connects to MCP servers for additional capabilities. This pipeline's MCP server is one of those capabilities: IronClaw is the *host*, this repository is the *extension*.

MCP is a wire protocol, so nothing here needs to be written in Rust. The integration surface is one manifest file plus a reachable HTTPS endpoint.

## The manifest

`deploy/ironclaw/manifest.toml` is an extension manifest in IronClaw's `reborn.extension_manifest.v3` schema. The parts that matter:

```toml
[mcp]
server = "https://ironclaw-telemetry.internal/mcp"
namespace = "ironclaw-telemetry"
max_tools = 16
default_permission = "ask"
effects = ["network", "use_secret"]

[[mcp.credentials]]
handle = "ironclaw_telemetry_token"
injection = { type = "header", name = "authorization", prefix = "Bearer " }
```

Constraints the host enforces, all confirmed against its parser rather than assumed:

| Rule | Consequence if broken |
| --- | --- |
| `server` must be `https://` | manifest rejected outright — plaintext is not a downgrade path, it is a parse failure |
| `namespace` must equal the top-level `id` | manifest rejected |
| `max_tools` must be non-zero | manifest rejected |
| unknown fields are refused | a typo fails closed rather than being ignored |

`default_permission = "ask"` means the user is prompted before a tool runs. `origin_gate_matrix` forbids invocation from the `product` and `automation` origins entirely — telemetry is readable in an interactive loop, not from unattended automation.

Change `server` to wherever the MCP server actually listens for your deployment. Everything else can stay as shipped.

## Running the server for the host

The endpoint the manifest points at must speak HTTPS and accept the bearer token IronClaw injects:

```bash
go build -o bin/mcp-server ./mcp-server

./bin/mcp-server \
  -http-addr :8443 \
  -tls-cert deploy/pki/out/edge.crt \
  -tls-key  deploy/pki/out/edge.key \
  -require-auth \
  -tokens 'THE-TOKEN:fleet-admin:ironclaw:admin' \
  -redis localhost:16379 -kafka localhost:19092 -emit-open-windows
```

The token registered in IronClaw under the `ironclaw_telemetry_token` handle must match one issued here. In a real deployment tokens come from an OIDC issuer rather than the `-tokens` flag, which exists for local runs and tests.

### Scopes

The token's scopes decide what the agent can see:

- a plain token (`token:user-000`) reads only `user-000`'s devices;
- a token holding `ironclaw:admin` reads any tenant and the fleet-wide pipeline metrics.

Give the agent the narrowest token that does its job. An agent asking about one user's devices does not need `ironclaw:admin`.

## Verifying the integration

```bash
make ironclaw-verify
```

This starts the server as the manifest describes it and asserts 18 properties, including:

- a TLS handshake completes before any rejection is scored, so "nothing was listening" can never be mistaken for "policy refused me";
- no token and an unknown token are both refused with 401, a valid one gets 200;
- the tool catalogue is non-empty and within the manifest's declared `max_tools` — this is the assertion that catches the manifest and the server drifting apart;
- every named tool is present;
- a tenant token reads its own devices, is refused another tenant's, and is refused fleet metrics;
- a session opened by one principal cannot be reused by another.

Assertions read the JSON-RPC result, never the HTTP status alone. An MCP tool error arrives inside a `200` response, so a status-only check reports success while the call is in fact failing — which is exactly how the first version of this script produced false passes.

## What has been verified, and what has not

**Verified:** the manifest parses under IronClaw's own `ExtensionManifestRecord::from_toml` — its real v3 parser, compiled from source, not a reimplementation. Two negative controls confirm the test discriminates: downgrading `server` to `http://` is refused, and a `namespace` that disagrees with `id` is refused. The server side is verified by `make ironclaw-verify` as described above.

**Not verified:** no running IronClaw instance has loaded this extension. Installing the agent runtime and completing a live tool call from it is the remaining step. Everything on both sides of the boundary is proven; the handshake between them is proven only against the host's parser and the protocol contract.

## What this integration did not require

No Rust in this repository, no changes to the pipeline, and no changes to the MCP tools. The one code change it did prompt was a genuine bug fix: a refused tool call returned a zero-valued output whose nil map failed the SDK's output-schema check, so the refusal reached the client as a schema violation rather than a reason. That is fixed and covered by a regression test that fails without the fix.
