# IronClaw

An event-driven device telemetry pipeline in Go, exposed to LLM clients through a [Model Context Protocol](https://modelcontextprotocol.io) server.

It is designed to be consumed by [IronClaw](https://github.com/nearai/ironclaw), the Rust agent OS, which connects to MCP servers for capabilities. MCP is a wire protocol, so the integration is one extension manifest and an HTTPS endpoint — see [`docs/ironclaw-integration.md`](docs/ironclaw-integration.md). Any other MCP client works equally well.

An eBPF agent captures kernel telemetry on-premise and ships it over mTLS through HAProxy to a cloud ingest tier. Events flow through Kafka into an aggregator that maintains per-process counters and DDSketch latency percentiles over tumbling windows. Two independent consumer groups fan the aggregates out — one to Redis as the hot read path, one to Postgres as the durable archive. The MCP server reads Redis and serves device state to an LLM client as tools.

The design target is **1,000,000 concurrent devices**; `docs/capacity.md` shows the arithmetic and where the current defaults fall short of it.

```
                    mTLS                mTLS
  ┌─────────┐   client cert    ┌─────┐  re-encrypt   ┌────────┐
  │  agent  │ ───────────────► │ HA  │ ────────────► │ ingest │
  │  eBPF   │   CN = DeviceID  │Proxy│   CN pinned   └───┬────┘
  └─────────┘                  └─────┘                   │
       │ disk spool on outage                            │ Kafka
       ▼                                                 ▼
   ┌────────┐                                    ┌──────────────┐
   │ spool  │                                    │ events.raw   │
   └────────┘                                    └──────┬───────┘
                                                        │
                                                 ┌──────▼───────┐
                                                 │  aggregator  │
                                                 │ counters +   │
                                                 │ p95/p99      │
                                                 └──────┬───────┘
                                                        │ events.aggregated
                                          ┌─────────────┴─────────────┐
                                    .hot-state                    .archive
                                          │                           │
                                     ┌────▼────┐                ┌─────▼────┐
                                     │  Redis  │                │ Postgres │
                                     └────┬────┘                └──────────┘
                                          │
                                   ┌──────▼───────┐
                                   │  MCP server  │ ◄── LLM client
                                   └──────────────┘
```

## Quick start

Requires Go 1.26+. No infrastructure needed for the default run — the whole pipeline has in-memory implementations of every port.

```bash
make run                    # simulated devices → full pipeline → MCP over stdio
```

With real infrastructure:

```bash
make infra-up               # Kafka (KRaft), Redis, Postgres via docker compose
go build -o bin/mcp-server ./mcp-server

./bin/mcp-server -redis localhost:16379 \
                 -postgres 'postgres://ironclaw:ironclaw@localhost:15432/ironclaw?sslmode=disable' \
                 -kafka localhost:19092 \
                 -emit-open-windows
```

The binary logs which backend it started with. Every adapter satisfies the same ports, so no service code changes between the two modes.

## MCP tools

| Tool | Description |
| --- | --- |
| `get_device_state(user_id, device_id)` | Counters and p95/p99 latency for one device |
| `get_user_devices(user_id, limit)` | Devices tracked for a user, capped and reporting `truncated` |
| `get_pipeline_metrics()` | Pipeline observability counters (admin scope) |
| `watch_device(user_id, device_id, timeout_seconds)` | Long-polls until the device's state changes |
| `reset_device_counters(user_id, device_id, actor)` | Publishes a command event; never writes state inline |

Serve over stdio (default) or streamable HTTP with mTLS and bearer tokens:

```bash
./bin/mcp-server -http-addr :8443 -require-auth \
                 -tls-cert edge.crt -tls-key edge.key -tls-client-ca ca.crt \
                 -tokens 'sometoken:user-000:ironclaw:admin'
```

Every user-scoped tool authorises the caller against the requested tenant. A principal without `ironclaw:admin` cannot read another user's devices, and fleet metrics are admin-only.

The catalogue declares what each tool does: the four readers carry `readOnlyHint`, and `reset_device_counters` declares `destructiveHint` and is not marked read-only, so a host can prompt differently for the one tool that changes state. The transport refuses cross-origin requests, closes idle sessions (`-mcp-session-ttl`), and caps concurrent watches (`-max-watches`), because each watch holds a subscription for up to five minutes.

## Design

Services depend on interfaces in `internal/domain/ports.go` and never on concrete clients — Kafka, Redis and Postgres are injected at `main`. Swapping the in-memory doubles for real infrastructure required no service code changes, which is the property the port boundary exists to buy.

| Package | Responsibility |
| --- | --- |
| `internal/domain` | Core types and every port interface |
| `internal/pipeline` | Synthetic source, batcher, injectable overflow strategies |
| `internal/aggregate` | DDSketch quantiles, tumbling windows, delta emission |
| `internal/aggregator` | Batch consumer, two-phase window emission |
| `internal/persister` | Window consumer, state writes |
| `internal/mcpserver` | Tool logic, authorisation, MCP protocol wiring |
| `internal/kafkabus` `internal/redisstore` `internal/postgresstore` | Real infrastructure adapters |
| `internal/bus` `internal/store` | In-memory doubles for the above |
| `internal/wire` | JSON and Protobuf codecs behind one interface |
| `internal/spool` | Crash-safe, size-capped disk queue for agent outages |
| `internal/metrics` `internal/tracing` | Prometheus counters, OpenTelemetry propagation |
| `internal/ebpf` `bpf/` | Kernel record decoding and the eBPF programs |

Three properties are load-bearing and worth knowing before changing anything:

**Emissions are deduplicated, not windows.** `AggregateWindow.Sequence` is a monotonic counter assigned at collect time. `Identity()` is `key#window#sequence` and `WindowIdentity()` is `key#window`. Late events for an already-closed window are emitted as a second delta with a new sequence, so they accumulate — while a genuine Kafka redelivery of the same emission is still absorbed. Collapsing these two identities silently dropped 6.3% of events under load.

**Redis and Postgres are independent.** Both consume `events.aggregated` in separate consumer groups. Neither depends on the other, and the Redis rebuild path is a Kafka replay, not a database scan. When Redis is unavailable the read path serves device state and device lists from the archive with `stale: true` rather than failing; a device the hot path reports as unknown is *not* probed in the archive, so the fast path stays fast.

**Freshness has a floor at the window size.** With closed-window emission an event cannot appear in Redis until its window closes, so a 10 s window measures p99 10.0 s. `-emit-open-windows` publishes the in-flight window as a delta each tick, which drops the same configuration to p99 1.017 s at the cost of one extra emission per active key per tick.

## Testing

```bash
make race                   # unit tests under the race detector
make integration            # adapter suites against live Kafka/Redis/Postgres
make lint                   # golangci-lint
make vuln                   # govulncheck against the Go vulnerability database
```

The agent builds in two capture modes, and CI builds both so the kernel path cannot rot:

```bash
make agent                  # synthetic capture, no privileges required
make agent-ebpf             # -tags ebpf, requires clang and CAP_BPF to run
```

Integration tests skip with an explanatory message when the containers are not reachable, so `make race` stays green either way.

Infrastructure-level checks, each requiring the relevant tooling:

| Target | What it proves |
| --- | --- |
| `make mtls-test` | A valid device cert is accepted; no cert and a wrong-CA cert are refused at the TLS layer |
| `make edge-test` | A forged `X-Device-Id` header does not survive the edge; Kafka only ever sees the certificate identity |
| `make loadtest` | JMeter driving the real edge with per-device mTLS keystores |
| `make freshness` | Agent-send to Redis-visible latency, measured directly |
| `make chaos-broker-kill` | Broker failure mid-load against a 3-broker RF=3 cluster |
| `make k8s-validate` / `make k8s-up` | Manifests against published schemas / a live kind cluster |
| `make tf-validate` | `terraform fmt -check` and `terraform validate` |
| `make ironclaw-verify` | The IronClaw extension contract: TLS, bearer injection, tool catalogue against the manifest's `max_tools`, tenant isolation, session binding |
| `make vuln` | No reachable vulnerabilities in the standard library or the module graph |

CI runs the race suite, lint with the `ebpf` build tag, the adapter suites against real Kafka/Redis/Postgres, the manifest and Terraform validation, `govulncheck`, and the IronClaw contract suite.

## Security

| Hop | Protection |
| --- | --- |
| agent → HAProxy | mTLS, client cert CN is the device identity |
| HAProxy → ingest | mTLS re-encrypt, permitted client CN pinned |
| service → Kafka | TLS + SASL/SCRAM |
| service → Redis | TLS + AUTH |
| service → Postgres | TLS, `sslmode=verify-full` |
| MCP client → server | mTLS + bearer token, per-tenant authorisation |
| MCP transport | Cross-origin requests refused, idle sessions expire, request bodies and concurrent watches bounded |

Each transport option refuses insecure combinations at startup rather than warning — credentials without TLS is a startup error.

Device identity comes from the certificate CN, extracted by HAProxy into a header that is *set, never merged*, so a client-supplied value cannot spoof it. Pod identity is stamped by ingest from the Kubernetes downward API for the same reason. Ingest additionally pins the permitted client-certificate CN at both the handshake and the handler, because the CA that signs the edge also signs device certs — without the pin, a device could bypass HAProxy and write telemetry attributed to another device.

Certificates for local development are issued by `deploy/pki/issue-certs.sh`. **All generated key material is gitignored** and nothing in this repository is a real credential.

## Deployment

- `deploy/k8s/base/` — namespace with restricted pod security, per-service ServiceAccounts, Deployments with HPAs and PodDisruptionBudgets, NetworkPolicies. `deploy/k8s/dev/` adds single-node data stores for local runs; `deploy/k8s/addons/` holds KEDA scalers and cert-manager Certificates.
- `deploy/terraform/` — VPC across three AZs, EKS with IRSA, MSK with TLS client auth, ElastiCache in cluster mode, RDS multi-AZ with a read replica.
- `deploy/observability/` — Grafana dashboard and Prometheus alert rules with thresholds derived from measured numbers.

## Documentation

| Document | Contents |
| --- | --- |
| [`docs/capacity.md`](docs/capacity.md) | The 1M-device arithmetic, per-component ceilings, measured codec benchmarks |
| [`docs/availability.md`](docs/availability.md) | SLOs, redundancy, degradation modes, and measured chaos results |
| [`docs/observability.md`](docs/observability.md) | Metrics, dashboards, alert thresholds and their derivation |
| [`docs/state-sync.md`](docs/state-sync.md) | What merges without coordination and what needs a fencing lock |
| [`docs/ironclaw-integration.md`](docs/ironclaw-integration.md) | Registering as an extension with the IronClaw agent OS |

## Status

The pipeline, adapters, edge, Kubernetes manifests and load path have all been executed against real infrastructure, not merely written. Each document above separates what was measured from what is design intent.

Known gaps, stated plainly:

- eBPF programs compile and their decoder is tested, but **attaching requires `CAP_BPF`** and has not been executed in the development environment. The agent selects its source with `-capture`: `synthetic` is the default and needs no privileges; `ebpf` loads the kernel programs through the same `EventSource` port. Both are built and tested — `make agent` and `make agent-ebpf` — and the eBPF path has been driven far enough to prove it reaches the loader, failing on the kernel memlock limit exactly as an unprivileged host must. The kernel/userspace record contract is *derived* rather than asserted: a test parses `struct event` out of the C source, computes its C layout with alignment and padding, and checks the decoder's size and every field offset against it. That test was written after it caught a real 40-vs-48 byte mismatch which would have rejected every kernel record on first attach.
- **`get_pipeline_metrics` and `watch_device` do not degrade.** Both read the hot path directly and neither has an archive equivalent, so a Redis outage leaves them unavailable while device reads keep serving `stale` data. Serving fleet counters from Prometheus instead would close the first of these.
- **The user a device reports is self-attested.** Device identity is pinned to the certificate CN and pod identity is stamped by ingest, but `user_id` arrives in the batch body, so a device holding a valid certificate can attribute its telemetry to another tenant. Closing this needs a device-to-user binding at ingest — a registry lookup or a claim in the certificate — and no such source exists yet. Treat the ingest write path as trusted-network until it does. The identifier delimiters that build correlation keys (`{`, `}`, `:`) are part of the same boundary: a user id containing them can collide with another key's string, so a deployment that also binds users should reject them at the edge.
- The fencing lock is implemented and tested, but **no feature consumes it yet** — device ownership reassignment and quarantine are unbuilt.
- Grafana and Prometheus **provisioning** is not built. The dashboard and rules validate and every metric name they reference was cross-checked against a live scrape, but neither has been loaded into a running Grafana here.
- Cross-region active-active is designed but **multi-region convergence is untested**.
- The IronClaw extension manifest is validated against the host's **own parser**, but **no running IronClaw instance has loaded it** — installing the agent runtime and completing a live tool call is the remaining step.

## License

No license has been chosen yet. Without one, default copyright applies and others cannot legally reuse this code — add a `LICENSE` file before making the repository public if reuse is intended.
