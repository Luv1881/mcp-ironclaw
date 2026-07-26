# IronClaw

An event-driven device telemetry pipeline in Go, exposed to LLM clients through a [Model Context Protocol](https://modelcontextprotocol.io) server.

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
| `get_user_devices(user_id)` | Every device tracked for a user |
| `get_pipeline_metrics()` | Pipeline observability counters (admin scope) |
| `watch_device(user_id, device_id, timeout_seconds)` | Long-polls until the device's state changes |
| `reset_device_counters(user_id, device_id, actor)` | Publishes a command event; never writes state inline |

Serve over stdio (default) or streamable HTTP with mTLS and bearer tokens:

```bash
./bin/mcp-server -http-addr :8443 -require-auth \
                 -tls-cert edge.crt -tls-key edge.key -client-ca ca.crt \
                 -static-tokens 'sometoken:user-000:ironclaw:admin'
```

Every user-scoped tool authorises the caller against the requested tenant. A principal without `ironclaw:admin` cannot read another user's devices, and fleet metrics are admin-only.

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

**Redis and Postgres are independent.** Both consume `events.aggregated` in separate consumer groups. Neither depends on the other, and the Redis rebuild path is a Kafka replay, not a database scan.

**Freshness has a floor at the window size.** With closed-window emission an event cannot appear in Redis until its window closes, so a 10 s window measures p99 10.0 s. `-emit-open-windows` publishes the in-flight window as a delta each tick, which drops the same configuration to p99 1.017 s at the cost of one extra emission per active key per tick.

## Testing

```bash
make race                   # unit tests under the race detector
make integration            # adapter suites against live Kafka/Redis/Postgres
make lint                   # golangci-lint
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

## Security

| Hop | Protection |
| --- | --- |
| agent → HAProxy | mTLS, client cert CN is the device identity |
| HAProxy → ingest | mTLS re-encrypt, permitted client CN pinned |
| service → Kafka | TLS + SASL/SCRAM |
| service → Redis | TLS + AUTH |
| service → Postgres | TLS, `sslmode=verify-full` |
| MCP client → server | mTLS + bearer token, per-tenant authorisation |

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

## Status

The pipeline, adapters, edge, Kubernetes manifests and load path have all been executed against real infrastructure, not merely written. Each document above separates what was measured from what is design intent.

Known gaps, stated plainly:

- eBPF programs compile and their decoder is tested, but **attaching requires `CAP_BPF`** and has not been executed in the development environment. The agent ships a synthetic source behind the same `EventSource` port.
- The fencing lock is implemented and tested, but **no feature consumes it yet** — device ownership reassignment and quarantine are unbuilt.
- Grafana and Prometheus **provisioning** is not built. The dashboard and rules validate and every metric name they reference was cross-checked against a live scrape, but neither has been loaded into a running Grafana here.
- Cross-region active-active is designed but **multi-region convergence is untested**.

## License

No license has been chosen yet. Without one, default copyright applies and others cannot legally reuse this code — add a `LICENSE` file before making the repository public if reuse is intended.
