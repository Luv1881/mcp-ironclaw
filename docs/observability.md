# Observability: Metrics, Dashboards and Alerts

Phase 9 asked for Prometheus metrics, OpenTelemetry tracing, and Grafana dashboards with alerts. This is what exists and how to read it.

## Where metrics come from

Every service records through `domain.MetricsRecorder`, a one-method port. Three implementations are composed at startup:

| Implementation | Purpose |
| --- | --- |
| `metrics.Prometheus` | registry-backed counters, scraped at `/metrics` |
| `redisstore.Store` (Redis hash) | the counters the MCP `get_pipeline_metrics` tool reads |
| `metrics.Async` | a bounded, non-blocking buffer in front of Redis |
| `metrics.Fanout` | writes to all of the above |

`metrics.Async` exists because of a measured failure: with metrics written synchronously to Redis, a Redis outage pushed a single ingest request to **18.1 seconds**. Recording is now fire-and-forget with a bounded queue, so a slow metrics backend degrades observability rather than availability.

Both binaries expose a scrape endpoint:

```
./ingest      -health-addr :8080     # GET /metrics
./mcp-server  -health-addr :8080     # GET /metrics  (headless mode)
```

Counter names are sanitised into `ironclaw_<name>_total`.

### Pre-registration, and why it matters

A Prometheus counter that has never been incremented does not appear in a scrape at all. That makes a fresh deployment's dashboards read "No data", and — worse — makes any alert of the form *"X is happening but Y is not"* silently unfirable, because `Y` has no series to compare against. `metrics.KnownCounters()` lists every counter the system emits and both binaries pre-register the full set at startup, so every series exists from the first scrape at value zero.

Two tests keep that list honest: one fails if any `Metric… = "…"` constant in the tree is missing from the list, the other fails if the list names a counter nothing emits any more.

## Dashboards and alerts

- `deploy/observability/dashboard-pipeline.json` — Grafana dashboard, four rows: ingest SLO, freshness SLO, correctness (duplicate suppression, dead letters, stage failures), and agent fleet degradation.
- `deploy/observability/alerts.yaml` — a `PrometheusRule` with three groups.

Thresholds are derived from the measured numbers in `docs/availability.md`, not invented:

| Alert | Threshold | Where the number comes from |
| --- | --- | --- |
| `IngestErrorBudgetBurningFast` | 0.75% failure ratio for 5m | 99.95% monthly = 0.05% allowed; 0.75% burns the whole ~21-minute budget in under two days. Measured baseline under load was 0.05%. |
| `IngestErrorBudgetBurningSlow` | 0.1% for 1h | Twice the objective, 20× the measured baseline. |
| `AggregatorConsumerLagBreachingFreshness` | drain time > 5s for 5m | The freshness SLO is p99 < 5 s. Lag ÷ consumption rate is time-to-drain. Measured drain after a 40 s load run was ~45 s. |
| `AgentSpoolDroppingBatches` | any rate for 5m | The spool is the designed outage response; only a *full* spool is real loss. |

Every alert that fires on a *ratio* uses `clamp_min(..., 1)` on the denominator so an idle pipeline does not divide by zero into a false page.

## What the dashboard caught

Building this found two defects that all 300-odd tests had missed, because both are only visible in a running system:

1. **`windows_applied` never reached Prometheus.** `redisstore.Store` incremented its *own* counters internally, bypassing the fanout the runtime had wired up. Redis hot-state writes — the single most important throughput signal on the read path — were invisible to Prometheus while every other counter worked. The store now takes an injectable recorder and the runtime attaches the fanout.

2. **A healthy local run was dead-lettering 60 valid windows, silently.** The consumer retried each record to exhaustion, dead-lettered it, and **discarded the handler's error entirely** — no log line, no reason, nothing but a counter ticking up. The underlying cause was a legacy Redis key: an earlier schema stored the dedupe set with `SADD`, the current script uses `ZSCORE`/`ZADD`, and the type collision made every window for that device fail forever with `WRONGTYPE`.

Both halves are fixed:

- `kafkabus.Config.OnHandlerError` reports every failed attempt with topic and attempt number; the runtime logs it and increments `consumer_handler_errors`. Dead-lettered records now carry `ironclaw-dlq-reason` and `ironclaw-dlq-origin` headers, so the DLQ explains itself instead of being an opaque pile of bytes.
- The dedupe key carries a schema version (`:applied:v2`). A key whose Redis *type* changes between versions cannot be migrated in place, so the version in the name keeps old and new from ever colliding; the old keys age out under their existing TTL. State hashes and the device index are untouched — their types never changed.

The general lesson is worth keeping: **a counter with no reason attached is not observability.** A dead-letter count tells you something broke; the reason header tells you it was `WRONGTYPE` on a dedupe key, which is a five-minute fix instead of an afternoon.

## Tracing

`internal/tracing` provides a W3C-propagating OpenTelemetry provider. Trace context is injected into agent HTTP requests and into Kafka record headers, and extracted on the consuming side, so one event is followable agent → ingest → aggregator → persister. `-otlp-endpoint stdout` prints spans locally without a collector; verified by reading real `traceparent` values off a live Kafka topic.

## Not built

Grafana provisioning (datasource + dashboard ConfigMaps), a Prometheus Operator deployment, and Alertmanager routing. The dashboard JSON and the `PrometheusRule` are written against standard schemas and validate, but **neither has been loaded into a running Grafana or Prometheus here** — no such cluster exists in this environment. The metric names they reference were verified a different way: every `ironclaw_*` name in both files was cross-checked against a live `/metrics` scrape from the running pipeline, and all 24 are present.
