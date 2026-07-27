# IronClaw Availability, SLOs and Failure Behaviour

This is the Phase 11 deliverable. It states what the system promises, what happens when each component fails, and how it recovers.

## Service Level Objectives

| Path | Objective | Measured as |
| --- | --- | --- |
| Device ingest (agent → 202 accepted) | 99.95% success, monthly | Non-5xx, non-timeout responses at the HAProxy edge |
| Read path (MCP tool call → response) | 99.9% success, monthly | Successful `tools/call` responses from the MCP server |
| End-to-end freshness | p99 under 5 s | Agent send timestamp → the event being visible in Redis |
| Aggregation accuracy | p95/p99 within 1% relative error | DDSketch relative accuracy, asserted in tests against exact quantiles |

Error budget follows directly: 99.95% monthly allows roughly 21 minutes of failed ingest. Burn faster than 2% per day and deploys stop until the burn rate recovers.

## No Single Point of Failure

| Tier | Redundancy |
| --- | --- |
| HAProxy edge | ≥2 instances across availability zones behind an NLB; sized so any one instance can absorb the full connection count |
| Ingest | ≥3 replicas, HPA on request rate and publish latency, PodDisruptionBudget of `maxUnavailable: 1` |
| Kafka | Replication factor 3 across 3 AZs, `min.insync.replicas=2`, `acks=all` |
| Aggregator | One consumer per partition, KEDA-scaled on consumer lag |
| Postgres | Multi-AZ primary with synchronous standby plus an async read replica |
| Redis | Cluster mode, one replica per shard, automatic failover |
| MCP server | Stateless, ≥2 replicas behind a load balancer |

## Failure Modes and Degradation

**Redis unavailable.** The MCP read path fails over to Postgres and sets `stale: true` on every `DeviceState` response, so callers can tell a degraded read from a fresh one. Writes are unaffected: the Redis writer is an independent consumer group and simply lags, then catches up from its committed offset.

**Kafka degraded or unreachable.** Ingest returns 503 and the edge sheds load. Agents spool batches to a size-capped local disk queue and replay when the path recovers. Nothing is acknowledged that was not durably written, so an agent never believes a dropped batch succeeded.

**Agent restart during an outage — measured.** The interesting case is not spool-and-replay, it is spool, *restart*, spool more, then replay. Measured against the live edge:

| Phase | Action | Result |
| --- | --- | --- |
| 1 | Edge down, agent produces 2,000 events, then is killed mid-outage | 21 batches on disk |
| 2 | Agent **restarts** on the same spool directory, produces 2,000 more, edge still down | 42 batches on disk — the original 21 intact |
| 3 | Edge returns, agent drains | spool empties to 0 |
| 4 | Reconcile in Redis via the real edge, Kafka and aggregator | **4,200 of 4,200 events, zero loss** |

Phase 2 is the assertion that matters. Before this was fixed, restarting the agent regenerated spool filenames from zero and `os.Rename` silently overwrote every surviving entry, so phase 1's 2,000 events were destroyed on disk while the spool still reported holding them. An earlier "61 batches spooled, all drained, zero loss" run could not see it, because it never restarted the agent with entries still in the queue.

**A permanently refused batch no longer stalls the queue.** A 4xx other than 408/429 is treated as permanent: it is not spooled, and if already spooled it is released and counted as `agent_batches_refused` so the drain moves on. Previously any non-202 was retried forever, so one malformed batch at the head blocked every healthy batch behind it until the byte budget dropped them.

**Postgres unavailable.** Aggregated windows accumulate in Kafka. With retention of at least 24 hours, the persister catches up once the database returns. The Redis hot path is unaffected because it consumes the same topic independently — this is exactly why the original DB-poll-to-Redis design was rejected.

**Aggregator pod loss or rebalance.** Offsets are committed only after a window is emitted, so the new partition owner replays from the last committed offset and rebuilds the sketch from the actual events. Replay is safe because every window carries an identity (`correlation key + window_id`) and the store applies each identity exactly once.

**A device floods the edge.** The HAProxy stick table rate-limits per client-certificate CN, so one misbehaving device cannot consume the ingest budget of the rest. In-kernel sampling and a per-CPU token bucket in the eBPF program cap what a device can generate in the first place.

**Device certificate compromised.** Revoke it into the CRL that the edge loads; the next handshake from that identity is rejected. Because ingest verifies that the payload's `device_id` matches the authenticated certificate CN, a compromised device cannot impersonate another device even before revocation lands.

## Recovery Objectives

- **RPO ≈ 0** for acknowledged events. Acknowledgement happens only after `acks=all` with two in-sync replicas.
- **RTO**: edge under 1 minute (health-check driven), ingest under 2 minutes (HPA and rolling restart), aggregator under 5 minutes (rebalance plus replay of the open window), Redis under 1 minute (automatic failover), Postgres under 5 minutes (Multi-AZ promotion).
- **Redis rebuild**: replay `device.events.aggregated` from the retained offset. Only a rebuild older than Kafka retention falls back to a batch read of Postgres.
- **Cross-region**: MirrorMaker 2 replicates the aggregated topic. Counters and sketches are mergeable, so regions converge without coordination; the few non-mergeable operations use a Redis lock with a fencing token.

## Measured Results

A JMeter run against the real edge (`make loadtest`, 20 device certificates, 100 events per batch, 40 s, one batch every 200 ms per device — the profile a real fleet produces):

| Measure | Result |
| --- | --- |
| Batches accepted | 3,765 (0.05% error, 2 connection resets during ramp) |
| Throughput | 94 batches/s ≈ 9,400 events/s |
| Edge latency p50 / p95 / p99 | 4 ms / 7 ms / 10 ms |
| Edge latency max | 277 ms (first TLS handshake per connection) |
| Events ingested | 376,401 |
| Events counted in Redis | 376,401 — **exact reconciliation, zero loss** |
| Windows emitted / applied to Redis | 1,633 / 1,633 |
| Windows archived to Postgres | 1,633 — both consumer groups received every window |
| Duplicate emissions in Postgres | 0 |
| Time to drain the backlog after the run | ~45 s |

Note the drain time: Redis reaches exact reconciliation about 45 s after the load stops, because the aggregator is still working through consumer lag. Measuring immediately after a run understates the count and looks like loss. The freshness SLO (p99 under 5 s) has **not** been measured — that needs per-event timestamping from agent send to Redis visibility, which this harness does not yet do.

The per-certificate rate limiter was also exercised, unintentionally at first. An earlier unthrottled run pushed roughly 145 batches/s *per device*, and HAProxy returned 429 for 118,998 of 130,998 requests while still accepting 12,000. That is the stick table doing exactly its job: one misbehaving device cannot consume the ingest budget of the fleet. The realistic profile above sits far below the 600-per-10-s ceiling and sees no throttling.

## Chaos and SLO Results

**End-to-end freshness (`make freshness`)** — measured by posting a batch through the edge and polling Redis until it is visible:

| Configuration | p50 | p95 | p99 | Verdict |
| --- | --- | --- | --- | --- |
| 2 s window, closed-window emission | 1.998 s | 2.013 s | 2.017 s | within 5 s |
| 10 s window, closed-window emission | 10.0 s | 10.01 s | 10.012 s | **breaches 5 s** |
| 10 s window, partial emission | 995 ms | 1.017 s | 1.017 s | within 5 s |

The middle row matters: with closed-window emission an event cannot become visible until its window closes, so **freshness has a hard floor at the window size**. The plan's 10 s tumbling window and its p99-under-5 s freshness SLO were mutually incompatible. Enabling `-emit-open-windows` publishes the in-flight window as a delta each tick, so freshness tracks the emit interval instead and both requirements hold. This costs one extra emission per active key per tick, which is the trade to make consciously at fleet scale.

**Broker kill (3 brokers, RF=3, `min.insync.replicas=2`)** — a broker was killed 25 s into a 60 s load run. ISR shrank from `[1,2,3]` to `[1,3]` and partition leadership moved to a surviving broker. 29 of 4,654 batches were refused during leader election (0.62%) and latency spiked to 15 s, but **every acked event survived: 465,200 acked, 465,200 counted**. The refused batches were never acknowledged, so RPO ≈ 0 for acked events holds.

**Redis outage** — with Redis stopped, ingest keeps accepting at 202 in ~20 ms, events stay durable in Kafka, and when Redis returns the aggregator replays from its committed offset and rebuilds the hot state. The 8 events posted *during* the outage were present in Redis afterwards.

**Autoscaling (kind, metrics-server)** — under 242 batches/s the ingest HPA scaled **3 → 4 → 6 → 8** replicas, all reaching Ready with 0.00% errors, and returned to the floor of 3 after load stopped.

## What Is Not Yet Proven

The load figures above come from a 20-device, 40-second run on one laptop. They demonstrate the path works and is lossless; they are **not** evidence that the 1M-device target holds. Nothing here has been run multi-AZ, multi-broker, or with a Redis Cluster, so the redundancy and failover claims remain design intent. Broker-kill, Redis-failover, and autoscaling behaviour under load are still untested.

What *is* verified today, by tests and runs in this repository:

- Idempotent window application under redelivery and under concurrent duplicate writes.
- No event loss through the in-process pipeline, including under a blocking backpressure policy.
- Ordered, lossless shutdown: producers stop, batches drain, windows flush, then consumers stop.
- Partition ownership is exclusive, and every message is delivered to exactly one consumer.
- Percentile accuracy within 1% of exact quantiles over 100k samples.
- Device identity spoofing is rejected at ingest, and at the edge: a batch whose body claims another device is refused with 403, and a client-supplied `X-Device-Id` header never survives HAProxy, which overwrites it from the certificate CN.
- mTLS is enforced at the edge: valid certificate accepted, missing certificate refused with TLS alert 116, untrusted CA refused with alert 48.
- A device certificate cannot bypass HAProxy and impersonate another device at ingest: ingest pins the permitted client-certificate CN, so a device connecting directly is refused at the handshake with alert 42.
- Every adapter (Redis, Postgres, Kafka) passes its suite against the real service.
