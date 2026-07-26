# State Sync: What Merges and What Needs a Lock

Phase 6.3 asked for an explicit decision on "hot sync or lock" per piece of state. This is that decision.

## The rule

State that is **commutative and associative** merges without coordination. State where the *order* of operations changes the outcome needs a single writer or a lock. Every piece of IronClaw state is classified below; nothing is left to judgement at runtime.

## Merge without coordination (CRDT-style)

| State | Why it merges | Mechanism |
| --- | --- | --- |
| `count`, `error_count`, `bytes` | Grow-only counters; addition commutes | `HINCRBY` per emission, deduplicated by emission identity |
| Latency sketches | DDSketch buckets are additive; merging two sketches equals sketching the union | `Sketch.Merge`, exercised by a commutativity test |
| Device index per user | Grow-only set | `SADD` |
| Pipeline metrics | Grow-only counters | `HINCRBY`, fanned out to Prometheus |

The load-bearing detail is **idempotence**, not commutativity alone. At-least-once delivery means the same emission can arrive twice, and addition is commutative but *not* idempotent. That is what `AggregateWindow.Sequence` is for: dedupe keys on `correlation key + window id + emission sequence`, so a redelivery is dropped while a genuinely new delta still accumulates. Without the sequence, late data was silently discarded — see the Peer Review Log.

Because these merge, two regions can both accept writes and converge. Cross-region replication of `events.aggregated` via MirrorMaker 2 is sufficient; no coordination is required on the counter path.

## Requires a lock or a single writer

| State | Why it cannot merge | Mechanism |
| --- | --- | --- |
| Device ownership / tenant reassignment | Last-writer-wins silently steals telemetry between tenants | Redis `SET NX PX` with a fencing token |
| Counter reset | Reset is not commutative with increment: reset-then-add and add-then-reset differ | Serialized through the command topic, partitioned by `user_id` |
| Device quarantine / revocation | Security-relevant; a stale write must never resurrect a revoked device | Fencing token, plus CRL as the durable source of truth |
| Kafka partition assignment | Two owners of one partition double-process | Kafka consumer group protocol |

### Fencing token protocol

A lock alone is not sufficient: a process can pause (GC, scheduler) past its lease and wake believing it still holds the lock. The token makes the stale writer harmless.

1. Acquire with `SET lock:{resource} <token> NX PX <ttl>`, where the token is a monotonic counter from `INCR fence:{resource}`.
2. Every write carries the token and is applied by a Lua script that refuses to write when the stored token is greater than the presented one.
3. Release with a compare-and-delete script, never a bare `DEL`.

**Status: the primitive is implemented and tested; no feature consumes it yet.** `domain.FencedLocker` is the port; `store.Locker` (in-memory, injectable clock) and `redisstore.Store` (Lua `SET`/`INCR`/compare-and-delete, keys hash-tagged `{resource}` so lock and fence share a cluster slot) are the two implementations. Both expose `Guard(ctx, lease)`, which is the half that actually makes fencing work: it refuses a write whose token is behind the current fence, so a holder that paused past its lease and woke up believing it still owned the lock is rejected rather than obeyed.

What the tests prove, against a real Redis as well as in memory:

- A second acquire while a lease is live is refused.
- Tokens advance monotonically per resource across acquire/release cycles.
- After a lease expires and someone else acquires, the *old* holder's `Guard` fails and the new holder's succeeds — this is the exact scenario a bare `SET NX PX` lock gets wrong, and a lock without tokens passes every other assertion in the suite while failing this one.
- A stale `Release` cannot unlock the resource out from under the new holder.
- 16–32 concurrent contenders yield exactly one winner.

The command path still serializes resets through Kafka partitioned by `user_id`, which gives single-writer-per-user semantics and is sufficient for the reset case; `ResetDevice` preserves `last_window_id` so an in-flight window cannot re-apply after a reset. Ownership reassignment and quarantine remain unbuilt — but the lock they need is now in place rather than on paper.

## Read-side merging for global views

A global view over regions merges counters by summation and sketches by `Merge`. Reads must not sum across regions that both received the *same* upstream events — with MirrorMaker the aggregated topic is replicated, not re-aggregated, so each region holds the same windows. Deduplicate by emission identity before summing, exactly as the stores do.

## What is proven

Sketch merge commutativity, idempotent apply under redelivery and concurrent duplicate writes, late-delta accumulation, and the fencing-token protocol above are covered by tests in `internal/aggregate`, `internal/store` and `internal/redisstore`. **Multi-region convergence is not tested** — there is no second region in this environment, and the two-region docker-compose test named in Phase 6.3's test gate has not been built.
