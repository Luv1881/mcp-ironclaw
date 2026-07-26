# IronClaw Capacity Model

The arithmetic behind every sizing number in `deploy/terraform`. Phase 8.1 asked for this so partition counts and instance types stop being guesses.

## Inputs

| Symbol | Meaning | Assumed value |
| --- | --- | --- |
| `D` | concurrent devices | 1,000,000 |
| `e` | events per device per second | 10 |
| `B` | events per batch | 500 |
| `b` | batch period | 200 ms |
| `S` | encoded bytes per event | 189 B JSON / 54 B Protobuf (measured, see below) |
| `K` | correlation keys per device | 4 (process/pod pairs) |
| `W` | window size | 10 s |

## Derived load

- **Event rate**: `D × e` = **10,000,000 events/s**.
- **Batch rate**: `D / b` = 1,000,000 / 0.2 s = **5,000,000 batches/s** at the stated batch period. This is the number that dominates everything, and it is the first thing to push back on: at `b = 1 s` and `B = 500` the batch rate drops to 1,000,000/s while still carrying the full event rate. **Recommendation: batch period 1 s, not 200 ms.**
- **Raw ingress**: `10e6 × 189 B` ≈ **1.89 GB/s** on the wire with JSON, `10e6 × 54 B` ≈ **540 MB/s** under Protobuf.
- **Daily Kafka volume** at RF=3: JSON `1.89 GB/s × 86400 × 3` ≈ **490 TB/day**; Protobuf ≈ **140 TB/day**. With 24 h retention that is the steady-state disk requirement, and it is the single most expensive number in the design. Protobuf removes 350 TB/day of it; sampling is the only remaining lever after that.

### Wire format: measured, not assumed

`internal/wire` implements both codecs behind one `Codec` interface, and `go test -bench` in that package produced these numbers on a 500-event batch (12-core dev box):

| | JSON | Protobuf | Protobuf advantage |
| --- | --- | --- | --- |
| encoded size | 94,520 B (189.0 B/event) | 27,008 B (54.0 B/event) | **3.50× smaller** |
| encode | 162,953 ns/op | 122,903 ns/op | 1.33× faster |
| decode | 1,240,819 ns/op | 261,669 ns/op | **4.74× faster** |

Decode is where it matters: every event is decoded at least once by the aggregator and again by the persister, so the 4.74× applies to the tier that is CPU-bound, while the 3.50× applies to broker disk and cross-AZ transfer. The earlier estimate in this document (~180 B / ~60 B, "roughly 3×") was close enough that no sizing conclusion changes, but the table above is now measured rather than assumed.

Selection is a flag, not a rebuild: `-wire-format protobuf` on `mcp-server` and `ingest`, defaulting to `json` so an existing deployment reading a topic full of JSON keeps working. **Verified end to end**: an agent posting 400 events through the mTLS edge into a Protobuf-configured ingest produced `count=400` in Redis via a Protobuf-configured aggregator, with the on-topic bytes confirmed as Protobuf field-tag encoding rather than JSON.

Migration across the two formats is the one operational cost: producers and consumers of a given topic must agree. The safe path is to drain a topic (or cut over to a new topic prefix) rather than flip formats in place.

## Kafka partitions

A single partition sustains roughly 10 MB/s of writes comfortably on gp3-class storage.

```
partitions_needed = 1.8 GB/s ÷ 10 MB/s ≈ 180
```

The plan's starting point of 96 is therefore **about half of what the stated fleet needs**. Either start at 192 (a power-of-two multiple that divides evenly across 3 AZs and 48 consumers) or accept that 96 supports ~500k devices at these rates. Partitions cannot shrink, so over-provisioning is the cheaper error: **`kafka_partitions` default should be 192**.

Consumer parallelism is capped by partition count, so the aggregator tier can never exceed it. At 192 partitions and ~200k events/s per aggregator pod, the tier needs ~50 pods, which is why `maxReplicaCount` on the KEDA object is 96 rather than a small number.

## Aggregator memory

Each open correlation key holds one DDSketch. A DDSketch at 1% relative accuracy over a 10 s window holds on the order of 200 buckets ≈ **4 KB** including map overhead.

```
keys = D × K = 4,000,000
memory = 4e6 × 4 KB ≈ 16 GB across the tier
```

Spread over 50 pods that is ~320 MB each, which fits the 2 GiB limit in `deploy/k8s/base/20-aggregator.yaml` with headroom for the pending stage. **This is why `MaxOpenWindows` exists**: without a ceiling a key-cardinality spike (a misbehaving device inventing process IDs) turns directly into an OOM. The default of 500,000 keys per pod is ~2 GB worst case, matching the limit.

## Redis

Hot state is one hash per device plus a dedupe sorted set:

```
per device ≈ 250 B state + 6 h of window identities
identities ≈ 6 h ÷ 10 s × K = 8,640 entries/device worst case
```

That dedupe set is the dominant term and the reason it is trimmed by score rather than left to a whole-key TTL. Budget **~64 GB** across the cluster for 1M devices, which at `cache.r7g.large` (13 GB usable) means **6 shards minimum**; the Terraform default of 3 supports roughly 500k devices. Keys carry a `{user_id}` hash tag so a user's state, dedupe set, and device index share a slot.

## Postgres

Aggregated windows only, never raw events:

```
rows/day = D × K × (86400 ÷ W) = 1e6 × 4 × 8640 ≈ 34.5 billion/day
```

That is far beyond a single RDS instance, and it is the second number to push back on. Options, in order of preference: widen the window for the archive tier (60 s windows cut this 6×), aggregate to per-device rather than per-process before archiving, or archive to S3/Parquet and keep only recent days in Postgres. **As written, the durable archive is the least scalable component in the design and should not be considered solved.**

Day partitioning is implemented so retention is a partition drop rather than a mass `DELETE`.

## Edge

Long-lived HTTP/2 connections, one per device:

```
connections = 1,000,000
per instance ≈ 150,000 (file descriptors, memory, TLS state)
instances ≈ 7
```

Matches the "5–10 edge instances" in the plan. Each needs `nofile` well above 150k and `SO_REUSEPORT` for multi-process accept balancing.

## Where the model is uncertain

Every number above is arithmetic from assumed inputs, not measurement. The only measured figures in this repository are the load test in `docs/availability.md` (94 batches/s, ~9,400 events/s on one laptop, 20 devices) — roughly **one thousandth** of the target rate. The per-partition and per-pod throughput constants are industry rules of thumb, not benchmarks of this code. Before trusting the sizing, benchmark a single aggregator pod and a single Kafka partition with real payloads and substitute the measured constants.
