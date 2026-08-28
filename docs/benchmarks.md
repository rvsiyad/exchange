# Benchmarks

Order→fill latency and sustained throughput for the whole venue — real
gateway, real matching engine, real settlement, real Redpanda and TigerBeetle
— under constant offered load.

## Headline numbers

At **1,000 orders/s sustained for 60s** — the highest offered rate at which
every stage, settlement included, keeps up — on the hardware below:

| span | p50 | p90 | p99 | p99.9 | max |
|---|---|---|---|---|---|
| **order→fill** | 1.20ms | 7.47ms | 19.5ms | 62.9ms | 95.3ms |
| HTTP accept (reserve + publish) | 0.80ms | 6.39ms | 18.3ms | 58.1ms | 90.5ms |

25,877 order→fill samples over 60,000 measured orders (≈43% crossed), 649
fills/s emitted, dispatch lateness p99 0.52ms, zero rejections, zero errors.

### Across offered rates

Every run: 20s warm-up + 60s measured, seed 42, same scenario. Order→fill
percentiles in ms:

| offered | achieved | fills/s | p50 | p90 | p99 | p99.9 | settlement |
|---|---|---|---|---|---|---|---|
| 500/s | 500/s | 324 | 1.94 | 2.57 | 11.8 | 35.6 | keeps up (settled p50 1.7ms) |
| 1,000/s | 1,000/s | 649 | 1.20 | 7.47 | 19.5 | 62.9 | keeps up (settled p50 3.1ms, excursions to ~0.7s at p99) |
| 1,500/s | 1,500/s | 974 | 1.40 | 11.2 | 22.8 | 42.3 | **backlogs** (median 18.7s behind) |
| 2,000/s | 2,000/s | 1,299 | 2.01 | 12.1 | 29.3 | 83.6 | **backlogs** (median 46s behind) |

Two findings worth more than the table:

1. **The matching path is not the bottleneck — settlement is.** Gateway,
   Kafka, and the engine hold order→fill p99 under 30ms all the way to
   2,000 orders/s. Settlement, a single idempotent consumer executing one
   linked TigerBeetle transfer chain per fill, saturates between ~650 and
   ~975 fills/s; past that it falls behind and the backlog grows without
   bound for as long as the load lasts (it drains once load stops — nothing
   is lost, money is just late). The fix is textbook and deliberate future
   work: batch many fills' chains into one TigerBeetle submission —
   TigerBeetle is designed for 8k-transfer batches — and the ceiling moves
   an order of magnitude.
2. **Higher load can *improve* the median.** p50 at 1,000/s (1.20ms) beats
   500/s (1.94ms): at low rates Kafka's client-side batching waits on
   linger; at higher rates batches fill and dispatch immediately. Averages
   would have hidden this; distributions surfaced it.

## Methodology

Numbers without methodology are noise. This is how these were produced, and
why each choice matters.

**The harness is open-loop.** Orders are dispatched on a fixed schedule
(`bench.rate` per second) that never waits for a response — one dispatcher
thread on the schedule, one virtual thread per in-flight request. A
closed-loop generator (send, wait for reply, send the next) lets a slow
system slow the load down: the worse the system performs, the fewer hard
samples the benchmark collects, and the better the numbers look. That failure
mode is *coordinated omission*, and open-loop dispatch is the fix. Real order
flow doesn't wait for your matching engine either.

**Latency is measured from the scheduled send time, not the actual one.** If
the dispatcher (or the whole system) stalls for 100ms, the orders that were
due during the stall are charged the stall. The harness also reports its own
dispatch lateness (p99 and max) with every run — if the load generator can't
hold the schedule, the run is invalid and the numbers say so.

**Warm-up is discarded.** The first `bench.warmup` seconds (20s for the
recorded runs) pay for JIT compilation, Kafka client batching heuristics, and
the TigerBeetle client's session registration, and are excluded from every
distribution except where noted.

**Percentiles, not averages.** All distributions are recorded in HdrHistogram
at 3 significant digits. An average order→fill latency would hide exactly the
thing a trading system is judged on: the tail.

### Spans

| Span | From → to | Measured by |
|---|---|---|
| **order→fill** | scheduled dispatch → taker's first `Fill` observed on the `fills` topic | harness clock, both ends (coordinated-omission-safe) |
| **HTTP accept** | actual send → 202 | the synchronous path: validate, reserve worst-case funds in TigerBeetle, publish to Kafka with `acks=all`, ack |
| **order→settled** | gateway accept → funds moved in TigerBeetle | the services' own `settlement_latency_seconds` summary; whole run including warm-up, so its far tail carries start-up cost |

Only crossing (taker) orders have an order→fill latency — a resting order
produces no fill by definition, so roughly the marketable half of the flow
contributes samples. The order→fill span crosses the full pipeline: HTTP →
gateway (TigerBeetle reserve + Kafka publish) → `orders` topic → engine match
→ `fills` topic → harness consumer.

### Topology

Everything runs on one machine, services in one JVM (as the storm test
assembles them), brokers and ledger in containers: Redpanda v24.2.7
(single node, 4 `orders` partitions) and TigerBeetle 0.16.27 (single
replica) via Testcontainers. Engine snapshot cadence is the production
default (every 1000 commands). The flow is the storm generator minus
cancels: 8 users, 2 symbols, quantity 1–4, limit prices uniform in mid±5
(≈ half marketable), funding sized so the ledger never rejects — a
rejection would be a lost sample. The whole scenario derives from one seed.

One-box numbers measure the software stack, not the network: order→fill
includes two real Kafka round trips and a TigerBeetle two-phase reserve, but
no cross-host hops. Treat them as the architecture's floor, not a production
claim.

### Hardware

Recorded runs: Apple M4 (10 cores), 16 GB, macOS 15.6, OpenJDK 21.0.12.1
(Homebrew), Docker Desktop 29.7.2 — stated because percentiles without
hardware are meaningless.

## Reproducing

```
./mvnw -pl bench test -Dbench=true -Dbench.rate=1000 -Dbench.warmup=20 -Dbench.measure=60
```

The class is opt-in (`-Dbench=true`) so CI's `mvn verify` never runs it:
benchmark numbers from shared CI runners are noise. A run prints its seed;
`-Dbench.seed=<seed>` replays the same scenario (the interleaving, as ever,
is the system's own).
