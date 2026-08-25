# ADR 0001: Single writer per partition

Status: accepted (session 3) — draft, refine after teach-back

## Context

The matching engine must process orders for a symbol strictly in order (price-time
priority is meaningless if arrival order is ambiguous), and we want to scale across
symbols. The classic answer is a concurrent order book guarded by locks, which is
both slow (contention on the hot path) and hard to reason about.

## Decision

Orders are published to the `orders` topic **keyed by symbol**, and the engine runs
**one worker thread per partition**, each exclusively owning the `OrderBook`s of
every symbol routed to it. Workers use manual partition `assign()`, not consumer
groups — no rebalancing can move a partition's books away from the thread that
owns them mid-run.

Kafka partitioning therefore provides *both* guarantees at once:

- **Ordering** — all commands for a symbol land in one partition, which is a total
  order; the single owning thread applies them in that order.
- **Parallelism** — different partitions are processed by different threads with
  zero shared state, so throughput scales by adding partitions.

No lock guards any book anywhere. (The session-2.5 demo used a `synchronized`
block to serialize HTTP handlers onto the book — that lock's job is now done by
partition ownership.)

## Consequences

- The unit of parallelism is the partition, so a hot symbol cannot be split across
  threads: throughput per symbol is bounded by one core. This is true of real
  venues too (an instrument's book is inherently serial); mitigations are partition
  count and symbol-to-partition assignment.
- A worker crash stalls only its partitions; others keep matching.
- Book state is confined to one thread, so `OrderBook` stays free of any
  concurrency machinery and remains trivially testable.
