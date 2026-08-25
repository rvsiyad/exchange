# ADR 0003: Event-sourced engine (the log is the database)

Status: accepted (session 3) — draft, refine after teach-back

## Context

The book must live in memory — a database in the matching hot path is an
anti-pattern no real venue tolerates. But memory dies with the process, and an
exchange cannot lose resting orders.

## Decision

The `orders` topic is the engine's write-ahead log and source of truth. The
gateway's 202 means "durably appended to the log" (`acks=all`), not "matched".
Matching state is a pure, deterministic function of the log: replaying the same
commands into a fresh book rebuilds byte-identical state *and* byte-identical
fills (fill ids come from a per-book sequence, timestamps from the taker command).

Recovery is therefore: load the latest **snapshot** (all books for a partition +
the next offset to consume, one atomically-replaced JSON file per partition,
written every N commands and on graceful stop), `seek()` to that offset, and
replay the tail. No snapshot — or a corrupt one — just means replaying from the
beginning: slower, never wrong.

Two placement rules keep this correct:

- **The offset lives inside the snapshot**, never in Kafka's consumer offsets. A
  committed offset without the book state it belongs to would silently drop
  orders on restart; state and position must move atomically.
- **Fills are flushed before the snapshot is written**, so a snapshot never
  claims an offset whose fills might still be sitting in a producer buffer.

## Delivery semantics: at-least-once + idempotency, not exactly-once delivery

Replaying the tail re-emits fills the world may have already seen. We embrace
at-least-once delivery everywhere and make *effects* exactly-once at consumers:

- fills carry a deterministic id, so any consumer (market-data's tape today,
  TigerBeetle transfer ids in session 4) dedupes replays for free;
- book-update deltas carry the *absolute* new quantity of a level, so applying
  one twice is harmless by construction;
- the book itself ignores redelivered commands (seen order-id set).

We deliberately did not reach for Kafka transactions/EOS: idempotency is needed
anyway (any consumer can crash between side effect and offset commit), and once
you have it, EOS buys complexity, not correctness.

## Consequences

- Kill-and-recover works and is demonstrable: SIGKILL the engine, place orders
  while it is dead (gateway still 202s them into the log), restart — the book is
  rebuilt, the backlog is matched, and deduping consumers show no duplicates.
- Replay time is bounded by snapshot frequency (`ENGINE_SNAPSHOT_EVERY`), a
  straightforward recovery-time vs write-amplification dial.
- Determinism is now a hard contract on `OrderBook`: no clocks, no randomness,
  no iteration-order dependence may ever creep into matching.
