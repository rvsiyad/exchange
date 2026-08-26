# ADR 0005: Fanout backpressure — bounded queues, then eviction

Status: accepted (session 5) — draft, refine after teach-back

## Context

The market-data service fans one stream out to N WebSocket clients of wildly
unequal speed: a colocated dashboard drains messages in microseconds while a
laptop on hotel wifi drains them at whatever TCP decides. A socket write to a
client whose buffers are full *blocks*. If the thread doing that write is the
Kafka consumer thread, one slow client stalls the feed for everyone — the
classic fanout failure, and the question every streaming system design
interview turns on: **what happens when one consumer can't keep up?**

Whatever the answer, it must not be "the server buffers forever": memory per
client must be bounded, or one stuck TCP connection can take the process down.

## Decision

Each client gets a **bounded queue** (256 messages) drained by its own writer
thread. Producers hand off with a non-blocking offer and never touch a socket,
so the feed's pace is independent of every client's link. When a client's
queue fills, the client is **evicted** — closed on the spot, logged, forgotten.

Eviction is safe because the feed speaks **snapshot+delta**: a reconnecting
client receives a full snapshot before any deltas, atomically with its
subscription, so there is no state it can permanently miss. Eviction converts
"slow consumer" from a server-memory problem into a client-reconnect problem.

## Alternatives considered

- **Block the producer** (what the naive code does): one bad link stalls every
  client. Rejected — this is the failure, not a policy.
- **Unbounded queue:** memory grows at feed rate × client slowness, forever.
  A single stuck client OOMs the server. Never acceptable in fanout.
- **Drop oldest on overflow:** bounded, but silently corrupting — our deltas
  carry the absolute quantity *per price level*, so a dropped delta leaves that
  level stale until it next changes, with no signal to the client that it
  happened. A gap the client can't detect is worse than a disconnect it can.
- **Conflation** — coalesce queued updates per price level so a slow client
  skips intermediate states but always lands on the latest: the
  professional-feed answer (and how real vendors serve retail links), kept as
  future work because it needs a keyed queue per client, and eviction already
  makes the demo safe. Conflation composes with eviction; it doesn't replace
  the bound.

## Consequences

- Server memory is O(clients × 256 messages) worst case, independent of client
  behaviour.
- A slow client sees a clean disconnect (then self-heals by reconnecting) —
  never a silently wrong book.
- The tape's `sequence` field gives clients a gap detector if we ever want
  resume-from-sequence instead of resnapshot; not needed while snapshots are
  cheap.
