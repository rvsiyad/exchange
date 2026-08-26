# ADR 0002: TigerBeetle for money, Postgres for everything else

Status: accepted (session 4) — draft, refine after teach-back

## Context

The venue stores two very different kinds of state. Money movements need hard
invariants (no overdrafts, double-entry consistency, atomic multi-leg trades) at
high write rates. Everything else — users, order metadata, idempotency keys —
is ordinary relational data with ordinary access patterns.

## Decision

Split by nature of the data, not by service:

- **TigerBeetle** holds accounts and transfers — everything that is money.
  It is a purpose-built double-entry ledger: balance invariants
  (`debits_must_not_exceed_credits`) are enforced inside the database's state
  machine, two-phase transfers give reserve/post/void as first-class verbs,
  and linked events give multi-leg atomicity without a transaction manager.
  Amounts-only by design: it stores no strings, no metadata, no queries
  beyond account/transfer lookup and filtering.
- **Postgres** holds what TigerBeetle refuses to: user records, order
  metadata, and (upcoming) the gateway's idempotency keys, where a unique
  constraint is exactly the right tool. As of session 4 it runs in compose
  with only its schema's future occupants sketched; the split is the
  decision, the tables arrive with their features.

The bridge between the two worlds is naming: TigerBeetle's 128-bit ids are
derived deterministically (SHA-256) from the domain's string ids —
`account:alice:USD`, `reserve:o-123:0`, `post:ETH-USD-7:quote` — so any
service can address any account or transfer without a lookup table, and
retries collapse into id uniqueness.

## The alternative we deliberately did not choose

One Postgres for everything, invariants enforced by application code inside
SERIALIZABLE
transactions (or by check constraints and triggers). It works, and knowing how
to build it matters. But every invariant the application enforces is an
invariant every future code path can forget to enforce. Moving the invariant
into the database's own state machine removes the entire class of bug — and a
ledger database designed around that idea also removes the row-lock contention
a hot account creates in a general-purpose store.

## Consequences

- No code anywhere checks a balance; services *attempt* transfers and handle
  refusal. The overdraft check cannot be raced, bypassed, or forgotten.
- Trades settle atomically across four accounts with no distributed
  transaction, saga, or outbox — a linked chain is the whole mechanism.
- The cost: two stores to run, and TigerBeetle's austerity means anything
  string-shaped must live elsewhere and be joined by derived id.
- Single-replica TigerBeetle here is a dev topology; production runs a
  6-replica cluster with consensus, which changes operations but not this
  API.
