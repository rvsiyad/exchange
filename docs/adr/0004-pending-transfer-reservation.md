# ADR 0004: Pending-transfer reservation (reserve, post, void)

Status: accepted (session 4) — draft, refine after teach-back

## Context

An order is a promise to pay. The naive implementation — check the balance,
then debit it at fill time — has two fatal flaws. First, check-then-debit is a
race: two orders can both pass the check against the same dollars. Second, the
debit happens long after the check, and the money may be gone by then. A real
venue must know at accept time that the order can always settle.

## Decision

Funds are **reserved at order entry** as a TigerBeetle *pending transfer* from
the user's asset account into the venue's escrow account, sized at the order's
worst case: a buy holds `limit x quantity` of the quote asset, a sell holds the
base quantity. The hold itself is atomic — TigerBeetle either records it or
rejects it with `exceeds_credits`, counting every other open order's hold in
the same operation. There is no window between checking and holding because
checking *is* holding.

The rest of the order lifecycle maps onto the two-phase transfer verbs:

- **Fill → post.** Settlement posts the pending transfer for the amount the
  fill actually traded (`price x quantity`, at the maker's price). Posting a
  smaller amount than was held automatically releases the difference — price
  improvement refunds itself with no code.
- **Partial fill → post + re-reserve, atomically.** A pending transfer can be
  posted once, but an order can fill many times. So reservations are numbered
  per order (*generations*): the gateway creates generation 0, and each fill's
  settlement chain posts generation *n* and creates generation *n+1* for
  `limit x remaining` — inside the same linked chain. Because the chain is
  atomic, there is no instant at which an open order's funds are unheld.
- **Cancel → void.** The engine emits a release only when a cancel actually
  removed a resting order; settlement voids the current generation. The hold
  evaporates; money never moved.

Both legs of a trade — buyer's quote, seller's base, and the two escrow
payouts — ride one **linked chain**, applied all-or-nothing. Escrow accounts
are overdraft-protected like user accounts, and payouts are ordered after
posts because linked transfers check balances in sequence: escrow can never
pay out value it has not just received.

## Why the database, not the application

`debits_must_not_exceed_credits` on every user and escrow account means the
overdraft check lives at the storage layer. This is categorically stronger
than an application check: there is no code path — no retry, no race, no
future bug — that can slip a debit past it, because the invariant is evaluated
inside the same state machine that applies the transfer. The application's job
reduces to *asking*; the database's job is *refusing*.

## Consequences

- Insufficient funds is decided before an order ever reaches the book; the
  matching engine has no concept of money at all.
- Settlement is exactly-once by construction: every transfer id derives
  deterministically from the fill or order id, so a redelivered fill's whole
  chain collapses into id uniqueness as a no-op.
- The reservation generation counter is projection state in settlement,
  rebuilt by replay; it advances in lock-step with the chains it names and is
  deduped per fill id (a mid-stream duplicate must not advance it twice — a
  real bug the tests caught).
- Worst-case sizing over-reserves for buys that fill with price improvement;
  the refund arrives at settlement, not before. This is the standard trade-off
  and the honest price of "an accepted order can always settle".
