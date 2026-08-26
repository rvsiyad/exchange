# Learning log

Lab notebook for the Exchange build. One dated entry per session: what surprised me,
what broke, what I'd say in an interview. Raw material — no polish needed.

---

## 2026-08-26 · Session 4 — money: TigerBeetle settlement (PRs #16–#22)

**What went in:** asset registry + deterministic 128-bit ids (#16) → fills
carry limits and remainders (#17) → engine emits reservation releases on
cancel (#18) → the `ledger` module: accounts, pending reservations, linked
settlement chains (#19) → gateway reserves before publishing (#20) →
settlement service consumes the fill stream (#21) → these docs (#22).

**The one-sentence version:** the application never checks a balance — orders
*hold* their worst case as pending transfers at entry, fills *post* those holds
and re-reserve remainders in one atomic linked chain, cancels *void* them, and
the database refuses everything else.

**What surprised / what broke:**

- The check-then-act race isn't fixed by reservation so much as *deleted*:
  `exceeds_credits` is evaluated inside TigerBeetle's state machine, counting
  every other hold. There is nothing to race, in any code path, ever.
- "A pending transfer can be posted once, but an order fills many times" is
  the real design problem of the session. The answer — post generation *n* and
  create generation *n+1* for the remainder inside the same linked chain — took
  longer to see than to build, and it's the part I'd whiteboard in an
  interview.
- The one real bug: a *mid-stream duplicate* fill returned ALREADY_SETTLED
  (ledger fine) but still advanced settlement's generation projection a second
  time — the next fill then referenced a generation that didn't exist
  (`pending_transfer_not_found`). Fix: dedupe by fill id in the projection,
  exactly like the tape. Lesson: idempotency is needed at *every* layer that
  keeps state, not just the one that keeps money.
- `Exists` is a success. Deterministic ids turn TigerBeetle's id-uniqueness
  error into the idempotency mechanism — funding, reserving, settling, and
  voiding all treat "already happened" as "done".
- Testcontainers word-splits `withCommand(String)`, which silently shredded
  the `sh -c` script (`args=[-c, /tigerbeetle, format, ...]`); and the Java
  client's address parser wants `ip:port`, not `localhost:port`. Both cost
  more time than the settlement chain itself.
- Escrow is overdraft-protected too, which forces payout legs *after* post
  legs in the chain — linked transfers check balances in sequence, so escrow
  provably never pays out value it hasn't just received. Ordering as an
  invariant.

**Interview lines earned this session:**

- "Reservation beats check-then-debit: checking is holding, atomically, at the
  database" (ADR 0004).
- "Partial fills walk a generation chain: post n and re-reserve n+1 in one
  linked chain, so open funds are never unheld" (ADR 0004).
- "Push invariants into the database: an app-level check is a bug every future
  code path can reintroduce; a storage-level invariant can't be raced or
  forgotten" (ADR 0002).
- "Exactly-once settlement is deterministic transfer ids + a ledger that
  dedupes by id — no bookkeeping table anywhere."

**Check-yourself questions for the teach-back:**

1. Two orders from the same user race for the same dollars. Walk through why
   no interleaving of gateway threads can over-commit the account.
2. A buy at limit $21 fills 2 of 5 at $20. Exactly which transfers are in the
   settlement chain, with which amounts, and what got released vs re-held?
3. Why must the release event share the fills topic (and key) instead of
   having its own topic? What goes wrong on separate topics?
4. Why does settlement's generation counter dedupe by fill id when the ledger
   already returns ALREADY_SETTLED for duplicates?

---

## 2026-08-26 · Session 3 — event sourcing over Kafka (PRs #9–#15)

**What went in:** book emits depth deltas (#9) → engine consumer loop, one thread
per partition (#10) → gateway REST → log (#11) → snapshot + replay (#12) →
market-data projection + demo UI on the real pipeline (#13) → in-process demo
retired (#14) → these docs (#15).

**The one-sentence version:** the log became the database — the book lives in
memory, durability is `orders` + snapshots, and every consumer is idempotent
because replay *will* re-send things.

**What surprised / what broke:**

- Docker 29 dropped Docker API < 1.44, which broke Testcontainers on this machine
  with an inscrutable 400-with-empty-body from `/info`. Fix: `api.version=1.44`
  in `~/.docker-java.properties`. CI (older Docker) never saw it.
- SIGKILLing the engine before any snapshot exists is the best possible demo of
  the fallback path: "no snapshot, replaying from the beginning" ×4 in the logs,
  then the book comes back and the backlog matches. Ran it live: placed an order
  *while the engine was dead* (gateway 202 — it's in the log), restarted, and the
  fill appeared. The replayed old fill was deduped by the tape — both idempotency
  layers visible in one screenshot.
- The offset question is the whole ballgame: committing offsets to Kafka would be
  *wrong* here, because the offset must travel atomically with the book state it
  describes. Once you see that, snapshots-own-offsets is obvious.
- 202 vs 200 is a real API-design decision, not pedantry: the gateway can no
  longer tell you your fills synchronously, because it genuinely doesn't know.
  The UI had to change from "read fills from the response" to "watch the tape".

**Interview lines earned this session:**

- "Ordering and parallelism both come from partitioning; there are no locks in
  the matching path" (ADR 0001).
- "Exactly-once delivery is a myth; exactly-once *effect* is deterministic ids +
  idempotent consumers" (ADR 0003).
- "Recovery is load-snapshot, seek, replay; corrupt snapshot degrades to slower,
  never to wrong."

**Check-yourself questions for the teach-back:**

1. Why would committing consumer offsets to Kafka lose orders, concretely —
   what's the failure sequence?
2. A buy sweeps two ask levels and partially rests. Which BookUpdates get
   emitted, and why absolute quantities instead of deltas?
3. Why does the engine flush fills *before* writing a snapshot, and what could a
   consumer observe if it didn't?
4. Why manual `assign()` instead of a consumer group for the engine — and why
   would losing market-data's position (it also never commits) not matter at all?

