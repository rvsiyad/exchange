# Learning log

Lab notebook for the Exchange build. One dated entry per session: what surprised me,
what broke, what I'd say in an interview. Raw material — no polish needed.

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

