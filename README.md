# exchange

[![CI](https://github.com/rvsiyad/exchange/actions/workflows/ci.yml/badge.svg)](https://github.com/rvsiyad/exchange/actions/workflows/ci.yml)

![Demo: orders resting, crossing the spread, and sweeping two price levels](docs/demo.gif)

A mini trading venue. Clients place orders through a REST gateway, an event-sourced
in-memory matching engine crosses them over Kafka, fills settle atomically as
two-phase transfers in TigerBeetle, and a market-data service streams the live
order book to a dashboard over WebSockets.

## Demo

The demo UI runs on the real event-sourced pipeline: orders enter through the
gateway, cross in the engine, and the book you see is projected back out of the
log by market-data — **gateway → Kafka → engine → Kafka → market-data**. (The
session-2.5 walking skeleton ran the same UI directly on the in-process matcher;
the guts were swapped without changing the UI contract.)

```
docker compose up -d redpanda redpanda-init tigerbeetle
./mvnw -q install -DskipTests
./mvnw -q -pl engine exec:java &
./mvnw -q -pl settlement exec:java &
./mvnw -q -pl gateway exec:java &
./mvnw -q -pl market-data exec:java
```

Then open http://localhost:8090 and trade as the funded demo users `alice` and
`bob` (balances at `GET :8091/api/balances?userId=alice`). Orders reserve their
worst-case cost in TigerBeetle before they reach the log — an order you cannot
afford is rejected by the ledger database itself with a 422. Ports/config via
`KAFKA_BOOTSTRAP`, `TIGERBEETLE_ADDRESS` (3000), `GATEWAY_PORT` (8091),
`MARKET_DATA_PORT` (8090), `MARKET_DATA_WS_PORT` (8092), `GATEWAY_URL`,
`ENGINE_SNAPSHOT_DIR`, `ENGINE_SNAPSHOT_EVERY`.

The page rides the WebSocket feed (snapshot on connect, deltas after): the
book, tape, and TigerBeetle balances update live, and a dropped or evicted
connection self-heals by reconnecting for a fresh snapshot
([ADR 0005](docs/adr/0005-fanout-backpressure.md)).

## Design

Matching happens in memory on purpose — putting a database in the matching hot path
is an anti-pattern; durability comes from the Kafka event log plus snapshots, the
same architecture real exchanges use. Money correctness is delegated to a
purpose-built ledger database that enforces balance invariants itself: orders hold
their worst case as TigerBeetle pending transfers at entry, fills settle as one
atomic linked chain (post both holds, re-reserve remainders, pay out of escrow),
cancels void — and no application code ever checks a balance. Built in Java 21 as
a Maven multi-module monorepo.

Decisions are written up as ADRs in [docs/adr](docs/adr); the running lab
notebook is [docs/LEARNING.md](docs/LEARNING.md).
