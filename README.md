# exchange

[![CI](https://github.com/rvsiyad/exchange/actions/workflows/ci.yml/badge.svg)](https://github.com/rvsiyad/exchange/actions/workflows/ci.yml)

A mini trading venue. Clients place orders through a REST gateway, an event-sourced
in-memory matching engine crosses them over Kafka, fills settle atomically as
two-phase transfers in TigerBeetle, and a market-data service streams the live
order book to a dashboard over WebSockets.

## Demo

A walking-skeleton demo serves a crude web UI directly on the real matching engine
in a single process — no Kafka or TigerBeetle yet; later sessions swap those in
behind the same UI contract:

```
./mvnw -pl engine -am compile
./mvnw -pl engine exec:java -Dexec.mainClass=dev.rvsiyad.exchange.engine.demo.DemoServerMain
```

Then open http://localhost:8090 (port configurable via `DEMO_PORT`).

## Design

Matching happens in memory on purpose — putting a database in the matching hot path
is an anti-pattern; durability comes from the Kafka event log plus snapshots, the
same architecture real exchanges use. Money correctness is delegated to a
purpose-built ledger database that enforces balance invariants itself. Built in
Java 21 as a Maven multi-module monorepo.
