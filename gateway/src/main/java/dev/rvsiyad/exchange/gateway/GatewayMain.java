package dev.rvsiyad.exchange.gateway;

import dev.rvsiyad.exchange.common.Env;
import dev.rvsiyad.exchange.common.MetricsServer;
import dev.rvsiyad.exchange.ledger.Ledger;

public class GatewayMain {

    public static void main(String[] args) throws Exception {
        var metrics = new MetricsServer();
        metrics.start(Integer.parseInt(Env.get("GATEWAY_METRICS_PORT", "7001")));

        var ledger = new Ledger(Env.get("TIGERBEETLE_ADDRESS", "3000"));
        ledger.ensureVenueAccounts();
        // Demo money. Funding ids are deterministic, so restarts never double-fund.
        for (var user : new String[]{"alice", "bob"}) {
            ledger.ensureUserAccounts(user);
            ledger.fund(user, "USD", 1_000_000_00);
            ledger.fund(user, "BTC", 10);
            ledger.fund(user, "ETH", 100);
            ledger.fund(user, "SOL", 1_000);
        }

        var gateway = new GatewayServer(Env.get("KAFKA_BOOTSTRAP", "localhost:9092"), ledger);
        gateway.start(Integer.parseInt(Env.get("GATEWAY_PORT", "8091")));
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            gateway.close();
            metrics.close();
        }, "gateway-shutdown"));
        Thread.currentThread().join();
    }
}
