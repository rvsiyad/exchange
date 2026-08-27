package dev.rvsiyad.exchange.marketdata;

import dev.rvsiyad.exchange.common.Env;
import dev.rvsiyad.exchange.common.MetricsServer;

import java.net.URI;

public class MarketDataMain {

    public static void main(String[] args) throws Exception {
        var metrics = new MetricsServer();
        metrics.start(Integer.parseInt(Env.get("MARKET_DATA_METRICS_PORT", "7004")));

        var server = new MarketDataServer(
                Env.get("KAFKA_BOOTSTRAP", "localhost:9092"),
                URI.create(Env.get("GATEWAY_URL", "http://localhost:8091")));
        server.start(
                Integer.parseInt(Env.get("MARKET_DATA_PORT", "8090")),
                Integer.parseInt(Env.get("MARKET_DATA_WS_PORT", "8092")));
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.close();
            metrics.close();
        }, "market-data-shutdown"));
        Thread.currentThread().join();
    }
}
