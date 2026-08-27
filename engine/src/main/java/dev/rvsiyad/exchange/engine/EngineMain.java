package dev.rvsiyad.exchange.engine;

import dev.rvsiyad.exchange.common.Env;
import dev.rvsiyad.exchange.common.MetricsServer;

import java.nio.file.Path;

public class EngineMain {

    public static void main(String[] args) throws Exception {
        var metrics = new MetricsServer();
        metrics.start(Integer.parseInt(Env.get("ENGINE_METRICS_PORT", "7002")));

        var engine = new Engine(
                Env.get("KAFKA_BOOTSTRAP", "localhost:9092"),
                Path.of(Env.get("ENGINE_SNAPSHOT_DIR", "snapshots")),
                Integer.parseInt(Env.get("ENGINE_SNAPSHOT_EVERY", "1000")));
        engine.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            engine.close();
            metrics.close();
        }, "engine-shutdown"));
        Thread.currentThread().join();
    }
}
