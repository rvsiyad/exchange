package dev.rvsiyad.exchange.engine;

import dev.rvsiyad.exchange.common.Env;

import java.nio.file.Path;

public class EngineMain {

    public static void main(String[] args) throws Exception {
        var engine = new Engine(
                Env.get("KAFKA_BOOTSTRAP", "localhost:9092"),
                Path.of(Env.get("ENGINE_SNAPSHOT_DIR", "snapshots")),
                Integer.parseInt(Env.get("ENGINE_SNAPSHOT_EVERY", "1000")));
        engine.start();
        Runtime.getRuntime().addShutdownHook(new Thread(engine::close, "engine-shutdown"));
        Thread.currentThread().join();
    }
}
