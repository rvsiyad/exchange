package dev.rvsiyad.exchange.engine;

import dev.rvsiyad.exchange.common.Env;

public class EngineMain {

    public static void main(String[] args) throws Exception {
        var engine = new Engine(Env.get("KAFKA_BOOTSTRAP", "localhost:9092"));
        engine.start();
        Runtime.getRuntime().addShutdownHook(new Thread(engine::close, "engine-shutdown"));
        Thread.currentThread().join();
    }
}
