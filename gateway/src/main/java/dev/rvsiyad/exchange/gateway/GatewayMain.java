package dev.rvsiyad.exchange.gateway;

import dev.rvsiyad.exchange.common.Env;

public class GatewayMain {

    public static void main(String[] args) throws Exception {
        var gateway = new GatewayServer(Env.get("KAFKA_BOOTSTRAP", "localhost:9092"));
        gateway.start(Integer.parseInt(Env.get("GATEWAY_PORT", "8091")));
        Runtime.getRuntime().addShutdownHook(new Thread(gateway::close, "gateway-shutdown"));
        Thread.currentThread().join();
    }
}
