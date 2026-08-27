package dev.rvsiyad.exchange.common;

import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

/**
 * The /metrics endpoint every service exposes for Prometheus, on its own port
 * so scraping never competes with (or depends on) the service's real API.
 * The compose Prometheus scrapes gateway:7001, engine:7002, settlement:7003
 * and market-data:7004 — see infra/prometheus/prometheus.yml.
 */
public final class MetricsServer implements AutoCloseable {

    private static final String CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8";

    private HttpServer server;

    /** Starts on the given port (0 picks a free one) and returns the actual port. */
    public int start(int port) throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/metrics", exchange -> {
            var body = Metrics.scrape().getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", CONTENT_TYPE);
            exchange.sendResponseHeaders(200, body.length);
            try (var out = exchange.getResponseBody()) {
                out.write(body);
            }
        });
        server.start();
        return server.getAddress().getPort();
    }

    @Override
    public void close() {
        if (server != null) {
            server.stop(0);
        }
    }
}
