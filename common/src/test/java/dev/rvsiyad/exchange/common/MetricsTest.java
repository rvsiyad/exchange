package dev.rvsiyad.exchange.common;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MetricsTest {

    @BeforeEach
    void clearRegistry() {
        Metrics.clear();
    }

    @Test
    void registrationIsIdempotent() {
        var first = Metrics.counter("orders_total", "orders");
        first.increment();
        var second = Metrics.counter("orders_total", "orders");
        assertSame(first, second);
        assertEquals(1, second.value());
    }

    @Test
    void labelsSeparateSeriesWithinOneFamily() {
        Metrics.counter("rejected_total", "rejections", "reason", "bad_symbol").add(2);
        Metrics.counter("rejected_total", "rejections", "reason", "insufficient_funds").increment();

        var scrape = Metrics.scrape();
        assertTrue(scrape.contains("rejected_total{reason=\"bad_symbol\"} 2"), scrape);
        assertTrue(scrape.contains("rejected_total{reason=\"insufficient_funds\"} 1"), scrape);
        // HELP/TYPE render once per family, not once per series.
        assertEquals(1, scrape.split("# HELP rejected_total", -1).length - 1, scrape);
    }

    @Test
    void gaugesReadTheirCurrentValueAtScrapeTime() {
        var lag = Metrics.gauge("lag_seconds", "settlement lag");
        lag.set(0.25);
        assertTrue(Metrics.scrape().contains("lag_seconds 0.25"), Metrics.scrape());

        lag.set(3);
        assertTrue(Metrics.scrape().contains("lag_seconds 3\n"), Metrics.scrape());
    }

    @Test
    void computedGaugesTrackLiveState() {
        var clients = new java.util.ArrayList<String>();
        Metrics.gaugeOf("clients", "connected clients", clients::size);
        clients.add("a");
        clients.add("b");
        assertTrue(Metrics.scrape().contains("clients 2"), Metrics.scrape());
    }

    @Test
    void aNameCannotBeBothCounterAndGauge() {
        Metrics.counter("x_total", "x");
        assertThrows(IllegalArgumentException.class, () -> Metrics.gauge("x_total", "x"));
    }

    @Test
    void metricsServerServesTheExposition() throws Exception {
        Metrics.counter("served_total", "served").add(7);
        try (var server = new MetricsServer()) {
            int port = server.start(0);
            var response = HttpClient.newHttpClient().send(
                    HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/metrics")).build(),
                    HttpResponse.BodyHandlers.ofString());
            assertEquals(200, response.statusCode());
            assertTrue(response.headers().firstValue("Content-Type").orElse("").startsWith("text/plain"));
            assertTrue(response.body().contains("# TYPE served_total counter"), response.body());
            assertTrue(response.body().contains("served_total 7"), response.body());
        }
    }
}
