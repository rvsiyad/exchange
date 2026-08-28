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
    void counterTotalSumsAcrossLabels() {
        Metrics.counter("cmds_total", "commands", "partition", "0").add(3);
        Metrics.counter("cmds_total", "commands", "partition", "1").add(4);
        assertEquals(7, Metrics.counterTotal("cmds_total"));
        assertEquals(0, Metrics.counterTotal("never_registered_total"));
    }

    @Test
    void aNameCannotBeBothCounterAndGauge() {
        Metrics.counter("x_total", "x");
        assertThrows(IllegalArgumentException.class, () -> Metrics.gauge("x_total", "x"));
        assertThrows(IllegalArgumentException.class, () -> Metrics.histogram("x_total", "x"));
    }

    @Test
    void histogramsExportQuantilesSumAndCount() {
        var latency = Metrics.histogram("settle_seconds", "settle latency");
        for (int i = 1; i <= 1000; i++) {
            latency.observeNanos(i * 1_000_000L);   // 1ms .. 1000ms, uniform
        }

        var scrape = Metrics.scrape();
        assertTrue(scrape.contains("# TYPE settle_seconds summary"), scrape);
        // HdrHistogram keeps 3 significant digits, so quantiles land within
        // 0.1% of the exact answer for this uniform distribution.
        assertNear(0.500, scraped(scrape, "settle_seconds{quantile=\"0.5\"}"));
        assertNear(0.990, scraped(scrape, "settle_seconds{quantile=\"0.99\"}"));
        assertNear(0.999, scraped(scrape, "settle_seconds{quantile=\"0.999\"}"));
        assertNear(500.5, scraped(scrape, "settle_seconds_sum"));
        assertEquals(1000, (long) scraped(scrape, "settle_seconds_count"));
    }

    @Test
    void histogramQuantilesMergeIntoExistingLabels() {
        Metrics.histogram("match_seconds", "match latency", "symbol", "ETH-USD").observeNanos(1_000_000);
        assertTrue(Metrics.scrape().contains("match_seconds{symbol=\"ETH-USD\",quantile=\"0.5\"}"),
                Metrics.scrape());
    }

    private static void assertNear(double expected, double actual) {
        assertTrue(Math.abs(actual - expected) <= expected * 0.005,
                "expected ~" + expected + " but scraped " + actual);
    }

    private static double scraped(String scrape, String prefix) {
        for (var line : scrape.split("\n")) {
            if (line.startsWith(prefix + " ")) {
                return Double.parseDouble(line.substring(prefix.length() + 1));
            }
        }
        throw new AssertionError("no series " + prefix + " in:\n" + scrape);
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
