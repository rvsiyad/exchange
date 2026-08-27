package dev.rvsiyad.exchange.marketdata;

import com.sun.net.httpserver.HttpServer;
import dev.rvsiyad.exchange.common.BookUpdate;
import dev.rvsiyad.exchange.common.Fill;
import dev.rvsiyad.exchange.common.Json;
import dev.rvsiyad.exchange.common.Metrics;
import dev.rvsiyad.exchange.common.ReservationRelease;
import dev.rvsiyad.exchange.common.Side;
import dev.rvsiyad.exchange.common.Topics;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.redpanda.RedpandaContainer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.WebSocket;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class MarketDataServerTest {

    @Container
    static final RedpandaContainer REDPANDA = new RedpandaContainer("redpandadata/redpanda:v24.2.7");

    static MarketDataServer marketData;
    static String baseUrl;
    static String wsUrl;
    static HttpServer fakeGateway;
    static final AtomicReference<String> gatewayRequestBody = new AtomicReference<>();
    static final AtomicReference<String> gatewayBalancesQuery = new AtomicReference<>();
    static final HttpClient http = HttpClient.newHttpClient();

    @BeforeAll
    static void startEverything() throws Exception {
        var bootstrap = REDPANDA.getBootstrapServers();
        try (var admin = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap))) {
            admin.createTopics(List.of(
                    new NewTopic(Topics.FILLS, 1, (short) 1),
                    new NewTopic(Topics.BOOK_UPDATES, 1, (short) 1))).all().get();
        }
        fakeGateway = startFakeGateway();
        marketData = new MarketDataServer(
                bootstrap, URI.create("http://localhost:" + fakeGateway.getAddress().getPort()));
        var server = marketData.start(0, 0);
        baseUrl = "http://localhost:" + server.getAddress().getPort();
        wsUrl = "ws://localhost:" + marketData.webSocketPort();
    }

    @AfterAll
    static void stopEverything() {
        marketData.close();
        fakeGateway.stop(0);
    }

    @Test
    void projectsBookAndDedupedTapeFromTheStreams() throws Exception {
        try (var producer = newProducer()) {
            update(producer, new BookUpdate("BTC-USD", Side.SELL, 101_00, 5, 1));
            update(producer, new BookUpdate("BTC-USD", Side.BUY, 100_00, 3, 2));
            update(producer, new BookUpdate("BTC-USD", Side.SELL, 101_00, 2, 3));   // absolute qty: idempotent
            update(producer, new BookUpdate("BTC-USD", Side.BUY, 100_00, 0, 4));    // level gone
            var fill = new Fill("BTC-USD-1", "BTC-USD", "b1", "s1", "alice", "bob", Side.BUY, 101_00, 3, 101_00, 0, 0, 5);
            fill(producer, fill);
            fill(producer, fill);                                                   // engine replay: same fill id
            // A cancel's reservation release shares the topic; it must never hit the tape.
            producer.send(new ProducerRecord<>(Topics.FILLS, "BTC-USD",
                    Json.toBytes(new ReservationRelease("s2", "carol", "BTC-USD", Side.SELL, 102_00, 4, 6))));
        }

        awaitBook(s -> !s.trades().isEmpty() && s.bids().isEmpty()
                && s.asks().size() == 1 && s.asks().get(0).quantity() == 2);
        Thread.sleep(500);   // settle window: the duplicate fill has had time to arrive
        var snapshot = fetchBook();
        assertEquals(List.of(), snapshot.bids());
        assertEquals(101_00, snapshot.asks().get(0).priceTicks());
        assertEquals(2, snapshot.asks().get(0).quantity());
        assertEquals(1, snapshot.trades().size(), "duplicate fill must not double the tape");
        assertEquals(3, snapshot.trades().get(0).quantity());
        assertEquals(Side.BUY, snapshot.trades().get(0).takerSide());
    }

    @Test
    void streamsSnapshotThenDeltasOverTheWebSocket() throws Exception {
        var messages = new LinkedBlockingQueue<String>();
        // The JDK ships a WebSocket *client*; pointing it at our hand-rolled
        // server means an independent implementation validates the handshake
        // and framing, not just our own code round-tripping with itself.
        var ws = http.newWebSocketBuilder()
                .buildAsync(URI.create(wsUrl + "/ws?symbol=ETH-USD"), collectInto(messages))
                .get(10, TimeUnit.SECONDS);
        try {
            var snapshot = Json.tree(take(messages).getBytes(StandardCharsets.UTF_8));
            assertEquals("snapshot", snapshot.get("type").asText());
            assertEquals("ETH-USD", snapshot.get("book").get("symbol").asText());
            assertTrue(snapshot.get("book").get("bids").isEmpty(), "nothing traded on ETH-USD yet");

            try (var producer = newProducer()) {
                update(producer, new BookUpdate("ETH-USD", Side.BUY, 50_00, 7, 10));
                fill(producer, new Fill("ETH-USD-1", "ETH-USD", "b9", "s9", "alice", "bob",
                        Side.SELL, 50_00, 2, 50_00, 0, 0, 11));
            }

            // book-updates and fills are separate topics, so cross-topic
            // arrival order is not guaranteed — match by type, not position.
            var received = new HashMap<String, com.fasterxml.jackson.databind.JsonNode>();
            for (var i = 0; i < 2; i++) {
                var message = Json.tree(take(messages).getBytes(StandardCharsets.UTF_8));
                received.put(message.get("type").asText(), message);
            }
            assertEquals(Set.of("book", "trade"), received.keySet());
            assertEquals(7, received.get("book").get("update").get("newQuantity").asLong());
            assertEquals(2, received.get("trade").get("trade").get("quantity").asLong());

            // The fanout counted the client and what it pushed to it.
            var scrape = Metrics.scrape();
            assertTrue(scrape.contains("marketdata_clients 1"), scrape);
            assertTrue(Metrics.counterTotal("marketdata_messages_total") >= 2,
                    "expected at least the two deltas counted");
        } finally {
            ws.abort();
        }
    }

    @Test
    void proxiesOrderEntryToTheGateway() throws Exception {
        var body = "{\"userId\":\"alice\",\"symbol\":\"BTC-USD\",\"side\":\"BUY\",\"priceTicks\":10000,\"quantity\":1}";
        var response = http.send(
                HttpRequest.newBuilder(URI.create(baseUrl + "/api/orders"))
                        .POST(HttpRequest.BodyPublishers.ofString(body))
                        .build(),
                HttpResponse.BodyHandlers.ofString());

        assertEquals(202, response.statusCode());
        assertTrue(response.body().contains("o-fake"));
        assertEquals(body, gatewayRequestBody.get(), "request body must reach the gateway unchanged");
    }

    @Test
    void servesTheDemoPage() throws Exception {
        var response = http.send(
                HttpRequest.newBuilder(URI.create(baseUrl + "/")).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());
        assertTrue(response.body().contains("Order ticket"));
        assertTrue(response.body().contains("Balances"));
    }

    @Test
    void servesTheWebSocketPortAsConfig() throws Exception {
        var response = http.send(
                HttpRequest.newBuilder(URI.create(baseUrl + "/api/config")).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());
        assertEquals(marketData.webSocketPort(),
                Json.tree(response.body().getBytes(StandardCharsets.UTF_8)).get("webSocketPort").asInt());
    }

    @Test
    void proxiesBalancesToTheGatewayWithTheQueryString() throws Exception {
        var response = http.send(
                HttpRequest.newBuilder(URI.create(baseUrl + "/api/balances?userId=alice")).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());
        assertTrue(response.body().contains("USD"));
        assertEquals("userId=alice", gatewayBalancesQuery.get(), "query string must reach the gateway");
    }

    private static String take(LinkedBlockingQueue<String> messages) throws InterruptedException {
        var message = messages.poll(15, TimeUnit.SECONDS);
        assertNotNull(message, "timed out waiting for a websocket message");
        return message;
    }

    /** The JDK client delivers text in chunks; reassemble whole messages into the queue. */
    private static WebSocket.Listener collectInto(LinkedBlockingQueue<String> messages) {
        return new WebSocket.Listener() {
            final StringBuilder partial = new StringBuilder();

            @Override
            public CompletionStage<?> onText(WebSocket ws, CharSequence data, boolean last) {
                partial.append(data);
                if (last) {
                    messages.add(partial.toString());
                    partial.setLength(0);
                }
                ws.request(1);
                return null;
            }
        };
    }

    private static HttpServer startFakeGateway() throws IOException {
        var server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/api/orders", exchange -> {
            gatewayRequestBody.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            var out = "{\"orderId\":\"o-fake\"}".getBytes();
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(202, out.length);
            try (var os = exchange.getResponseBody()) {
                os.write(out);
            }
        });
        server.createContext("/api/balances", exchange -> {
            gatewayBalancesQuery.set(exchange.getRequestURI().getRawQuery());
            var out = "{\"userId\":\"alice\",\"balances\":[{\"asset\":\"USD\",\"total\":100,\"reserved\":0,\"available\":100}]}"
                    .getBytes();
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, out.length);
            try (var os = exchange.getResponseBody()) {
                os.write(out);
            }
        });
        server.start();
        return server;
    }

    private static KafkaProducer<String, byte[]> newProducer() {
        var config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, REDPANDA.getBootstrapServers());
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        return new KafkaProducer<>(config);
    }

    private static void update(KafkaProducer<String, byte[]> producer, BookUpdate update) {
        producer.send(new ProducerRecord<>(Topics.BOOK_UPDATES, update.symbol(), Json.toBytes(update)));
    }

    private static void fill(KafkaProducer<String, byte[]> producer, Fill fill) {
        producer.send(new ProducerRecord<>(Topics.FILLS, fill.symbol(), Json.toBytes(fill)));
    }

    private static MarketDataServer.BookSnapshot fetchBook() throws Exception {
        var body = http.send(
                HttpRequest.newBuilder(URI.create(baseUrl + "/api/book?symbol=BTC-USD")).GET().build(),
                HttpResponse.BodyHandlers.ofString()).body();
        return Json.fromBytes(body.getBytes(StandardCharsets.UTF_8), MarketDataServer.BookSnapshot.class);
    }

    /** Polls /api/book until the projection reflects everything produced (or times out and fails later asserts). */
    private static void awaitBook(java.util.function.Predicate<MarketDataServer.BookSnapshot> ready) throws Exception {
        var deadline = Instant.now().plusSeconds(30);
        while (Instant.now().isBefore(deadline) && !ready.test(fetchBook())) {
            Thread.sleep(100);
        }
    }
}
