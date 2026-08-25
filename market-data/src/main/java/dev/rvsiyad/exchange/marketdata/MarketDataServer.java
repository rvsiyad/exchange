package dev.rvsiyad.exchange.marketdata;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import dev.rvsiyad.exchange.common.BookUpdate;
import dev.rvsiyad.exchange.common.Fill;
import dev.rvsiyad.exchange.common.Json;
import dev.rvsiyad.exchange.common.Side;
import dev.rvsiyad.exchange.common.Topics;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;

/**
 * Read-side of the exchange: rebuilds a live book per symbol from the
 * `book-updates` stream and a trade tape from `fills`, and serves both to the
 * demo page. All state is a projection of the log — restart just replays.
 *
 * Two idempotency lessons live here: book deltas carry the absolute new
 * quantity per level, so a redelivered update is harmless; fills are deduped
 * by their deterministic id, so the engine replaying its tail after a restart
 * never doubles the tape. At-least-once delivery, exactly-once effect.
 *
 * Order entry is proxied to the gateway so the page stays same-origin — the
 * UI contract from the session-2.5 demo is unchanged while the guts behind it
 * are now the full gateway → Kafka → engine → Kafka pipeline. Polling becomes
 * a WebSocket push in session 5.
 */
public final class MarketDataServer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(MarketDataServer.class);
    private static final int MAX_DEPTH_LEVELS = 10;
    private static final int MAX_RECENT_TRADES = 20;

    record Level(long priceTicks, long quantity) {
    }

    record Trade(long priceTicks, long quantity, Side takerSide, long sequence) {
    }

    record BookSnapshot(String symbol, List<Level> bids, List<Level> asks, List<Trade> trades) {
    }

    private static final class SymbolState {
        final NavigableMap<Long, Long> bids = new TreeMap<>(Comparator.reverseOrder());
        final NavigableMap<Long, Long> asks = new TreeMap<>();
        final Deque<Trade> trades = new ArrayDeque<>();
    }

    private final String bootstrapServers;
    private final URI gatewayUri;
    private final HttpClient gatewayClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();
    private final Map<String, SymbolState> symbols = new HashMap<>();
    private final Set<String> seenFillIds = new HashSet<>();
    private final Object lock = new Object();
    private final KafkaConsumer<String, byte[]> consumer;
    private final Thread consumerThread;
    private long tradeSequence;
    private volatile boolean running = true;
    private HttpServer server;

    public MarketDataServer(String bootstrapServers, URI gatewayUri) {
        this.bootstrapServers = bootstrapServers;
        this.gatewayUri = gatewayUri;
        this.consumer = new KafkaConsumer<>(consumerConfig());
        this.consumerThread = new Thread(this::consume, "market-data-consumer");
    }

    public HttpServer start(int port) throws IOException {
        consumerThread.start();
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/", this::servePage);
        server.createContext("/api/book", this::serveBook);
        server.createContext("/api/orders", exchange -> proxyToGateway(exchange, "/api/orders"));
        server.createContext("/api/cancel", exchange -> proxyToGateway(exchange, "/api/cancel"));
        server.start();
        log.info("market-data listening on port {}", server.getAddress().getPort());
        return server;
    }

    private void consume() {
        try {
            var partitions = new ArrayList<TopicPartition>();
            for (var topic : List.of(Topics.BOOK_UPDATES, Topics.FILLS)) {
                consumer.partitionsFor(topic)
                        .forEach(p -> partitions.add(new TopicPartition(topic, p.partition())));
            }
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            while (running) {
                for (var record : consumer.poll(Duration.ofMillis(250))) {
                    synchronized (lock) {
                        if (record.topic().equals(Topics.BOOK_UPDATES)) {
                            applyBookUpdate(Json.fromBytes(record.value(), BookUpdate.class));
                        } else {
                            applyFill(Json.fromBytes(record.value(), Fill.class));
                        }
                    }
                }
            }
        } catch (WakeupException e) {
            if (running) {
                throw e;
            }
        } finally {
            consumer.close();
        }
    }

    private void applyBookUpdate(BookUpdate update) {
        var state = symbols.computeIfAbsent(update.symbol(), s -> new SymbolState());
        var side = update.side() == Side.BUY ? state.bids : state.asks;
        if (update.newQuantity() == 0) {
            side.remove(update.priceTicks());
        } else {
            side.put(update.priceTicks(), update.newQuantity());
        }
    }

    private void applyFill(Fill fill) {
        if (!seenFillIds.add(fill.fillId())) {
            return;
        }
        var state = symbols.computeIfAbsent(fill.symbol(), s -> new SymbolState());
        state.trades.addFirst(new Trade(fill.priceTicks(), fill.quantity(), fill.takerSide(), ++tradeSequence));
        while (state.trades.size() > MAX_RECENT_TRADES) {
            state.trades.removeLast();
        }
    }

    private void servePage(HttpExchange exchange) throws IOException {
        if (!exchange.getRequestURI().getPath().equals("/")) {
            respond(exchange, 404, "not found".getBytes(), "text/plain");
            return;
        }
        try (var page = MarketDataServer.class.getResourceAsStream("/web/index.html")) {
            respond(exchange, 200, page.readAllBytes(), "text/html");
        }
    }

    private void serveBook(HttpExchange exchange) throws IOException {
        var query = exchange.getRequestURI().getQuery();
        var symbol = "BTC-USD";
        if (query != null && query.startsWith("symbol=") && !query.substring(7).isBlank()) {
            symbol = query.substring(7);
        }
        BookSnapshot snapshot;
        synchronized (lock) {
            var state = symbols.getOrDefault(symbol, new SymbolState());
            snapshot = new BookSnapshot(
                    symbol,
                    topLevels(state.bids),
                    topLevels(state.asks),
                    List.copyOf(state.trades));
        }
        respond(exchange, 200, Json.toBytes(snapshot), "application/json");
    }

    private static List<Level> topLevels(NavigableMap<Long, Long> side) {
        var levels = new ArrayList<Level>();
        for (var entry : side.entrySet()) {
            if (levels.size() == MAX_DEPTH_LEVELS) {
                break;
            }
            levels.add(new Level(entry.getKey(), entry.getValue()));
        }
        return levels;
    }

    /** Keeps the page same-origin; a real deployment would put both services behind one edge proxy. */
    private void proxyToGateway(HttpExchange exchange, String path) throws IOException {
        try {
            var upstream = gatewayClient.send(
                    HttpRequest.newBuilder(gatewayUri.resolve(path))
                            .method(exchange.getRequestMethod(),
                                    HttpRequest.BodyPublishers.ofByteArray(exchange.getRequestBody().readAllBytes()))
                            .timeout(Duration.ofSeconds(15))
                            .build(),
                    HttpResponse.BodyHandlers.ofByteArray());
            var contentType = upstream.headers().firstValue("Content-Type").orElse("application/json");
            respond(exchange, upstream.statusCode(), upstream.body(), contentType);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            respond(exchange, 502, "gateway unavailable".getBytes(), "text/plain");
        } catch (IOException e) {
            log.warn("gateway proxy failure for {}", path, e);
            respond(exchange, 502, "gateway unavailable".getBytes(), "text/plain");
        }
    }

    private Properties consumerConfig() {
        var config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return config;
    }

    private static void respond(HttpExchange exchange, int status, byte[] body, String contentType) throws IOException {
        exchange.getResponseHeaders().set("Content-Type", contentType);
        exchange.sendResponseHeaders(status, body.length);
        try (var out = exchange.getResponseBody()) {
            out.write(body);
        }
    }

    @Override
    public void close() {
        if (server != null) {
            server.stop(0);
        }
        running = false;
        consumer.wakeup();
        try {
            consumerThread.join(10_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
