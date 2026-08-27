package dev.rvsiyad.exchange.gateway;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import dev.rvsiyad.exchange.common.Assets;
import dev.rvsiyad.exchange.common.Json;
import dev.rvsiyad.exchange.common.Metrics;
import dev.rvsiyad.exchange.common.OrderCommand;
import dev.rvsiyad.exchange.common.Side;
import dev.rvsiyad.exchange.common.Topics;
import dev.rvsiyad.exchange.ledger.Ledger;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * REST order entry. The gateway owns none of the matching: it validates,
 * reserves the order's worst-case cost in TigerBeetle, assigns an order id,
 * and publishes an OrderCommand to `orders` keyed by symbol — the key is what
 * routes every command for a symbol to the same partition, and therefore to
 * the same single-writer engine thread.
 *
 * The reservation happens before the publish, so an order can only reach the
 * book with its funds already held: insufficient funds is rejected by the
 * ledger database itself (422), and the matching engine never needs to know
 * money exists. A buy holds limit x quantity of the quote asset (worst case
 * — price improvement is refunded at settlement); a sell holds the base
 * quantity.
 *
 * Responses are 202: the order is durably in the log (acks=all before we
 * answer), but matching happens asynchronously downstream.
 */
public final class GatewayServer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(GatewayServer.class);
    private static final long PUBLISH_TIMEOUT_SECONDS = 10;

    // The RED view of order entry: everything that comes in is either accepted
    // (funds held, durably in the log) or rejected with a reason — the reason
    // label is what turns a 4xx blip on a dashboard into a diagnosis.
    private static final Metrics.Counter ORDERS_ACCEPTED = Metrics.counter(
            "gateway_orders_accepted_total", "Orders with funds reserved and durably published to the log");
    private static final Metrics.Counter CANCELS_ACCEPTED = Metrics.counter(
            "gateway_cancels_accepted_total", "Cancel commands durably published to the log");

    record NewOrderRequest(String userId, String symbol, Side side, long priceTicks, long quantity) {
    }

    record CancelRequest(String orderId, String userId, String symbol) {
    }

    record AcceptedResponse(String orderId) {
    }

    record BalancesResponse(String userId, List<Ledger.AssetBalance> balances) {
    }

    private final KafkaProducer<String, byte[]> producer;
    private final Ledger ledger;
    private final Set<String> knownUsers = ConcurrentHashMap.newKeySet();
    private HttpServer server;

    public GatewayServer(String bootstrapServers, Ledger ledger) {
        this.ledger = ledger;
        var config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        this.producer = new KafkaProducer<>(config);
    }

    public HttpServer start(int port) throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);
        // Without an executor the JDK server runs every handler on its one
        // dispatcher thread — a serial gateway. A virtual thread per request
        // makes order entry actually concurrent; the safety story is
        // unchanged because reservations are atomic in the ledger, not in
        // application locks.
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        server.createContext("/health", exchange -> respond(exchange, 200, "ok".getBytes(), "text/plain"));
        server.createContext("/api/orders", this::placeOrder);
        server.createContext("/api/cancel", this::cancelOrder);
        server.createContext("/api/balances", this::serveBalances);
        server.start();
        log.info("gateway listening on port {}", server.getAddress().getPort());
        return server;
    }

    private void placeOrder(HttpExchange exchange) throws IOException {
        if (!exchange.getRequestMethod().equals("POST")) {
            respond(exchange, 405, "POST only".getBytes(), "text/plain");
            return;
        }
        NewOrderRequest request;
        try {
            request = Json.fromBytes(exchange.getRequestBody().readAllBytes(), NewOrderRequest.class);
        } catch (RuntimeException e) {
            rejectOrder(exchange, 400, "malformed request", "malformed");
            return;
        }
        if (isBlank(request.userId()) || isBlank(request.symbol()) || request.side() == null
                || request.priceTicks() <= 0 || request.quantity() <= 0) {
            rejectOrder(exchange, 400,
                    "userId, symbol, side, positive priceTicks and quantity required", "invalid_fields");
            return;
        }
        var instrument = Assets.parseSymbol(request.symbol()).orElse(null);
        if (instrument == null) {
            rejectOrder(exchange, 400, "unknown symbol", "unknown_symbol");
            return;
        }

        // Worst case the order can cost: a buy pays quote at its limit, a sell delivers base.
        String reserveAsset;
        long reserveAmount;
        if (request.side() == Side.BUY) {
            reserveAsset = instrument.quote();
            try {
                reserveAmount = Math.multiplyExact(request.priceTicks(), request.quantity());
            } catch (ArithmeticException e) {
                rejectOrder(exchange, 400, "order cost overflows", "cost_overflow");
                return;
            }
        } else {
            reserveAsset = instrument.base();
            reserveAmount = request.quantity();
        }

        var orderId = "o-" + UUID.randomUUID();
        // First contact creates the user's accounts. Every racer on a brand-new
        // user runs the (idempotent) creation itself — marking the user known
        // first would open a window where a concurrent request reserves against
        // accounts that do not exist yet.
        if (!knownUsers.contains(request.userId())) {
            ledger.ensureUserAccounts(request.userId());
            knownUsers.add(request.userId());
        }
        switch (ledger.reserve(orderId, request.userId(), reserveAsset, reserveAmount)) {
            case INSUFFICIENT_FUNDS -> {
                rejectOrder(exchange, 422, "insufficient funds", "insufficient_funds");
                return;
            }
            case FAILED -> {
                rejectOrder(exchange, 503, "ledger unavailable", "ledger_unavailable");
                return;
            }
            case RESERVED -> {
            }
        }

        var command = OrderCommand.newOrder(
                orderId, request.userId(), request.symbol(), request.side(),
                request.priceTicks(), request.quantity(), epochNanos());
        if (publish(command)) {
            ORDERS_ACCEPTED.increment();
            respond(exchange, 202, Json.toBytes(new AcceptedResponse(orderId)), "application/json");
        } else {
            // The order never reached the log, so nothing downstream will ever
            // settle or void it — release the hold before failing the request.
            ledger.voidReservation(orderId, 0);
            rejectOrder(exchange, 503, "order log unavailable", "log_unavailable");
        }
    }

    private void cancelOrder(HttpExchange exchange) throws IOException {
        if (!exchange.getRequestMethod().equals("POST")) {
            respond(exchange, 405, "POST only".getBytes(), "text/plain");
            return;
        }
        CancelRequest request;
        try {
            request = Json.fromBytes(exchange.getRequestBody().readAllBytes(), CancelRequest.class);
        } catch (RuntimeException e) {
            respond(exchange, 400, "malformed request".getBytes(), "text/plain");
            return;
        }
        if (isBlank(request.orderId()) || isBlank(request.userId()) || isBlank(request.symbol())) {
            respond(exchange, 400, "orderId, userId and symbol required".getBytes(), "text/plain");
            return;
        }

        // No ledger work here: only the engine knows whether the cancel removes
        // anything, and settlement voids the reservation when it says so.
        var command = OrderCommand.cancel(request.orderId(), request.userId(), request.symbol(), epochNanos());
        if (publish(command)) {
            CANCELS_ACCEPTED.increment();
            respond(exchange, 202, Json.toBytes(new AcceptedResponse(request.orderId())), "application/json");
        } else {
            respond(exchange, 503, "order log unavailable".getBytes(), "text/plain");
        }
    }

    private void serveBalances(HttpExchange exchange) throws IOException {
        if (!exchange.getRequestMethod().equals("GET")) {
            respond(exchange, 405, "GET only".getBytes(), "text/plain");
            return;
        }
        var query = exchange.getRequestURI().getQuery();
        if (query == null || !query.startsWith("userId=") || query.substring(7).isBlank()) {
            respond(exchange, 400, "userId required".getBytes(), "text/plain");
            return;
        }
        var userId = query.substring(7);
        respond(exchange, 200,
                Json.toBytes(new BalancesResponse(userId, ledger.balances(userId))), "application/json");
    }

    /** Blocks until the broker acknowledges the write: a 202 means the command is durably in the log. */
    private boolean publish(OrderCommand command) {
        try {
            producer.send(new ProducerRecord<>(Topics.ORDERS, command.symbol(), Json.toBytes(command)))
                    .get(PUBLISH_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            return true;
        } catch (ExecutionException | TimeoutException e) {
            log.error("failed to publish {} for order {}", command.type(), command.orderId(), e);
            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private static long epochNanos() {
        var now = Instant.now();
        return now.getEpochSecond() * 1_000_000_000L + now.getNano();
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }

    private static void rejectOrder(HttpExchange exchange, int status, String message, String reason) throws IOException {
        Metrics.counter("gateway_orders_rejected_total",
                "Orders rejected before reaching the log, by reason", "reason", reason).increment();
        respond(exchange, status, message.getBytes(), "text/plain");
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
        producer.close();
        ledger.close();
    }
}
