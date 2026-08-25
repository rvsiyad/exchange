package dev.rvsiyad.exchange.gateway;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import dev.rvsiyad.exchange.common.Json;
import dev.rvsiyad.exchange.common.OrderCommand;
import dev.rvsiyad.exchange.common.Side;
import dev.rvsiyad.exchange.common.Topics;
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
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * REST order entry. The gateway owns none of the matching: it validates,
 * assigns an order id, and publishes an OrderCommand to `orders` keyed by
 * symbol — the key is what routes every command for a symbol to the same
 * partition, and therefore to the same single-writer engine thread.
 *
 * Responses are 202: the order is durably in the log (acks=all before we
 * answer), but matching happens asynchronously downstream. Fund reservation
 * (TigerBeetle) and idempotency keys (Postgres) arrive in later sessions.
 */
public final class GatewayServer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(GatewayServer.class);
    private static final long PUBLISH_TIMEOUT_SECONDS = 10;

    record NewOrderRequest(String userId, String symbol, Side side, long priceTicks, long quantity) {
    }

    record CancelRequest(String orderId, String userId, String symbol) {
    }

    record AcceptedResponse(String orderId) {
    }

    private final KafkaProducer<String, byte[]> producer;
    private HttpServer server;

    public GatewayServer(String bootstrapServers) {
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
        server.createContext("/health", exchange -> respond(exchange, 200, "ok".getBytes(), "text/plain"));
        server.createContext("/api/orders", this::placeOrder);
        server.createContext("/api/cancel", this::cancelOrder);
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
            respond(exchange, 400, "malformed request".getBytes(), "text/plain");
            return;
        }
        if (isBlank(request.userId()) || isBlank(request.symbol()) || request.side() == null
                || request.priceTicks() <= 0 || request.quantity() <= 0) {
            respond(exchange, 400,
                    "userId, symbol, side, positive priceTicks and quantity required".getBytes(), "text/plain");
            return;
        }

        var orderId = "o-" + UUID.randomUUID();
        var command = OrderCommand.newOrder(
                orderId, request.userId(), request.symbol(), request.side(),
                request.priceTicks(), request.quantity(), epochNanos());
        if (publish(command)) {
            respond(exchange, 202, Json.toBytes(new AcceptedResponse(orderId)), "application/json");
        } else {
            respond(exchange, 503, "order log unavailable".getBytes(), "text/plain");
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

        var command = OrderCommand.cancel(request.orderId(), request.userId(), request.symbol(), epochNanos());
        if (publish(command)) {
            respond(exchange, 202, Json.toBytes(new AcceptedResponse(request.orderId())), "application/json");
        } else {
            respond(exchange, 503, "order log unavailable".getBytes(), "text/plain");
        }
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
    }
}
