package dev.rvsiyad.exchange.engine.demo;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import dev.rvsiyad.exchange.common.Fill;
import dev.rvsiyad.exchange.common.Json;
import dev.rvsiyad.exchange.common.OrderCommand;
import dev.rvsiyad.exchange.common.Side;
import dev.rvsiyad.exchange.engine.OrderBook;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/**
 * Walking-skeleton demo: one process, one symbol, the real matching engine
 * behind a crude web page. No Kafka, no TigerBeetle, no balances — sessions 3+
 * swap those in behind the same UI contract. Not part of the real data path.
 *
 * The book is single-writer by design; here a lock serializes HTTP handlers
 * onto it, standing in for the partition-consumer thread that arrives with
 * Kafka in session 3.
 */
public final class DemoServerMain {

    record NewOrderRequest(String userId, Side side, long priceTicks, long quantity) {
    }

    record CancelRequest(String orderId, String userId) {
    }

    record Trade(long priceTicks, long quantity, Side takerSide, long sequence) {
    }

    record BookSnapshot(String symbol, List<OrderBook.Level> bids, List<OrderBook.Level> asks, List<Trade> trades) {
    }

    record PlaceResponse(String orderId, List<Fill> fills) {
    }

    private static final String SYMBOL = "BTC-USD";
    private static final int MAX_DEPTH_LEVELS = 10;
    private static final int MAX_RECENT_TRADES = 20;

    private final OrderBook book = new OrderBook(SYMBOL);
    private final Deque<Trade> recentTrades = new ArrayDeque<>();
    private final Object lock = new Object();
    private long orderSequence;
    private long tradeSequence;

    public static void main(String[] args) throws IOException {
        int port = Integer.parseInt(System.getenv().getOrDefault("DEMO_PORT", "8090"));
        var server = new DemoServerMain().start(port);
        System.out.println("demo server listening on http://localhost:" + server.getAddress().getPort());
    }

    public HttpServer start(int port) throws IOException {
        var server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/", this::servePage);
        server.createContext("/api/book", this::serveBook);
        server.createContext("/api/orders", this::placeOrder);
        server.createContext("/api/cancel", this::cancelOrder);
        server.start();
        return server;
    }

    private void servePage(HttpExchange exchange) throws IOException {
        if (!exchange.getRequestURI().getPath().equals("/")) {
            respond(exchange, 404, "not found".getBytes(), "text/plain");
            return;
        }
        try (var page = DemoServerMain.class.getResourceAsStream("/web/index.html")) {
            respond(exchange, 200, page.readAllBytes(), "text/html");
        }
    }

    private void serveBook(HttpExchange exchange) throws IOException {
        BookSnapshot snapshot;
        synchronized (lock) {
            snapshot = new BookSnapshot(
                    SYMBOL,
                    book.depth(Side.BUY, MAX_DEPTH_LEVELS),
                    book.depth(Side.SELL, MAX_DEPTH_LEVELS),
                    List.copyOf(recentTrades));
        }
        respond(exchange, 200, Json.toBytes(snapshot), "application/json");
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
        if (request.userId() == null || request.userId().isBlank()
                || request.side() == null || request.priceTicks() <= 0 || request.quantity() <= 0) {
            respond(exchange, 400, "userId, side, positive priceTicks and quantity required".getBytes(), "text/plain");
            return;
        }

        PlaceResponse response;
        synchronized (lock) {
            var orderId = "o-" + ++orderSequence;
            var fills = book.apply(OrderCommand.newOrder(
                    orderId, request.userId(), SYMBOL, request.side(),
                    request.priceTicks(), request.quantity(), System.nanoTime()));
            for (var fill : fills) {
                recentTrades.addFirst(new Trade(fill.priceTicks(), fill.quantity(), fill.takerSide(), ++tradeSequence));
            }
            while (recentTrades.size() > MAX_RECENT_TRADES) {
                recentTrades.removeLast();
            }
            response = new PlaceResponse(orderId, new ArrayList<>(fills));
        }
        respond(exchange, 200, Json.toBytes(response), "application/json");
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
        if (request.orderId() == null || request.orderId().isBlank()) {
            respond(exchange, 400, "orderId required".getBytes(), "text/plain");
            return;
        }
        synchronized (lock) {
            book.apply(OrderCommand.cancel(request.orderId(), request.userId(), SYMBOL, System.nanoTime()));
        }
        respond(exchange, 200, "{}".getBytes(), "application/json");
    }

    private static void respond(HttpExchange exchange, int status, byte[] body, String contentType) throws IOException {
        exchange.getResponseHeaders().set("Content-Type", contentType);
        exchange.sendResponseHeaders(status, body.length);
        try (var out = exchange.getResponseBody()) {
            out.write(body);
        }
    }
}
