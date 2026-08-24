package dev.rvsiyad.exchange.engine.demo;

import com.sun.net.httpserver.HttpServer;
import dev.rvsiyad.exchange.common.Json;
import dev.rvsiyad.exchange.common.Side;
import dev.rvsiyad.exchange.engine.OrderBook;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests over real HTTP: each test boots the server on an ephemeral
 * port and talks to it exactly as the browser does.
 */
class DemoServerTest {

    private HttpServer server;
    private HttpClient client;
    private URI base;

    @BeforeEach
    void start() throws IOException {
        server = new DemoServerMain().start(0);
        base = URI.create("http://localhost:" + server.getAddress().getPort());
        client = HttpClient.newHttpClient();
    }

    @AfterEach
    void stop() {
        server.stop(0);
    }

    private HttpResponse<byte[]> get(String path) throws IOException, InterruptedException {
        var request = HttpRequest.newBuilder(base.resolve(path)).GET().build();
        return client.send(request, HttpResponse.BodyHandlers.ofByteArray());
    }

    private HttpResponse<byte[]> post(String path, byte[] body) throws IOException, InterruptedException {
        var request = HttpRequest.newBuilder(base.resolve(path))
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .build();
        return client.send(request, HttpResponse.BodyHandlers.ofByteArray());
    }

    private DemoServerMain.PlaceResponse place(String userId, Side side, long priceTicks, long quantity)
            throws IOException, InterruptedException {
        var response = post("/api/orders",
                Json.toBytes(new DemoServerMain.NewOrderRequest(userId, side, priceTicks, quantity)));
        assertEquals(200, response.statusCode());
        return Json.fromBytes(response.body(), DemoServerMain.PlaceResponse.class);
    }

    private DemoServerMain.BookSnapshot book() throws IOException, InterruptedException {
        var response = get("/api/book");
        assertEquals(200, response.statusCode());
        return Json.fromBytes(response.body(), DemoServerMain.BookSnapshot.class);
    }

    @Test
    void servesTheDemoPage() throws Exception {
        var response = get("/");
        assertEquals(200, response.statusCode());
        assertTrue(new String(response.body()).contains("<title>exchange"));
    }

    @Test
    void restingOrderAppearsInBookDepth() throws Exception {
        var placed = place("alice", Side.BUY, 100_00, 3);
        assertTrue(placed.fills().isEmpty());

        var snapshot = book();
        assertEquals(List.of(new OrderBook.Level(100_00, 3)), snapshot.bids());
        assertTrue(snapshot.asks().isEmpty());
        assertTrue(snapshot.trades().isEmpty());
    }

    @Test
    void crossingOrdersFillAndShowOnTheTape() throws Exception {
        place("bob", Side.SELL, 100_50, 2);
        var taker = place("dave", Side.BUY, 101_00, 2);

        assertEquals(1, taker.fills().size());
        assertEquals(100_50, taker.fills().get(0).priceTicks());
        assertEquals("bob", taker.fills().get(0).makerUserId());

        var snapshot = book();
        assertTrue(snapshot.bids().isEmpty());
        assertTrue(snapshot.asks().isEmpty());
        assertEquals(1, snapshot.trades().size());
        assertEquals(100_50, snapshot.trades().get(0).priceTicks());
        assertEquals(Side.BUY, snapshot.trades().get(0).takerSide());
    }

    @Test
    void cancelRemovesTheRestingOrder() throws Exception {
        var placed = place("alice", Side.BUY, 100_00, 3);
        var response = post("/api/cancel",
                Json.toBytes(new DemoServerMain.CancelRequest(placed.orderId(), "alice")));
        assertEquals(200, response.statusCode());
        assertTrue(book().bids().isEmpty());
    }

    @Test
    void rejectsMalformedAndInvalidOrders() throws Exception {
        assertEquals(400, post("/api/orders", "not json".getBytes()).statusCode());
        assertEquals(400, post("/api/orders",
                Json.toBytes(new DemoServerMain.NewOrderRequest("alice", Side.BUY, 100_00, 0))).statusCode());
        assertEquals(400, post("/api/orders",
                Json.toBytes(new DemoServerMain.NewOrderRequest(" ", Side.BUY, 100_00, 1))).statusCode());
        assertEquals(405, get("/api/orders").statusCode());
    }
}
