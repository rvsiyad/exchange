package dev.rvsiyad.exchange.marketdata;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Protocol-level tests against the hand-rolled hub with a raw socket — no
 * Kafka, no JDK WebSocket client, just bytes on a loopback connection.
 */
class WebSocketHubTest {

    /** Handshake driven with RFC 6455's own worked example (section 1.3). */
    @Test
    void answersTheHandshakeWithTheRfcSampleAcceptKey() throws Exception {
        var hub = new WebSocketHub(new NoopListener());
        var port = hub.start(0);
        try (var socket = new Socket("localhost", port)) {
            var head = handshake(socket);
            assertTrue(head.startsWith("HTTP/1.1 101"), head);
            assertTrue(head.contains("Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo="), head);
        } finally {
            hub.close();
        }
    }

    /**
     * The session-5 lesson: a client that stops reading must be evicted, not
     * allowed to block the feed. TCP buffers absorb the first wave of
     * messages, then the writer thread blocks, the bounded queue fills, and
     * enqueue starts refusing — all without the producer ever waiting.
     */
    @Test
    void evictsAClientThatStopsReadingInsteadOfBlockingTheProducer() throws Exception {
        var connected = new AtomicReference<WebSocketHub.Client>();
        var disconnected = new CountDownLatch(1);
        var hub = new WebSocketHub(new WebSocketHub.Listener() {
            @Override
            public void clientConnected(WebSocketHub.Client client, String symbol) {
                connected.set(client);
            }

            @Override
            public void clientDisconnected(WebSocketHub.Client client) {
                disconnected.countDown();
            }
        }, 4);
        var port = hub.start(0);
        try (var socket = new Socket()) {
            socket.setReceiveBufferSize(4 * 1024);   // small window: the server backs up sooner
            socket.connect(new java.net.InetSocketAddress("localhost", port));
            handshake(socket);
            // ... and now never read another byte.
            var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            while (connected.get() == null) {
                assertTrue(System.nanoTime() < deadline, "client never connected");
                Thread.sleep(10);
            }
            var client = connected.get();
            var message = "x".repeat(512);
            var evicted = false;
            for (var i = 0; i < 500_000 && !evicted; i++) {
                evicted = !client.enqueue(message);
            }
            assertTrue(evicted, "a client that never reads must eventually be refused");
            assertTrue(disconnected.await(10, TimeUnit.SECONDS),
                    "eviction must close the connection and fire the disconnect callback");
        } finally {
            hub.close();
        }
    }

    private static String handshake(Socket socket) throws IOException {
        socket.getOutputStream().write(("""
                GET /ws?symbol=TEST HTTP/1.1\r
                Host: localhost\r
                Upgrade: websocket\r
                Connection: Upgrade\r
                Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r
                Sec-WebSocket-Version: 13\r
                \r
                """).getBytes(StandardCharsets.US_ASCII));
        socket.getOutputStream().flush();
        return readResponseHead(socket.getInputStream());
    }

    private static String readResponseHead(InputStream in) throws IOException {
        var head = new StringBuilder();
        while (!head.toString().endsWith("\r\n\r\n")) {
            var b = in.read();
            if (b < 0) {
                throw new IOException("connection closed during handshake: " + head);
            }
            head.append((char) b);
        }
        return head.toString();
    }

    private static final class NoopListener implements WebSocketHub.Listener {
        @Override
        public void clientConnected(WebSocketHub.Client client, String symbol) {
        }

        @Override
        public void clientDisconnected(WebSocketHub.Client client) {
        }
    }
}
