package dev.rvsiyad.exchange.marketdata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

/**
 * Minimal RFC 6455 WebSocket server: HTTP Upgrade handshake in, text frames
 * out, close/ping handled. Hand-rolled because the JDK ships a WebSocket
 * client but no server, and com.sun.net.httpserver cannot hand its socket
 * over to an Upgrade — so the feed listens on its own port rather than
 * piggybacking the demo page's.
 *
 * The protocol surface we need is small: a client connects with a plain HTTP
 * GET carrying Sec-WebSocket-Key, we answer 101 with the derived accept key,
 * and from then on the TCP stream carries length-prefixed frames instead of
 * HTTP. Server-to-client frames are never masked; client-to-server frames
 * always are (a rule that exists to defeat cache-poisoning proxies, not for
 * secrecy).
 */
final class WebSocketHub implements AutoCloseable {

    interface Listener {
        void clientConnected(Client client, String symbol);

        void clientDisconnected(Client client);
    }

    private static final Logger log = LoggerFactory.getLogger(WebSocketHub.class);
    /** Fixed by RFC 6455; proves the server actually speaks WebSocket rather than blindly echoing headers. */
    private static final String HANDSHAKE_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
    private static final int MAX_HANDSHAKE_BYTES = 8 * 1024;
    private static final long MAX_INBOUND_FRAME_BYTES = 1 << 20;

    private final Listener listener;
    private ServerSocket serverSocket;
    private Thread acceptThread;
    private volatile boolean running = true;

    WebSocketHub(Listener listener) {
        this.listener = listener;
    }

    /** Binds and starts accepting; returns the bound port (useful when asked for port 0). */
    int start(int port) throws IOException {
        serverSocket = new ServerSocket(port);
        acceptThread = new Thread(this::acceptLoop, "ws-accept");
        acceptThread.start();
        return serverSocket.getLocalPort();
    }

    private void acceptLoop() {
        while (running) {
            try {
                var socket = serverSocket.accept();
                Thread.ofVirtual().name("ws-client-" + socket.getPort()).start(() -> serve(socket));
            } catch (IOException e) {
                if (running) {
                    log.warn("accept failed", e);
                }
            }
        }
    }

    private void serve(Socket socket) {
        Client client = null;
        try {
            var in = socket.getInputStream();
            var symbol = handshake(socket, in);
            client = new Client(socket);
            listener.clientConnected(client, symbol);
            readLoop(client, in);
        } catch (IOException e) {
            log.debug("websocket client dropped: {}", e.toString());
        } finally {
            if (client != null) {
                listener.clientDisconnected(client);
                client.close();
            } else {
                closeQuietly(socket);
            }
        }
    }

    private String handshake(Socket socket, InputStream in) throws IOException {
        var lines = readRequestHead(in).split("\r\n");
        var requestLine = lines[0].split(" ");
        if (requestLine.length < 2 || !"GET".equals(requestLine[0])) {
            throw new IOException("not a websocket handshake: " + lines[0]);
        }
        String key = null;
        for (var line : lines) {
            var colon = line.indexOf(':');
            if (colon > 0 && line.substring(0, colon).equalsIgnoreCase("Sec-WebSocket-Key")) {
                key = line.substring(colon + 1).trim();
            }
        }
        if (key == null) {
            throw new IOException("missing Sec-WebSocket-Key");
        }
        var response = "HTTP/1.1 101 Switching Protocols\r\n"
                + "Upgrade: websocket\r\n"
                + "Connection: Upgrade\r\n"
                + "Sec-WebSocket-Accept: " + acceptKey(key) + "\r\n"
                + "\r\n";
        socket.getOutputStream().write(response.getBytes(StandardCharsets.US_ASCII));
        socket.getOutputStream().flush();
        return symbolFrom(requestLine[1]);
    }

    /**
     * Reads raw bytes up to the blank line ourselves — a buffered reader could
     * slurp bytes past the head into its own buffer, and those bytes belong to
     * the first frame.
     */
    private static String readRequestHead(InputStream in) throws IOException {
        var head = new ByteArrayOutputStream();
        var state = 0;   // counts progress through \r\n\r\n
        while (state < 4) {
            var b = in.read();
            if (b < 0) {
                throw new EOFException("connection closed during handshake");
            }
            if (head.size() > MAX_HANDSHAKE_BYTES) {
                throw new IOException("handshake too large");
            }
            head.write(b);
            if (b == '\r' && (state == 0 || state == 2)) {
                state++;
            } else if (b == '\n' && (state == 1 || state == 3)) {
                state++;
            } else {
                state = b == '\r' ? 1 : 0;
            }
        }
        return head.toString(StandardCharsets.US_ASCII);
    }

    private static String acceptKey(String clientKey) {
        try {
            var digest = MessageDigest.getInstance("SHA-1")
                    .digest((clientKey + HANDSHAKE_GUID).getBytes(StandardCharsets.US_ASCII));
            return Base64.getEncoder().encodeToString(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new UncheckedIOException(new IOException("JVM without SHA-1", e));
        }
    }

    private static String symbolFrom(String path) {
        var query = path.indexOf('?');
        if (query >= 0) {
            for (var param : path.substring(query + 1).split("&")) {
                if (param.startsWith("symbol=") && !param.substring(7).isBlank()) {
                    return param.substring(7);
                }
            }
        }
        return "BTC-USD";
    }

    private void readLoop(Client client, InputStream in) throws IOException {
        while (true) {
            var b0 = in.read();
            if (b0 < 0) {
                return;
            }
            var b1 = require(in);
            var opcode = b0 & 0x0F;
            var length = (long) (b1 & 0x7F);
            if (length == 126) {
                length = ((long) require(in) << 8) | require(in);
            } else if (length == 127) {
                length = 0;
                for (var i = 0; i < 8; i++) {
                    length = (length << 8) | require(in);
                }
            }
            if (length > MAX_INBOUND_FRAME_BYTES) {
                throw new IOException("inbound frame too large: " + length);
            }
            var mask = (b1 & 0x80) != 0 ? readExactly(in, 4) : null;
            var payload = readExactly(in, (int) length);
            if (mask != null) {
                for (var i = 0; i < payload.length; i++) {
                    payload[i] ^= mask[i % 4];
                }
            }
            switch (opcode) {
                case 0x8 -> {   // close: echo it back, then hang up
                    client.sendFrame(0x8, payload);
                    return;
                }
                case 0x9 -> client.sendFrame(0xA, payload);   // ping -> pong
                default -> {
                    // inbound text/binary ignored: this feed is one-way
                }
            }
        }
    }

    private static int require(InputStream in) throws IOException {
        var b = in.read();
        if (b < 0) {
            throw new EOFException("connection closed mid-frame");
        }
        return b;
    }

    private static byte[] readExactly(InputStream in, int n) throws IOException {
        var bytes = in.readNBytes(n);
        if (bytes.length < n) {
            throw new EOFException("connection closed mid-frame");
        }
        return bytes;
    }

    private static void closeQuietly(Socket socket) {
        try {
            socket.close();
        } catch (IOException ignored) {
        }
    }

    @Override
    public void close() {
        running = false;
        if (serverSocket != null) {
            try {
                serverSocket.close();
            } catch (IOException ignored) {
            }
            try {
                acceptThread.join(10_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /** One connected browser. Frame writes are serialized so broadcasts cannot interleave bytes. */
    static final class Client {

        private final Socket socket;
        private final OutputStream out;

        private Client(Socket socket) throws IOException {
            this.socket = socket;
            this.out = new BufferedOutputStream(socket.getOutputStream());
        }

        void sendText(String message) throws IOException {
            sendFrame(0x1, message.getBytes(StandardCharsets.UTF_8));
        }

        private synchronized void sendFrame(int opcode, byte[] payload) throws IOException {
            out.write(0x80 | opcode);   // FIN set: we never fragment
            if (payload.length < 126) {
                out.write(payload.length);
            } else if (payload.length <= 0xFFFF) {
                out.write(126);
                out.write(payload.length >> 8);
                out.write(payload.length);
            } else {
                out.write(127);
                for (var shift = 56; shift >= 0; shift -= 8) {
                    out.write((int) ((long) payload.length >> shift));
                }
            }
            out.write(payload);
            out.flush();
        }

        void close() {
            closeQuietly(socket);
        }
    }
}
