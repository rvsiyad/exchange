package dev.rvsiyad.exchange.storm;

import dev.rvsiyad.exchange.common.Assets;
import dev.rvsiyad.exchange.common.Fill;
import dev.rvsiyad.exchange.common.FillsTopic;
import dev.rvsiyad.exchange.common.Json;
import dev.rvsiyad.exchange.common.Metrics;
import dev.rvsiyad.exchange.common.ReservationRelease;
import dev.rvsiyad.exchange.common.Side;
import dev.rvsiyad.exchange.common.Topics;
import dev.rvsiyad.exchange.engine.Engine;
import dev.rvsiyad.exchange.engine.OrderBook;
import dev.rvsiyad.exchange.gateway.GatewayServer;
import dev.rvsiyad.exchange.ledger.Ledger;
import dev.rvsiyad.exchange.ledger.TigerBeetleContainers;
import dev.rvsiyad.exchange.settlement.SettlementService;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.redpanda.RedpandaContainer;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * The order storm: the whole venue — real gateway, real engine, real
 * settlement, real Kafka and TigerBeetle — under randomized concurrent load,
 * then the invariants that define a financial system, asserted from the
 * outside:
 *
 *   1. Conservation: every unit the treasury issued is in a user account;
 *      escrow keeps nothing once settlement has caught up.
 *   2. No negative balances, anywhere, ever.
 *   3. Every fill settled exactly once.
 *   4. The engine's books and the ledger's holds agree to the cent.
 *   5. No book is crossed.
 *
 * Property-based, not example-based: the test doesn't know what the right
 * balances *are* — it knows what must be true of them no matter how the
 * randomized orders interleaved. The scenario derives from one seed, printed
 * on every run and stamped into every failure message: any red run reproduces
 * with -Dstorm.seed=<seed>.
 */
@Testcontainers
class OrderStormTest {

    @Container
    static final RedpandaContainer REDPANDA = new RedpandaContainer("redpandadata/redpanda:v24.2.7");

    @Container
    static final GenericContainer<?> TIGERBEETLE = TigerBeetleContainers.create();

    static final int USERS = 8;
    static final int ACTIONS_PER_USER = 100;
    static final double CANCEL_RATIO = 0.3;
    static final List<String> SYMBOLS = List.of("ETH-USD", "BTC-USD");
    static final Map<String, Long> MID_TICKS = Map.of("ETH-USD", 100L, "BTC-USD", 200L);
    /** Tight on purpose: holds pile up and some orders must bounce off the ledger. */
    static final long FUND_USD = 2_000;
    static final long FUND_BASE = 60;

    final long seed = Long.getLong("storm.seed") != null ? Long.getLong("storm.seed") : new Random().nextLong();

    @TempDir
    Path snapshotDir;

    record Action(boolean cancel, String symbol, Side side, long priceTicks, long quantity, int cancelPick) {
    }

    record PartitionSnapshot(long nextOffset, List<OrderBook.BookState> books) {
    }

    record PlacedOrder(String orderId, String symbol) {
    }

    @Test
    void moneyIsConservedUnderAConcurrentOrderStorm() throws Exception {
        System.out.println("order storm seed: " + seed + " (reproduce with -Dstorm.seed=" + seed + ")");
        var bootstrap = REDPANDA.getBootstrapServers();
        try (var admin = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap))) {
            admin.createTopics(List.of(
                    new NewTopic(Topics.ORDERS, 4, (short) 1),
                    new NewTopic(Topics.FILLS, 1, (short) 1),
                    new NewTopic(Topics.BOOK_UPDATES, 1, (short) 1))).all().get();
        }

        // Separate Ledger clients per service, as separate processes would have.
        var gatewayLedger = new Ledger(TigerBeetleContainers.address(TIGERBEETLE));
        var settlementLedger = new Ledger(TigerBeetleContainers.address(TIGERBEETLE));
        gatewayLedger.ensureVenueAccounts();
        for (var user : users()) {
            gatewayLedger.ensureUserAccounts(user);
            gatewayLedger.fund(user, "USD", FUND_USD);
            gatewayLedger.fund(user, "ETH", FUND_BASE);
            gatewayLedger.fund(user, "BTC", FUND_BASE);
        }

        var gateway = new GatewayServer(bootstrap, gatewayLedger);
        var baseUrl = "http://localhost:" + gateway.start(0).getAddress().getPort();
        var engine = new Engine(bootstrap, snapshotDir, 200);
        engine.start();
        var settlement = new SettlementService(bootstrap, settlementLedger);
        settlement.start();

        try {
            long commandsPublished = storm(baseUrl);

            // Quiescence, phase 1: the engine has applied every command that
            // was durably published.
            await(() -> Metrics.counterTotal("engine_commands_total"), commandsPublished,
                    Duration.ofSeconds(120), "engine consuming the order log");
        } finally {
            // Closing flushes the fills producer and writes final snapshots —
            // after this, the stream and the books are both complete.
            engine.close();
        }

        var stream = readWholeFillsTopic(bootstrap);
        try {
            // Quiescence, phase 2: settlement has consumed the whole stream.
            await(() -> Metrics.counterTotal("settlement_events_total"),
                    stream.fills().size() + stream.releases(),
                    Duration.ofSeconds(120), "settlement consuming the fills stream");
        } finally {
            settlement.close();
            gateway.close();   // also closes gatewayLedger
        }

        try (var auditor = new Ledger(TigerBeetleContainers.address(TIGERBEETLE))) {
            assertInvariants(auditor, stream);
        } finally {
            settlementLedger.close();
        }
    }

    /** N users, one virtual thread each, firing their scripted actions concurrently. Returns commands durably published. */
    private long storm(String baseUrl) throws Exception {
        var scripts = generateScripts();
        var http = HttpClient.newHttpClient();
        long published = 0;
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var futures = new ArrayList<Future<Long>>();
            for (var user : users()) {
                futures.add(executor.submit(() -> runUser(http, baseUrl, user, scripts.get(user))));
            }
            for (var future : futures) {
                published += future.get(5, TimeUnit.MINUTES);
            }
        }
        System.out.println("storm published " + published + " commands");
        return published;
    }

    /**
     * The whole scenario is a pure function of the seed. What is *not*
     * reproducible is the interleaving — that's the point: the invariants
     * must hold for every interleaving, and each run tries a new one.
     */
    private Map<String, List<Action>> generateScripts() {
        var random = new Random(seed);
        var scripts = new HashMap<String, List<Action>>();
        for (var user : users()) {
            var actions = new ArrayList<Action>(ACTIONS_PER_USER);
            for (int i = 0; i < ACTIONS_PER_USER; i++) {
                var symbol = SYMBOLS.get(random.nextInt(SYMBOLS.size()));
                if (random.nextDouble() < CANCEL_RATIO) {
                    actions.add(new Action(true, symbol, null, 0, 0, random.nextInt(1 << 16)));
                } else {
                    actions.add(new Action(
                            false,
                            symbol,
                            random.nextBoolean() ? Side.BUY : Side.SELL,
                            MID_TICKS.get(symbol) - 5 + random.nextInt(11),
                            1 + random.nextInt(4),
                            0));
                }
            }
            scripts.put(user, actions);
        }
        return scripts;
    }

    private long runUser(HttpClient http, String baseUrl, String user, List<Action> actions) throws Exception {
        long published = 0;
        var myOrders = new ArrayList<PlacedOrder>();
        for (var action : actions) {
            if (action.cancel()) {
                if (myOrders.isEmpty()) {
                    continue;
                }
                var target = myOrders.get(action.cancelPick() % myOrders.size());
                var response = post(http, baseUrl + "/api/cancel", Json.toBytes(Map.of(
                        "orderId", target.orderId(), "userId", user, "symbol", target.symbol())));
                assertEquals(202, response.statusCode(), withSeed("cancel must always publish"));
                published++;
            } else {
                var response = post(http, baseUrl + "/api/orders", Json.toBytes(Map.of(
                        "userId", user, "symbol", action.symbol(), "side", action.side(),
                        "priceTicks", action.priceTicks(), "quantity", action.quantity())));
                if (response.statusCode() == 202) {
                    var orderId = Json.tree(response.body().getBytes()).get("orderId").asText();
                    myOrders.add(new PlacedOrder(orderId, action.symbol()));
                    published++;
                } else {
                    // The only acceptable rejection is the ledger saying no.
                    assertEquals(422, response.statusCode(),
                            withSeed(user + " got an unexpected rejection: " + response.body()));
                }
            }
        }
        return published;
    }

    record FillsStream(List<Fill> fills, long releases) {
    }

    /** Reads `fills` beginning-to-end after the engine is closed: the complete, final stream. */
    private FillsStream readWholeFillsTopic(String bootstrap) {
        var config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        try (var consumer = new KafkaConsumer<String, byte[]>(config)) {
            var partition = new TopicPartition(Topics.FILLS, 0);
            consumer.assign(List.of(partition));
            long end = consumer.endOffsets(List.of(partition)).get(partition);
            consumer.seekToBeginning(List.of(partition));
            var fills = new ArrayList<Fill>();
            long releases = 0;
            while (consumer.position(partition) < end) {
                for (var record : consumer.poll(Duration.ofMillis(250))) {
                    if (FillsTopic.decode(record.value()) instanceof Fill fill) {
                        fills.add(fill);
                    } else {
                        releases++;
                    }
                }
            }
            return new FillsStream(fills, releases);
        }
    }

    private void assertInvariants(Ledger ledger, FillsStream stream) throws Exception {
        // ---- Every fill settled exactly once ----------------------------
        var fillIds = new HashSet<String>();
        for (var fill : stream.fills()) {
            assertTrue(fillIds.add(fill.fillId()),
                    withSeed("engine emitted duplicate fill id " + fill.fillId() + " without restarting"));
            assertTrue(ledger.fillSettled(fill.fillId()),
                    withSeed("fill " + fill.fillId() + " never settled"));
        }
        assertEquals(stream.fills().size(), Metrics.counterTotal("settlement_fills_settled_total"),
                withSeed("settled-fill count must equal the stream's distinct fills"));
        assertEquals(0, Metrics.counterTotal("settlement_duplicates_total"),
                withSeed("nothing restarted, so nothing should have been redelivered"));
        assertEquals(0, Metrics.counterTotal("settlement_failures_total"),
                withSeed("the ledger refused a settlement or void"));

        // ---- Books: internally consistent, and holds match the ledger ---
        var restingBySymbol = new HashMap<String, List<OrderBook.RestingOrderState>>();
        for (var file : listSnapshots()) {
            var snapshot = Json.fromBytes(Files.readAllBytes(file), PartitionSnapshot.class);
            for (var book : snapshot.books()) {
                restingBySymbol.computeIfAbsent(book.symbol(), s -> new ArrayList<>())
                        .addAll(book.restingOrders());
            }
        }
        var expectedHolds = new HashMap<String, Map<String, Long>>();   // user -> asset -> amount
        for (var entry : restingBySymbol.entrySet()) {
            var instrument = Assets.parseSymbol(entry.getKey()).orElseThrow();
            long bestBid = Long.MIN_VALUE;
            long bestAsk = Long.MAX_VALUE;
            for (var order : entry.getValue()) {
                assertTrue(order.remaining() > 0,
                        withSeed("book " + entry.getKey() + " holds an empty order " + order.orderId()));
                String asset;
                long amount;
                if (order.side() == Side.BUY) {
                    bestBid = Math.max(bestBid, order.priceTicks());
                    asset = instrument.quote();
                    amount = order.priceTicks() * order.remaining();
                } else {
                    bestAsk = Math.min(bestAsk, order.priceTicks());
                    asset = instrument.base();
                    amount = order.remaining();
                }
                expectedHolds.computeIfAbsent(order.userId(), u -> new HashMap<>())
                        .merge(asset, amount, Long::sum);
            }
            assertTrue(bestBid < bestAsk,
                    withSeed(entry.getKey() + " book is crossed: bid " + bestBid + " >= ask " + bestAsk));
        }
        for (var user : users()) {
            for (var asset : Assets.all()) {
                long expected = expectedHolds.getOrDefault(user, Map.of()).getOrDefault(asset, 0L);
                assertEquals(expected, ledger.balance(user, asset).reserved(),
                        withSeed("ledger hold for " + user + ":" + asset
                                + " disagrees with the resting orders in the book"));
            }
        }

        // ---- Conservation and no-negative-balances ----------------------
        for (var asset : Assets.all()) {
            assertEquals(0, ledger.escrowPosted(asset),
                    withSeed("settlement is caught up, yet escrow kept " + asset));
            long circulating = 0;
            long heldByOrders = 0;
            for (var user : users()) {
                var balance = ledger.balance(user, asset);
                assertTrue(balance.total() >= 0, withSeed(user + ":" + asset + " total below zero"));
                assertTrue(balance.reserved() >= 0, withSeed(user + ":" + asset + " reserved below zero"));
                assertTrue(balance.available() >= 0, withSeed(user + ":" + asset + " available below zero"));
                circulating += balance.total();
                heldByOrders += balance.reserved();
            }
            assertEquals(ledger.treasuryIssued(asset), circulating,
                    withSeed("conservation broken for " + asset
                            + ": issued != sum of user totals (escrow kept nothing)"));
            assertEquals(heldByOrders, ledger.escrowPending(asset),
                    withSeed("escrow's pending holds disagree with user reservations for " + asset));
        }

        System.out.println("storm invariants held: " + stream.fills().size() + " fills, "
                + stream.releases() + " releases, "
                + restingBySymbol.values().stream().mapToInt(List::size).sum() + " orders left resting");
    }

    private List<Path> listSnapshots() throws Exception {
        try (var files = Files.list(snapshotDir)) {
            return files.filter(f -> f.getFileName().toString().endsWith(".json")).toList();
        }
    }

    private static List<String> users() {
        var users = new ArrayList<String>(USERS);
        for (int i = 0; i < USERS; i++) {
            users.add("storm-user-" + i);
        }
        return users;
    }

    private static HttpResponse<String> post(HttpClient http, String url, byte[] body) throws Exception {
        return http.send(
                HttpRequest.newBuilder(URI.create(url))
                        .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
    }

    private void await(LongSupplier actual, long expected, Duration timeout, String what) {
        var deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            if (actual.getAsLong() == expected) {
                return;
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        fail(withSeed("timed out waiting for " + what + ": expected " + expected + ", saw " + actual.getAsLong()));
    }

    private String withSeed(String message) {
        return message + " [reproduce with -Dstorm.seed=" + seed + "]";
    }
}
