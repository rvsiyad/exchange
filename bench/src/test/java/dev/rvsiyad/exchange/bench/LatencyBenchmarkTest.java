package dev.rvsiyad.exchange.bench;

import dev.rvsiyad.exchange.common.Fill;
import dev.rvsiyad.exchange.common.FillsTopic;
import dev.rvsiyad.exchange.common.Json;
import dev.rvsiyad.exchange.common.Metrics;
import dev.rvsiyad.exchange.common.Side;
import dev.rvsiyad.exchange.common.Topics;
import dev.rvsiyad.exchange.engine.Engine;
import dev.rvsiyad.exchange.gateway.GatewayServer;
import dev.rvsiyad.exchange.ledger.Ledger;
import dev.rvsiyad.exchange.ledger.TigerBeetleContainers;
import dev.rvsiyad.exchange.settlement.SettlementService;
import org.HdrHistogram.ConcurrentHistogram;
import org.HdrHistogram.Histogram;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.redpanda.RedpandaContainer;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;
import java.util.function.LongSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * The order→fill latency benchmark: the whole venue — real gateway, real
 * engine, real settlement, real Kafka and TigerBeetle — under a constant
 * offered load, measured the way latency must be measured:
 *
 *   - **Open loop.** Orders are dispatched on a fixed schedule that never
 *     waits for responses, the way real order flow arrives. A closed loop
 *     (send, wait, send) would let a slow system slow the load generator
 *     down and grade its own homework — that is coordinated omission.
 *   - **Latency from the *scheduled* send time**, not the actual one: if the
 *     dispatcher or the system stalls, the stall is charged to the orders
 *     that were due during it, not silently dropped from the sample.
 *   - **Warm-up discarded.** The first `bench.warmup` seconds pay for JIT
 *     compilation, Kafka batching heuristics and page cache and are excluded.
 *   - **Percentiles, not averages**, recorded in HdrHistogram at 3
 *     significant digits.
 *
 * Spans reported:
 *   - order→fill: scheduled dispatch → the taker's first Fill observed on the
 *     `fills` topic by this harness (only crossing orders have one; resting
 *     orders produce no fill by definition).
 *   - HTTP accept: actual send → 202, the synchronous reserve-and-publish path.
 *   - order→settled: the settlement_latency_seconds summary the services
 *     export — gateway-accept to funds moved (whole run, warm-up included).
 *
 * Opt-in via -Dbench=true; tune with -Dbench.rate / bench.warmup /
 * bench.measure / bench.seed. Results print to stdout; docs/benchmarks.md
 * holds methodology and the recorded numbers.
 */
@EnabledIfSystemProperty(named = "bench", matches = "true")
@Testcontainers
class LatencyBenchmarkTest {

    static {
        // Per-fill INFO logging would dominate the run at benchmark rates.
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "warn");
    }

    @Container
    static final RedpandaContainer REDPANDA = new RedpandaContainer("redpandadata/redpanda:v24.2.7");

    @Container
    static final GenericContainer<?> TIGERBEETLE = TigerBeetleContainers.create();

    static final int USERS = 8;
    static final List<String> SYMBOLS = List.of("ETH-USD", "BTC-USD");
    static final Map<String, Long> MID_TICKS = Map.of("ETH-USD", 100L, "BTC-USD", 200L);
    /** Deep pockets on purpose: a rejection is a lost sample, so nothing may bounce off the ledger. */
    static final long FUND_USD = 1_000_000_000L;
    static final long FUND_BASE = 10_000_000L;

    final long ratePerSecond = Long.getLong("bench.rate", 1_000);
    final long warmupSeconds = Long.getLong("bench.warmup", 20);
    final long measureSeconds = Long.getLong("bench.measure", 60);
    final long seed = Long.getLong("bench.seed") != null ? Long.getLong("bench.seed") : new Random().nextLong();

    @TempDir
    Path snapshotDir;

    record Action(String user, String symbol, Side side, long priceTicks, long quantity) {
    }

    @Test
    void orderToFillLatencyUnderConstantLoad() throws Exception {
        System.out.printf("bench: %d orders/s offered, %ds warmup + %ds measured, seed %d%n",
                ratePerSecond, warmupSeconds, measureSeconds, seed);
        var bootstrap = REDPANDA.getBootstrapServers();
        try (var admin = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap))) {
            admin.createTopics(List.of(
                    new NewTopic(Topics.ORDERS, 4, (short) 1),
                    new NewTopic(Topics.FILLS, 1, (short) 1),
                    new NewTopic(Topics.BOOK_UPDATES, 1, (short) 1))).all().get();
        }

        var gatewayLedger = new Ledger(TigerBeetleContainers.address(TIGERBEETLE));
        var settlementLedger = new Ledger(TigerBeetleContainers.address(TIGERBEETLE));
        gatewayLedger.ensureVenueAccounts();
        for (int i = 0; i < USERS; i++) {
            var user = "bench-user-" + i;
            gatewayLedger.ensureUserAccounts(user);
            gatewayLedger.fund(user, "USD", FUND_USD);
            gatewayLedger.fund(user, "ETH", FUND_BASE);
            gatewayLedger.fund(user, "BTC", FUND_BASE);
        }

        var gateway = new GatewayServer(bootstrap, gatewayLedger);
        var baseUrl = "http://localhost:" + gateway.start(0).getAddress().getPort();
        var engine = new Engine(bootstrap, snapshotDir, 1_000);   // production snapshot cadence
        engine.start();
        var settlement = new SettlementService(bootstrap, settlementLedger);
        settlement.start();

        var listener = new FillsListener(bootstrap);
        listener.start();

        Run run;
        try {
            run = dispatch(baseUrl);
            // Quiescence: the engine has applied every accepted command...
            await(() -> Metrics.counterTotal("engine_commands_total"), run.accepted.sum(),
                    Duration.ofSeconds(120), "engine consuming the order log");
        } finally {
            engine.close();   // flushes the fills producer: the stream is complete
        }
        try {
            // ...the harness has observed the whole fills stream...
            long streamEnd = listener.endOffset();
            await(listener.eventsConsumed::get, streamEnd,
                    Duration.ofSeconds(120), "harness consuming the fills stream");
            // ...and settlement has moved every dollar.
            await(() -> Metrics.counterTotal("settlement_events_total"), streamEnd,
                    Duration.ofSeconds(120), "settlement consuming the fills stream");
        } finally {
            listener.close();
            settlement.close();
            gateway.close();
            settlementLedger.close();
        }

        report(run, listener);
    }

    /** One dispatcher thread on the schedule, one virtual thread per in-flight request. */
    private Run dispatch(String baseUrl) throws Exception {
        var actions = script();
        var http = HttpClient.newHttpClient();
        var run = new Run();
        long startNanos = System.nanoTime();
        run.warmupEndNanos = startNanos + warmupSeconds * 1_000_000_000L;
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < actions.length; i++) {
                long scheduledNanos = startNanos + i * 1_000_000_000L / ratePerSecond;
                for (long now = System.nanoTime(); now < scheduledNanos; now = System.nanoTime()) {
                    LockSupport.parkNanos(scheduledNanos - now);
                }
                var action = actions[i];
                executor.submit(() -> place(http, baseUrl, action, scheduledNanos, run));
            }
        }   // close() waits for every in-flight request
        run.dispatchEndNanos = System.nanoTime();
        return run;
    }

    private void place(HttpClient http, String baseUrl, Action action, long scheduledNanos, Run run) {
        boolean measured = scheduledNanos >= run.warmupEndNanos;
        try {
            long sentNanos = System.nanoTime();
            var response = http.send(
                    HttpRequest.newBuilder(URI.create(baseUrl + "/api/orders"))
                            .POST(HttpRequest.BodyPublishers.ofByteArray(Json.toBytes(Map.of(
                                    "userId", action.user(), "symbol", action.symbol(),
                                    "side", action.side(), "priceTicks", action.priceTicks(),
                                    "quantity", action.quantity()))))
                            .build(),
                    HttpResponse.BodyHandlers.ofString());
            long ackNanos = System.nanoTime();
            if (response.statusCode() != 202) {
                run.rejected.increment();
                return;
            }
            run.accepted.increment();
            var orderId = Json.tree(response.body().getBytes()).get("orderId").asText();
            if (measured) {
                run.scheduledByOrder.put(orderId, scheduledNanos);
                run.acceptedMeasured.increment();
                run.httpAccept.recordValue(Math.max(0, ackNanos - sentNanos));
                run.dispatchLateness.recordValue(Math.max(0, sentNanos - scheduledNanos));
            }
        } catch (Exception e) {
            run.errors.increment();
        }
    }

    /** The whole scenario is a pure function of the seed, like the storm — minus cancels. */
    private Action[] script() {
        var random = new Random(seed);
        var actions = new Action[(int) (ratePerSecond * (warmupSeconds + measureSeconds))];
        for (int i = 0; i < actions.length; i++) {
            var symbol = SYMBOLS.get(random.nextInt(SYMBOLS.size()));
            actions[i] = new Action(
                    "bench-user-" + random.nextInt(USERS),
                    symbol,
                    random.nextBoolean() ? Side.BUY : Side.SELL,
                    MID_TICKS.get(symbol) - 5 + random.nextInt(11),
                    1 + random.nextInt(4));
        }
        return actions;
    }

    private void report(Run run, FillsListener listener) {
        var orderToFill = new Histogram(3);
        for (var entry : run.scheduledByOrder.entrySet()) {
            var fillArrival = listener.firstFillNanos.get(entry.getKey());
            if (fillArrival != null) {
                orderToFill.recordValue(Math.max(0, fillArrival - entry.getValue()));
            }
        }
        long fillsInWindow = listener.fillArrivals.stream()
                .filter(nanos -> nanos >= run.warmupEndNanos && nanos < run.dispatchEndNanos)
                .count();
        double window = (run.dispatchEndNanos - run.warmupEndNanos) / 1e9;
        var settled = Metrics.histogram("settlement_latency_seconds", "");

        System.out.printf("%n== bench results (seed %d) ==%n", seed);
        System.out.printf("offered %,d orders/s for %.1fs measured; accepted %,d (%,d measured), rejected %d, errors %d%n",
                ratePerSecond, window, run.accepted.sum(), run.acceptedMeasured.sum(),
                run.rejected.sum(), run.errors.sum());
        System.out.printf("throughput: %,.0f orders/s accepted, %,.0f fills/s emitted%n",
                run.acceptedMeasured.sum() / window, fillsInWindow / window);
        System.out.printf("open-loop fidelity: dispatch lateness p99 %.3fms, max %.3fms%n",
                run.dispatchLateness.getValueAtPercentile(99) / 1e6,
                run.dispatchLateness.getMaxValue() / 1e6);
        print("order→fill (scheduled send → taker's first fill observed)", orderToFill);
        print("http accept (send → 202: reserve + publish)", run.httpAccept);
        System.out.printf("order→settled (service summary, whole run): p50 %.3fms  p99 %.3fms  p99.9 %.3fms  (%d fills)%n",
                settled.quantileSeconds(0.5) * 1e3, settled.quantileSeconds(0.99) * 1e3,
                settled.quantileSeconds(0.999) * 1e3, settled.count());

        assertEquals(0, run.rejected.sum(), withSeed("funding is sized so the ledger never rejects"));
        assertEquals(0, run.errors.sum(), withSeed("no request may error"));
        assertTrue(orderToFill.getTotalCount() > 0, withSeed("no crossing order ever filled"));
    }

    private static void print(String what, Histogram histogram) {
        System.out.printf("%s: p50 %.3fms  p90 %.3fms  p99 %.3fms  p99.9 %.3fms  max %.3fms  (%d samples)%n",
                what,
                histogram.getValueAtPercentile(50) / 1e6,
                histogram.getValueAtPercentile(90) / 1e6,
                histogram.getValueAtPercentile(99) / 1e6,
                histogram.getValueAtPercentile(99.9) / 1e6,
                histogram.getMaxValue() / 1e6,
                histogram.getTotalCount());
    }

    /** Everything one dispatch run accumulates. */
    static final class Run {
        final ConcurrentHashMap<String, Long> scheduledByOrder = new ConcurrentHashMap<>();
        final ConcurrentHistogram httpAccept = new ConcurrentHistogram(3);
        final ConcurrentHistogram dispatchLateness = new ConcurrentHistogram(3);
        final LongAdder accepted = new LongAdder();
        final LongAdder acceptedMeasured = new LongAdder();
        final LongAdder rejected = new LongAdder();
        final LongAdder errors = new LongAdder();
        volatile long warmupEndNanos;
        volatile long dispatchEndNanos;
    }

    /**
     * Tails the `fills` topic and stamps each taker's first fill with the
     * harness clock — the observation end of the order→fill span.
     */
    static final class FillsListener implements AutoCloseable {
        final ConcurrentHashMap<String, Long> firstFillNanos = new ConcurrentHashMap<>();
        final java.util.concurrent.ConcurrentLinkedQueue<Long> fillArrivals = new java.util.concurrent.ConcurrentLinkedQueue<>();
        final AtomicLong eventsConsumed = new AtomicLong();
        private final KafkaConsumer<String, byte[]> consumer;
        private final String bootstrap;
        private final Thread thread;
        private volatile boolean running = true;

        FillsListener(String bootstrap) {
            this.bootstrap = bootstrap;
            var config = new Properties();
            config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
            config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
            config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
            this.consumer = new KafkaConsumer<>(config);
            this.thread = new Thread(this::run, "bench-fills-listener");
        }

        void start() {
            thread.start();
        }

        private void run() {
            var partition = new TopicPartition(Topics.FILLS, 0);
            consumer.assign(List.of(partition));
            consumer.seekToBeginning(List.of(partition));
            while (running) {
                for (var record : consumer.poll(Duration.ofMillis(100))) {
                    long now = System.nanoTime();
                    if (FillsTopic.decode(record.value()) instanceof Fill fill) {
                        firstFillNanos.putIfAbsent(fill.takerOrderId(), now);
                        fillArrivals.add(now);
                    }
                    eventsConsumed.incrementAndGet();
                }
            }
            consumer.close();
        }

        /** The final size of the stream, read on a throwaway consumer (the tailing one owns its thread). */
        long endOffset() {
            var config = new Properties();
            config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
            config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
            try (var probe = new KafkaConsumer<String, byte[]>(config)) {
                var partition = new TopicPartition(Topics.FILLS, 0);
                return probe.endOffsets(List.of(partition)).get(partition);
            }
        }

        @Override
        public void close() {
            running = false;
            try {
                thread.join(10_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
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
        return message + " [reproduce with -Dbench.seed=" + seed + "]";
    }
}
