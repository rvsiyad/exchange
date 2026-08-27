package dev.rvsiyad.exchange.engine;

import dev.rvsiyad.exchange.common.BookUpdate;
import dev.rvsiyad.exchange.common.Fill;
import dev.rvsiyad.exchange.common.Json;
import dev.rvsiyad.exchange.common.Metrics;
import dev.rvsiyad.exchange.common.OrderCommand;
import dev.rvsiyad.exchange.common.Side;
import dev.rvsiyad.exchange.common.Topics;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.redpanda.RedpandaContainer;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end over a real broker: commands in on `orders`, fills and depth
 * deltas out — including replay determinism across an engine restart and
 * survival of an undecodable record.
 */
@Testcontainers
class EngineKafkaTest {

    @Container
    static final RedpandaContainer REDPANDA = new RedpandaContainer("redpandadata/redpanda:v24.2.7");

    static String bootstrap;

    @BeforeAll
    static void createTopics() throws Exception {
        bootstrap = REDPANDA.getBootstrapServers();
        try (var admin = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap))) {
            admin.createTopics(List.of(
                    new NewTopic(Topics.ORDERS, 2, (short) 1),
                    new NewTopic(Topics.FILLS, 1, (short) 1),
                    new NewTopic(Topics.BOOK_UPDATES, 1, (short) 1))).all().get();
        }
    }

    @TempDir
    static Path tempDir;

    /** A fresh snapshot dir per engine instance: this test exercises the no-snapshot full-replay path. */
    private static Engine newEngine(String name) {
        return new Engine(bootstrap, tempDir.resolve(name), Integer.MAX_VALUE);
    }

    @Test
    void matchesOverKafkaAndReplaysDeterministicallyOnRestart() {
        try (var producer = new KafkaProducer<String, byte[]>(producerConfig())) {
            send(producer, OrderCommand.newOrder("s1", "bob", "ETH-USD", Side.SELL, 2000_00, 2, 1));
            send(producer, OrderCommand.newOrder("b1", "alice", "ETH-USD", Side.BUY, 2000_00, 2, 2));
            send(producer, OrderCommand.newOrder("s2", "carol", "SOL-USD", Side.SELL, 150_00, 5, 3));
            // A poison pill on SOL-USD's partition: the worker must skip it and keep going.
            producer.send(new ProducerRecord<>(Topics.ORDERS, "SOL-USD", "not json".getBytes()));
            send(producer, OrderCommand.newOrder("b2", "dave", "SOL-USD", Side.BUY, 150_00, 1, 4));
        }

        List<Fill> firstRun;
        try (var engine = newEngine("first-run")) {
            engine.start();

            firstRun = consume(Topics.FILLS, 2, bytes -> Json.fromBytes(bytes, Fill.class));
            var ethFill = firstRun.stream().filter(f -> f.symbol().equals("ETH-USD")).findFirst().orElseThrow();
            assertEquals("ETH-USD-1", ethFill.fillId());
            assertEquals("b1", ethFill.takerOrderId());
            assertEquals("s1", ethFill.makerOrderId());
            assertEquals(2000_00, ethFill.priceTicks());
            assertEquals(2, ethFill.quantity());
            var solFill = firstRun.stream().filter(f -> f.symbol().equals("SOL-USD")).findFirst().orElseThrow();
            assertEquals("SOL-USD-1", solFill.fillId());
            assertEquals(1, solFill.quantity());

            var updates = consume(Topics.BOOK_UPDATES, 4, bytes -> Json.fromBytes(bytes, BookUpdate.class));
            assertEquals(
                    List.of(
                            new BookUpdate("ETH-USD", Side.SELL, 2000_00, 2, 1),
                            new BookUpdate("ETH-USD", Side.SELL, 2000_00, 0, 2)),
                    updates.stream().filter(u -> u.symbol().equals("ETH-USD")).toList());
            assertEquals(
                    List.of(
                            new BookUpdate("SOL-USD", Side.SELL, 150_00, 5, 3),
                            new BookUpdate("SOL-USD", Side.SELL, 150_00, 4, 4)),
                    updates.stream().filter(u -> u.symbol().equals("SOL-USD")).toList());
        }

        // The workers counted what they just did (summed across partition labels).
        assertTrue(Metrics.counterTotal("engine_commands_total") >= 4,
                "expected at least 4 commands counted, saw " + Metrics.counterTotal("engine_commands_total"));
        assertTrue(Metrics.counterTotal("engine_fills_total") >= 2,
                "expected at least 2 fills counted, saw " + Metrics.counterTotal("engine_fills_total"));

        // A fresh engine replays the whole log and re-emits byte-identical fills:
        // at-least-once by design, deduplicated downstream by deterministic fill id.
        try (var engine = newEngine("second-run")) {
            engine.start();
            var afterRestart = consume(Topics.FILLS, 4, bytes -> Json.fromBytes(bytes, Fill.class));
            var replayed = afterRestart.subList(2, 4);
            assertTrue(replayed.containsAll(firstRun), "replayed fills differ from the original run");
        }
    }

    private static void send(KafkaProducer<String, byte[]> producer, OrderCommand command) {
        producer.send(new ProducerRecord<>(Topics.ORDERS, command.symbol(), Json.toBytes(command)));
    }

    private static <T> List<T> consume(String topic, int expected, Function<byte[], T> decoder) {
        var config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        try (var consumer = new KafkaConsumer<String, byte[]>(config)) {
            var partitions = consumer.partitionsFor(topic).stream()
                    .map(p -> new org.apache.kafka.common.TopicPartition(topic, p.partition()))
                    .toList();
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            var decoded = new ArrayList<T>();
            var deadline = Instant.now().plusSeconds(30);
            while (decoded.size() < expected && Instant.now().isBefore(deadline)) {
                consumer.poll(Duration.ofMillis(250)).forEach(r -> decoded.add(decoder.apply(r.value())));
            }
            assertEquals(expected, decoded.size(), "timed out waiting for records on " + topic);
            return decoded;
        }
    }

    private static Properties producerConfig() {
        var config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        return config;
    }
}
