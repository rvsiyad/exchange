package dev.rvsiyad.exchange.engine;

import dev.rvsiyad.exchange.common.Fill;
import dev.rvsiyad.exchange.common.Json;
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
import org.apache.kafka.common.TopicPartition;
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

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The kill-and-recover story: an engine with a snapshot restarts by loading
 * state, seeking to the snapshotted offset, and replaying only the tail —
 * nothing lost, and nothing before the snapshot re-emitted.
 */
@Testcontainers
class EngineSnapshotTest {

    @Container
    static final RedpandaContainer REDPANDA = new RedpandaContainer("redpandadata/redpanda:v24.2.7");

    static String bootstrap;

    @TempDir
    static Path snapshotDir;

    @BeforeAll
    static void createTopics() throws Exception {
        bootstrap = REDPANDA.getBootstrapServers();
        try (var admin = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap))) {
            admin.createTopics(List.of(
                    new NewTopic(Topics.ORDERS, 1, (short) 1),
                    new NewTopic(Topics.FILLS, 1, (short) 1),
                    new NewTopic(Topics.BOOK_UPDATES, 1, (short) 1))).all().get();
        }
    }

    @Test
    void restartRecoversFromSnapshotWithoutLosingOrDuplicating() throws Exception {
        // Run 1: alice's big resting bid gets partially taken; the graceful stop snapshots.
        try (var producer = newProducer()) {
            send(producer, OrderCommand.newOrder("b1", "alice", "BTC-USD", Side.BUY, 100_00, 10, 1));
            send(producer, OrderCommand.newOrder("s1", "bob", "BTC-USD", Side.SELL, 100_00, 4, 2));
        }
        try (var engine = new Engine(bootstrap, snapshotDir, Integer.MAX_VALUE)) {
            engine.start();
            var fills = awaitFills(1);
            assertEquals("BTC-USD-1", fills.get(0).fillId());
            assertEquals(4, fills.get(0).quantity());
        }

        var snapshotFile = snapshotDir.resolve(Topics.ORDERS + "-0.json");
        assertTrue(Files.exists(snapshotFile), "graceful stop should have written a snapshot");
        var snapshot = Json.fromBytes(Files.readAllBytes(snapshotFile), Engine.PartitionSnapshot.class);
        assertEquals(2, snapshot.nextOffset());

        // Run 2: a sell that can only match if alice's remaining 6 was restored, not replayed.
        try (var producer = newProducer()) {
            send(producer, OrderCommand.newOrder("s2", "carol", "BTC-USD", Side.SELL, 100_00, 6, 3));
        }
        try (var engine = new Engine(bootstrap, snapshotDir, Integer.MAX_VALUE)) {
            engine.start();
            var fills = awaitFills(2);
            // No duplicate of the pre-snapshot fill — just the original and the new one.
            assertEquals("BTC-USD-1", fills.get(0).fillId());
            assertEquals("BTC-USD-2", fills.get(1).fillId());
            assertEquals("carol", fills.get(1).takerUserId());
            assertEquals("alice", fills.get(1).makerUserId());
            assertEquals(6, fills.get(1).quantity());
        }
    }

    private static void send(KafkaProducer<String, byte[]> producer, OrderCommand command) {
        producer.send(new ProducerRecord<>(Topics.ORDERS, command.symbol(), Json.toBytes(command)));
    }

    private static KafkaProducer<String, byte[]> newProducer() {
        var config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        return new KafkaProducer<>(config);
    }

    /** Reads the fills topic from the beginning and expects it to settle at exactly `expected` records. */
    private static List<Fill> awaitFills(int expected) {
        var config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        try (var consumer = new KafkaConsumer<String, byte[]>(config)) {
            var partition = new TopicPartition(Topics.FILLS, 0);
            consumer.assign(List.of(partition));
            consumer.seekToBeginning(List.of(partition));
            var fills = new ArrayList<Fill>();
            var deadline = Instant.now().plusSeconds(30);
            while (fills.size() < expected && Instant.now().isBefore(deadline)) {
                consumer.poll(Duration.ofMillis(250)).forEach(r -> fills.add(Json.fromBytes(r.value(), Fill.class)));
            }
            assertEquals(expected, fills.size(), "timed out waiting for fills");
            // Settle: a short extra window in which no unexpected duplicates may arrive.
            var settle = Instant.now().plusSeconds(2);
            while (Instant.now().isBefore(settle)) {
                consumer.poll(Duration.ofMillis(250)).forEach(r -> fills.add(Json.fromBytes(r.value(), Fill.class)));
            }
            assertEquals(expected, fills.size(), "unexpected extra fills (duplicates?)");
            return fills;
        }
    }
}
