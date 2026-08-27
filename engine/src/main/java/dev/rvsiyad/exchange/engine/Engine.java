package dev.rvsiyad.exchange.engine;

import dev.rvsiyad.exchange.common.Json;
import dev.rvsiyad.exchange.common.Metrics;
import dev.rvsiyad.exchange.common.OrderCommand;
import dev.rvsiyad.exchange.common.Topics;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * The event-sourced matching engine: one worker thread per `orders` partition,
 * each exclusively owning the books of every symbol routed to it. That is the
 * single-writer principle — ordering and parallelism both come from Kafka
 * partitioning (key = symbol), so no lock ever guards a book.
 *
 * The `orders` topic is the write-ahead log: matching state lives in memory
 * and durability comes from replay. Each worker periodically snapshots its
 * books together with the next offset to consume — one atomically-replaced
 * file per partition — so a restart is load + seek + replay-the-tail instead
 * of replaying history. The offset lives inside the snapshot, never in Kafka,
 * because a committed offset without the matching book state loses orders.
 *
 * Commands replayed after the snapshot point re-emit identical fills (the
 * book is deterministic); downstream consumers dedupe by fill id —
 * at-least-once delivery, exactly-once effect.
 */
public final class Engine implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(Engine.class);

    private final String bootstrapServers;
    private final Path snapshotDir;
    private final int snapshotEveryCommands;
    private final List<Worker> workers = new ArrayList<>();
    private final List<Thread> threads = new ArrayList<>();

    public Engine(String bootstrapServers, Path snapshotDir, int snapshotEveryCommands) {
        this.bootstrapServers = bootstrapServers;
        this.snapshotDir = snapshotDir;
        this.snapshotEveryCommands = snapshotEveryCommands;
    }

    /** What one partition's snapshot file holds: every book plus the offset replay resumes from. */
    record PartitionSnapshot(long nextOffset, List<OrderBook.BookState> books) {
    }

    public void start() {
        int partitions;
        try (var probe = new KafkaConsumer<String, byte[]>(consumerConfig())) {
            partitions = probe.partitionsFor(Topics.ORDERS).size();
        }
        for (int partition = 0; partition < partitions; partition++) {
            var worker = new Worker(partition);
            var thread = new Thread(worker, "engine-partition-" + partition);
            workers.add(worker);
            threads.add(thread);
            thread.start();
        }
        log.info("engine started: {} single-writer workers over `{}`", partitions, Topics.ORDERS);
    }

    @Override
    public void close() {
        workers.forEach(Worker::stop);
        for (var thread : threads) {
            try {
                thread.join(10_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
        log.info("engine stopped");
    }

    private Properties consumerConfig() {
        var config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        // Offsets live in the engine's own snapshots (next PR), never in Kafka:
        // a committed offset without the matching book state would lose orders.
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return config;
    }

    private Properties producerConfig() {
        var config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        return config;
    }

    private final class Worker implements Runnable {

        private final TopicPartition partition;
        private final Path snapshotPath;
        private final KafkaConsumer<String, byte[]> consumer;
        private final KafkaProducer<String, byte[]> producer;
        private final Map<String, OrderBook> books = new HashMap<>();
        private long nextOffset;
        private int commandsSinceSnapshot;
        private volatile boolean running = true;

        // Labeled by partition so the dashboard shows skew directly: a hot
        // symbol is one series running away from the others (the ADR 0001
        // tradeoff, visible instead of theoretical).
        private final Metrics.Counter commandsApplied;
        private final Metrics.Counter fillsEmitted;
        private final Metrics.Counter snapshotsWritten;

        Worker(int partition) {
            this.partition = new TopicPartition(Topics.ORDERS, partition);
            this.snapshotPath = snapshotDir.resolve(Topics.ORDERS + "-" + partition + ".json");
            this.consumer = new KafkaConsumer<>(consumerConfig());
            this.producer = new KafkaProducer<>(producerConfig());
            var label = String.valueOf(partition);
            this.commandsApplied = Metrics.counter(
                    "engine_commands_total", "Order commands applied to a book, by partition", "partition", label);
            this.fillsEmitted = Metrics.counter(
                    "engine_fills_total", "Fills emitted by matching, by partition", "partition", label);
            this.snapshotsWritten = Metrics.counter(
                    "engine_snapshots_total", "Book snapshots written, by partition", "partition", label);
        }

        @Override
        public void run() {
            try {
                consumer.assign(List.of(partition));
                var snapshot = loadSnapshot();
                if (snapshot != null) {
                    snapshot.books().forEach(state -> books.put(state.symbol(), OrderBook.restore(state)));
                    nextOffset = snapshot.nextOffset();
                    consumer.seek(partition, nextOffset);
                    log.info("{}: restored {} books from snapshot, replaying from offset {}",
                            partition, books.size(), nextOffset);
                } else {
                    consumer.seekToBeginning(List.of(partition));
                    log.info("{}: no snapshot, replaying from the beginning", partition);
                }
                while (running) {
                    for (var record : consumer.poll(Duration.ofMillis(250)).records(partition)) {
                        handle(record);
                    }
                }
            } catch (WakeupException e) {
                if (running) {
                    throw e;
                }
            } finally {
                writeSnapshot();
                consumer.close();
                producer.close();
            }
        }

        private void handle(ConsumerRecord<String, byte[]> record) {
            OrderCommand command = null;
            try {
                command = Json.fromBytes(record.value(), OrderCommand.class);
            } catch (RuntimeException e) {
                log.warn("skipping undecodable record at {} offset {}", partition, record.offset());
            }
            if (command != null) {
                var result = books.computeIfAbsent(command.symbol(), OrderBook::new).apply(command);
                commandsApplied.increment();
                fillsEmitted.add(result.fills().size());
                for (var fill : result.fills()) {
                    producer.send(new ProducerRecord<>(Topics.FILLS, fill.symbol(), Json.toBytes(fill)));
                }
                // Releases share the fills topic and key so settlement sees fills
                // and voids for a symbol in the order the engine decided them.
                for (var release : result.releases()) {
                    producer.send(new ProducerRecord<>(Topics.FILLS, release.symbol(), Json.toBytes(release)));
                }
                for (var update : result.bookUpdates()) {
                    producer.send(new ProducerRecord<>(Topics.BOOK_UPDATES, update.symbol(), Json.toBytes(update)));
                }
            }
            nextOffset = record.offset() + 1;
            if (++commandsSinceSnapshot >= snapshotEveryCommands) {
                writeSnapshot();
            }
        }

        private PartitionSnapshot loadSnapshot() {
            if (!Files.exists(snapshotPath)) {
                return null;
            }
            try {
                return Json.fromBytes(Files.readAllBytes(snapshotPath), PartitionSnapshot.class);
            } catch (IOException | RuntimeException e) {
                log.warn("{}: snapshot {} unreadable, falling back to full replay", partition, snapshotPath, e);
                return null;
            }
        }

        /**
         * Fills are flushed before the snapshot is written: state must never
         * claim an offset whose fills could still be lost. The write itself is
         * temp-file + atomic move, so a crash mid-write leaves the old
         * snapshot intact.
         */
        private void writeSnapshot() {
            commandsSinceSnapshot = 0;
            producer.flush();
            var state = new PartitionSnapshot(
                    nextOffset,
                    books.values().stream().map(OrderBook::snapshot).toList());
            try {
                Files.createDirectories(snapshotDir);
                var temp = snapshotPath.resolveSibling(snapshotPath.getFileName() + ".tmp");
                Files.write(temp, Json.toBytes(state));
                Files.move(temp, snapshotPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            } catch (IOException e) {
                throw new UncheckedIOException("failed to write snapshot " + snapshotPath, e);
            }
            snapshotsWritten.increment();
        }

        void stop() {
            running = false;
            consumer.wakeup();
        }
    }
}
