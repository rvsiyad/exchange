package dev.rvsiyad.exchange.engine;

import dev.rvsiyad.exchange.common.Json;
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
 * The `orders` topic is the write-ahead log and, for now, the only durability:
 * every start seeks to the beginning and replays. Because the book is
 * deterministic, replay rebuilds identical state — and re-emits identical
 * fills, which downstream consumers must (and do, by deterministic fill id)
 * treat idempotently. Snapshot + seek arrives next to bound replay time.
 */
public final class Engine implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(Engine.class);

    private final String bootstrapServers;
    private final List<Worker> workers = new ArrayList<>();
    private final List<Thread> threads = new ArrayList<>();

    public Engine(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
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
        private final KafkaConsumer<String, byte[]> consumer;
        private final KafkaProducer<String, byte[]> producer;
        private final Map<String, OrderBook> books = new HashMap<>();
        private volatile boolean running = true;

        Worker(int partition) {
            this.partition = new TopicPartition(Topics.ORDERS, partition);
            this.consumer = new KafkaConsumer<>(consumerConfig());
            this.producer = new KafkaProducer<>(producerConfig());
        }

        @Override
        public void run() {
            try {
                consumer.assign(List.of(partition));
                consumer.seekToBeginning(List.of(partition));
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
                consumer.close();
                producer.close();
            }
        }

        private void handle(ConsumerRecord<String, byte[]> record) {
            OrderCommand command;
            try {
                command = Json.fromBytes(record.value(), OrderCommand.class);
            } catch (RuntimeException e) {
                log.warn("skipping undecodable record at {} offset {}", partition, record.offset());
                return;
            }
            var result = books.computeIfAbsent(command.symbol(), OrderBook::new).apply(command);
            for (var fill : result.fills()) {
                producer.send(new ProducerRecord<>(Topics.FILLS, fill.symbol(), Json.toBytes(fill)));
            }
            for (var update : result.bookUpdates()) {
                producer.send(new ProducerRecord<>(Topics.BOOK_UPDATES, update.symbol(), Json.toBytes(update)));
            }
        }

        void stop() {
            running = false;
            consumer.wakeup();
        }
    }
}
