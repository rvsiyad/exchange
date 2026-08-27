package dev.rvsiyad.exchange.settlement;

import dev.rvsiyad.exchange.common.Fill;
import dev.rvsiyad.exchange.common.FillsTopic;
import dev.rvsiyad.exchange.common.Metrics;
import dev.rvsiyad.exchange.common.ReservationRelease;
import dev.rvsiyad.exchange.common.Topics;
import dev.rvsiyad.exchange.ledger.Ledger;
import dev.rvsiyad.exchange.ledger.TradeLegs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * The idempotent consumer closing the money loop: fills post reservations and
 * pay out through one linked TigerBeetle chain, releases void them.
 *
 * Nothing here commits offsets and nothing is persisted locally. On every
 * start the service replays `fills` from the beginning: the only local state
 * — which reservation generation each order is on — is a pure projection of
 * the stream, and every replayed settlement collapses into TigerBeetle's
 * deterministic transfer ids as a no-op. At-least-once delivery, exactly-once
 * money movement; the same shape as the engine and market-data, but with real
 * funds at stake.
 *
 * Generations advance in lock-step with the ledger: each settled fill spends
 * the current pending reservation of both orders and (for whatever remains
 * open) creates the next generation inside the same atomic chain, so this
 * counter can never disagree with what the chain actually did.
 */
public final class SettlementService implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(SettlementService.class);

    // The money loop's health in three numbers: how much is settling, how much
    // arrives twice (replay working as designed — nonzero is normal after a
    // restart), and how far behind the engine's clock settlement is running.
    private static final Metrics.Counter FILLS_SETTLED = Metrics.counter(
            "settlement_fills_settled_total", "Fills whose linked transfer chain was applied");
    private static final Metrics.Counter DUPLICATES = Metrics.counter(
            "settlement_duplicates_total", "Redelivered fills dropped by the idempotency layer");
    private static final Metrics.Counter RELEASES_VOIDED = Metrics.counter(
            "settlement_releases_voided_total", "Cancel releases whose reservation was voided");
    private static final Metrics.Counter FAILURES = Metrics.counter(
            "settlement_failures_total", "Fills or releases the ledger refused");
    private static final Metrics.Gauge LAG_SECONDS = Metrics.gauge(
            "settlement_lag_seconds", "Age of the last settled fill: now minus its match timestamp");
    // Every decoded event, duplicates included — this counter reaching the
    // stream's record count is what "settlement has caught up" means, which
    // is exactly the quiescence check the storm test needs.
    private static final Metrics.Counter EVENTS = Metrics.counter(
            "settlement_events_total", "Fills-topic events consumed (fills and releases, duplicates included)");

    private final Ledger ledger;
    private final KafkaConsumer<String, byte[]> consumer;
    private final Thread consumerThread;
    private final Map<String, Long> generations = new HashMap<>();
    private final Set<String> settledFillIds = new HashSet<>();
    private volatile boolean running = true;

    public SettlementService(String bootstrapServers, Ledger ledger) {
        this.ledger = ledger;
        var config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        this.consumer = new KafkaConsumer<>(config);
        this.consumerThread = new Thread(this::consume, "settlement-consumer");
    }

    public void start() {
        consumerThread.start();
        log.info("settlement started: replaying `{}` from the beginning", Topics.FILLS);
    }

    private void consume() {
        try {
            var partitions = new ArrayList<TopicPartition>();
            consumer.partitionsFor(Topics.FILLS)
                    .forEach(p -> partitions.add(new TopicPartition(Topics.FILLS, p.partition())));
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            while (running) {
                for (var record : consumer.poll(Duration.ofMillis(250))) {
                    try {
                        handle(FillsTopic.decode(record.value()));
                    } catch (RuntimeException e) {
                        log.warn("skipping unprocessable record at {}-{} offset {}",
                                record.topic(), record.partition(), record.offset(), e);
                    }
                }
            }
        } catch (WakeupException e) {
            if (running) {
                throw e;
            }
        } finally {
            consumer.close();
        }
    }

    private void handle(dev.rvsiyad.exchange.common.FillsTopicEvent event) {
        EVENTS.increment();
        switch (event) {
            case Fill fill -> settle(fill);
            case ReservationRelease release -> release(release);
        }
    }

    private void settle(Fill fill) {
        // The projection dedupes by fill id, exactly like the tape: a duplicate
        // mid-stream must not advance the generations a second time. A restart
        // starts with an empty set and rebuilds by replaying each fill once.
        if (settledFillIds.contains(fill.fillId())) {
            log.debug("fill {} already processed (duplicate delivery)", fill.fillId());
            DUPLICATES.increment();
            return;
        }
        var legs = TradeLegs.of(fill);
        long buyerGeneration = generations.getOrDefault(legs.buyOrderId(), 0L);
        long sellerGeneration = generations.getOrDefault(legs.sellOrderId(), 0L);
        var result = ledger.settleFill(fill, buyerGeneration, sellerGeneration);
        switch (result) {
            case SETTLED -> {
                FILLS_SETTLED.increment();
                LAG_SECONDS.set((nowNanos() - fill.timestampNanos()) / 1_000_000_000.0);
                log.info("settled {}: {} {} @ {} ({} -> {})",
                        fill.fillId(), fill.quantity(), legs.base(), fill.priceTicks(),
                        legs.sellerUserId(), legs.buyerUserId());
            }
            case ALREADY_SETTLED -> {
                // A replay caught by the ledger's deterministic ids instead of
                // this projection (fresh process, old stream) — still a duplicate.
                DUPLICATES.increment();
                log.debug("fill {} already settled (replay)", fill.fillId());
            }
            case FAILED -> {
                // Typically an order that never passed through the gateway, so no
                // reservation exists. The generation must not advance on a chain
                // that never ran.
                log.error("fill {} could not be settled; leaving generations untouched", fill.fillId());
                FAILURES.increment();
                return;
            }
        }
        settledFillIds.add(fill.fillId());
        advance(legs.buyOrderId(), buyerGeneration, legs.buyerRemaining());
        advance(legs.sellOrderId(), sellerGeneration, legs.sellerRemaining());
    }

    private void advance(String orderId, long generation, long remaining) {
        if (remaining > 0) {
            generations.put(orderId, generation + 1);
        } else {
            generations.remove(orderId);
        }
    }

    private void release(ReservationRelease release) {
        long generation = generations.getOrDefault(release.orderId(), 0L);
        var result = ledger.voidReservation(release.orderId(), generation);
        switch (result) {
            case VOIDED -> {
                RELEASES_VOIDED.increment();
                log.info("voided reservation for cancelled order {} (generation {})",
                        release.orderId(), generation);
            }
            case ALREADY_RELEASED -> log.debug("reservation for {} already released", release.orderId());
            case FAILED -> {
                FAILURES.increment();
                log.error("voiding reservation for {} (generation {}) failed",
                        release.orderId(), generation);
            }
        }
        generations.remove(release.orderId());
    }

    /** Test hook: the projected generation of an order's reservation chain. */
    long generationOf(String orderId) {
        return generations.getOrDefault(orderId, 0L);
    }

    private static long nowNanos() {
        var now = java.time.Instant.now();
        return now.getEpochSecond() * 1_000_000_000L + now.getNano();
    }

    @Override
    public void close() {
        running = false;
        consumer.wakeup();
        try {
            consumerThread.join(10_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
