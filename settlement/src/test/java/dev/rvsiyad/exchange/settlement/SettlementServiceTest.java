package dev.rvsiyad.exchange.settlement;

import dev.rvsiyad.exchange.common.Fill;
import dev.rvsiyad.exchange.common.Json;
import dev.rvsiyad.exchange.common.ReservationRelease;
import dev.rvsiyad.exchange.common.Side;
import dev.rvsiyad.exchange.common.Topics;
import dev.rvsiyad.exchange.ledger.Ledger;
import dev.rvsiyad.exchange.ledger.TigerBeetleContainers;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.redpanda.RedpandaContainer;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end money flow over real Kafka and TigerBeetle: reservations made
 * the way the gateway makes them, fills and releases arriving the way the
 * engine emits them, and balances proving settlement. Ordered scenarios share
 * the stream, like production would.
 */
@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class SettlementServiceTest {

    @Container
    static final RedpandaContainer REDPANDA = new RedpandaContainer("redpandadata/redpanda:v24.2.7");

    @Container
    static final GenericContainer<?> TIGERBEETLE = TigerBeetleContainers.create();

    static Ledger ledger;
    static SettlementService settlement;
    static KafkaProducer<String, byte[]> producer;

    static final long ALICE_USD = 10_000_00;
    static final long BOB_ETH = 10;

    @BeforeAll
    static void startEverything() throws Exception {
        var bootstrap = REDPANDA.getBootstrapServers();
        try (var admin = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap))) {
            admin.createTopics(List.of(new NewTopic(Topics.FILLS, 1, (short) 1))).all().get();
        }
        ledger = new Ledger(TigerBeetleContainers.address(TIGERBEETLE));
        ledger.ensureVenueAccounts();
        for (var user : new String[]{"alice", "bob", "carol"}) {
            ledger.ensureUserAccounts(user);
        }
        ledger.fund("alice", "USD", ALICE_USD);
        ledger.fund("bob", "ETH", BOB_ETH);
        ledger.fund("carol", "USD", 10_000_00);

        var config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        producer = new KafkaProducer<>(config);

        settlement = new SettlementService(bootstrap, ledger);
        settlement.start();
    }

    @AfterAll
    static void stopEverything() {
        producer.close();
        settlement.close();
        ledger.close();
    }

    @Test
    @Order(1)
    void aFillMovesBothLegsAndLeavesTheRemainderHeld() {
        // As the gateway would: bob asks 5 ETH (holds 5), alice bids 2 limit $21 (holds $42).
        assertEquals(Ledger.ReserveResult.RESERVED, ledger.reserve("s1", "bob", "ETH", 5));
        assertEquals(Ledger.ReserveResult.RESERVED, ledger.reserve("b1", "alice", "USD", 2 * 21_00));

        // As the engine would emit it: trade at bob's $20, alice done, bob 3 open.
        publish(new Fill("ETH-USD-1", "ETH-USD", "b1", "s1", "alice", "bob",
                Side.BUY, 20_00, 2, 21_00, 0, 3, 1L));

        awaitBalance("alice", "ETH", () -> new Ledger.AssetBalance("ETH", 2, 0, 2));
        // Alice paid $40 — the $2 price improvement and the extra hold came back.
        assertEquals(new Ledger.AssetBalance("USD", ALICE_USD - 40_00, 0, ALICE_USD - 40_00),
                ledger.balance("alice", "USD"));
        // Bob delivered 2, was paid $40, and his open remainder of 3 is still held.
        assertEquals(new Ledger.AssetBalance("ETH", BOB_ETH - 2, 3, BOB_ETH - 5), ledger.balance("bob", "ETH"));
        assertEquals(new Ledger.AssetBalance("USD", 40_00, 0, 40_00), ledger.balance("bob", "USD"));
    }

    @Test
    @Order(2)
    void aRedeliveredFillChangesNothing() throws Exception {
        publish(new Fill("ETH-USD-1", "ETH-USD", "b1", "s1", "alice", "bob",
                Side.BUY, 20_00, 2, 21_00, 0, 3, 1L));
        Thread.sleep(1_000);   // give the duplicate time to arrive and be dropped
        assertEquals(new Ledger.AssetBalance("USD", ALICE_USD - 40_00, 0, ALICE_USD - 40_00),
                ledger.balance("alice", "USD"));
        assertEquals(new Ledger.AssetBalance("ETH", BOB_ETH - 2, 3, BOB_ETH - 5), ledger.balance("bob", "ETH"));
    }

    @Test
    @Order(3)
    void theSecondPartialFillWalksTheGenerationChain() {
        assertEquals(Ledger.ReserveResult.RESERVED, ledger.reserve("b2", "carol", "USD", 3 * 20_00));
        publish(new Fill("ETH-USD-2", "ETH-USD", "b2", "s1", "carol", "bob",
                Side.BUY, 20_00, 3, 20_00, 0, 0, 2L));

        awaitBalance("carol", "ETH", () -> new Ledger.AssetBalance("ETH", 3, 0, 3));
        // s1 is now fully filled: nothing held, 5 delivered, $100 received in total.
        assertEquals(new Ledger.AssetBalance("ETH", BOB_ETH - 5, 0, BOB_ETH - 5), ledger.balance("bob", "ETH"));
        assertEquals(new Ledger.AssetBalance("USD", 100_00, 0, 100_00), ledger.balance("bob", "USD"));
    }

    @Test
    @Order(4)
    void aCancelReleaseVoidsTheCurrentGeneration() {
        // bob asks 2 more ETH, gets one fill of 1, then the engine reports the cancel.
        assertEquals(Ledger.ReserveResult.RESERVED, ledger.reserve("s2", "bob", "ETH", 2));
        assertEquals(Ledger.ReserveResult.RESERVED, ledger.reserve("b3", "alice", "USD", 20_00));
        publish(new Fill("ETH-USD-3", "ETH-USD", "b3", "s2", "alice", "bob",
                Side.BUY, 20_00, 1, 20_00, 0, 1, 3L));
        awaitBalance("alice", "ETH", () -> new Ledger.AssetBalance("ETH", 3, 0, 3));
        assertEquals(1, ledger.balance("bob", "ETH").reserved());

        publish(new ReservationRelease("s2", "bob", "ETH-USD", Side.SELL, 20_00, 1, 4L));
        awaitBalance("bob", "ETH", () -> new Ledger.AssetBalance("ETH", BOB_ETH - 6, 0, BOB_ETH - 6));
    }

    @Test
    @Order(5)
    void aRestartReplaysTheWholeStreamWithoutMovingMoney() throws Exception {
        var before = List.of(
                ledger.balance("alice", "USD"), ledger.balance("alice", "ETH"),
                ledger.balance("bob", "USD"), ledger.balance("bob", "ETH"),
                ledger.balance("carol", "USD"), ledger.balance("carol", "ETH"));

        settlement.close();
        settlement = new SettlementService(REDPANDA.getBootstrapServers(), ledger);
        settlement.start();
        Thread.sleep(2_000);   // full replay of everything above

        var after = List.of(
                ledger.balance("alice", "USD"), ledger.balance("alice", "ETH"),
                ledger.balance("bob", "USD"), ledger.balance("bob", "ETH"),
                ledger.balance("carol", "USD"), ledger.balance("carol", "ETH"));
        assertEquals(before, after, "replay must be balance-neutral");
    }

    private static void publish(Object event) {
        try {
            producer.send(new ProducerRecord<>(Topics.FILLS, "ETH-USD", Json.toBytes(event))).get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void awaitBalance(String userId, String asset, Supplier<Ledger.AssetBalance> expected) {
        var deadline = Instant.now().plusSeconds(30);
        while (Instant.now().isBefore(deadline)) {
            if (expected.get().equals(ledger.balance(userId, asset))) {
                return;
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        assertTrue(false, "timed out waiting for " + userId + ":" + asset + " to reach " + expected.get()
                + ", last saw " + ledger.balance(userId, asset));
    }
}
