package dev.rvsiyad.exchange.gateway;

import dev.rvsiyad.exchange.common.CommandType;
import dev.rvsiyad.exchange.common.Json;
import dev.rvsiyad.exchange.common.OrderCommand;
import dev.rvsiyad.exchange.common.Side;
import dev.rvsiyad.exchange.common.Topics;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import dev.rvsiyad.exchange.ledger.Ledger;
import dev.rvsiyad.exchange.ledger.TigerBeetleContainers;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.redpanda.RedpandaContainer;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class GatewayServerTest {

    @Container
    static final RedpandaContainer REDPANDA = new RedpandaContainer("redpandadata/redpanda:v24.2.7");

    @Container
    static final GenericContainer<?> TIGERBEETLE = TigerBeetleContainers.create();

    static GatewayServer gateway;
    static String baseUrl;
    static final HttpClient http = HttpClient.newHttpClient();

    @BeforeAll
    static void startGateway() throws Exception {
        var bootstrap = REDPANDA.getBootstrapServers();
        try (var admin = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap))) {
            admin.createTopics(List.of(new NewTopic(Topics.ORDERS, 2, (short) 1))).all().get();
        }
        var ledger = new Ledger(TigerBeetleContainers.address(TIGERBEETLE));
        ledger.ensureVenueAccounts();
        for (var user : new String[]{"alice", "seller-sam"}) {
            ledger.ensureUserAccounts(user);
        }
        ledger.fund("alice", "USD", 1_000_000_00);
        ledger.fund("seller-sam", "ETH", 50);
        gateway = new GatewayServer(bootstrap, ledger);
        var server = gateway.start(0);
        baseUrl = "http://localhost:" + server.getAddress().getPort();
    }

    @AfterAll
    static void stopGateway() {
        gateway.close();
    }

    @Test
    void acceptedOrderLandsOnTheLogKeyedBySymbol() throws Exception {
        var response = post("/api/orders",
                "{\"userId\":\"alice\",\"symbol\":\"ETH-USD\",\"side\":\"BUY\",\"priceTicks\":200000,\"quantity\":3}");

        assertEquals(202, response.statusCode());
        var orderId = Json.fromBytes(response.body().getBytes(), GatewayServer.AcceptedResponse.class).orderId();
        assertNotNull(orderId);

        var record = awaitOrderRecord(cmd -> cmd.orderId().equals(orderId));
        assertEquals("ETH-USD", record.key());
        var command = Json.fromBytes(record.value(), OrderCommand.class);
        assertEquals(CommandType.NEW, command.type());
        assertEquals("alice", command.userId());
        assertEquals(Side.BUY, command.side());
        assertEquals(200000, command.priceTicks());
        assertEquals(3, command.quantity());
        assertTrue(command.timestampNanos() > 0);
    }

    @Test
    void cancelPublishesACancelCommandForTheSameSymbol() throws Exception {
        var response = post("/api/cancel",
                "{\"orderId\":\"o-123\",\"userId\":\"alice\",\"symbol\":\"SOL-USD\"}");

        assertEquals(202, response.statusCode());
        var record = awaitOrderRecord(cmd -> cmd.type() == CommandType.CANCEL && cmd.orderId().equals("o-123"));
        assertEquals("SOL-USD", record.key());
    }

    @Test
    void acceptedBuyHoldsItsWorstCaseQuoteCost() throws Exception {
        var response = post("/api/orders",
                "{\"userId\":\"alice\",\"symbol\":\"BTC-USD\",\"side\":\"BUY\",\"priceTicks\":5000000,\"quantity\":2}");
        assertEquals(202, response.statusCode());

        // The hold is visible before any matching happened: reservation precedes publish.
        assertEquals(2 * 5000000, balance("alice", "USD").reserved());
        assertEquals(1_000_000_00, balance("alice", "USD").total());
    }

    @Test
    void acceptedSellHoldsTheBaseQuantity() throws Exception {
        var response = post("/api/orders",
                "{\"userId\":\"seller-sam\",\"symbol\":\"ETH-USD\",\"side\":\"SELL\",\"priceTicks\":200000,\"quantity\":7}");
        assertEquals(202, response.statusCode());
        assertEquals(7, balance("seller-sam", "ETH").reserved());
        assertEquals(50, balance("seller-sam", "ETH").total());
    }

    @Test
    void theLedgerRejectsOrdersTheUserCannotAfford() throws Exception {
        // mallory has accounts auto-created on first contact, but no money.
        var response = post("/api/orders",
                "{\"userId\":\"mallory\",\"symbol\":\"ETH-USD\",\"side\":\"BUY\",\"priceTicks\":200000,\"quantity\":1}");
        assertEquals(422, response.statusCode());
        assertEquals(0, balance("mallory", "USD").reserved());
    }

    @Test
    void unknownSymbolsNeverReachLedgerOrLog() throws Exception {
        assertEquals(400, post("/api/orders",
                "{\"userId\":\"alice\",\"symbol\":\"DOGE-USD\",\"side\":\"BUY\",\"priceTicks\":1,\"quantity\":1}")
                .statusCode());
    }

    @Test
    void balancesEndpointRequiresAUser() throws Exception {
        assertEquals(400, http.send(
                HttpRequest.newBuilder(URI.create(baseUrl + "/api/balances")).GET().build(),
                HttpResponse.BodyHandlers.ofString()).statusCode());
    }

    private static Ledger.AssetBalance balance(String userId, String asset) throws Exception {
        var response = http.send(
                HttpRequest.newBuilder(URI.create(baseUrl + "/api/balances?userId=" + userId)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());
        var balances = Json.fromBytes(response.body().getBytes(), GatewayServer.BalancesResponse.class);
        return balances.balances().stream()
                .filter(b -> b.asset().equals(asset))
                .findFirst()
                .orElseThrow();
    }

    @Test
    void invalidOrdersAreRejectedWithoutTouchingTheLog() throws Exception {
        assertEquals(400, post("/api/orders", "{\"userId\":\"alice\"}").statusCode());
        assertEquals(400, post("/api/orders",
                "{\"userId\":\"alice\",\"symbol\":\"ETH-USD\",\"side\":\"BUY\",\"priceTicks\":-5,\"quantity\":1}")
                .statusCode());
        assertEquals(400, post("/api/orders", "not json").statusCode());
        assertEquals(400, post("/api/cancel", "{\"orderId\":\"o-1\"}").statusCode());
        assertEquals(405, http.send(
                HttpRequest.newBuilder(URI.create(baseUrl + "/api/orders")).GET().build(),
                HttpResponse.BodyHandlers.ofString()).statusCode());
    }

    private static HttpResponse<String> post(String path, String body) throws Exception {
        return http.send(
                HttpRequest.newBuilder(URI.create(baseUrl + path))
                        .POST(HttpRequest.BodyPublishers.ofString(body))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
    }

    private static ConsumerRecord<String, byte[]> awaitOrderRecord(
            java.util.function.Predicate<OrderCommand> matcher) {
        var config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, REDPANDA.getBootstrapServers());
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        try (var consumer = new KafkaConsumer<String, byte[]>(config)) {
            var partitions = consumer.partitionsFor(Topics.ORDERS).stream()
                    .map(p -> new TopicPartition(Topics.ORDERS, p.partition()))
                    .toList();
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            var seen = new ArrayList<ConsumerRecord<String, byte[]>>();
            var deadline = Instant.now().plusSeconds(30);
            while (Instant.now().isBefore(deadline)) {
                for (var record : consumer.poll(Duration.ofMillis(250))) {
                    if (matcher.test(Json.fromBytes(record.value(), OrderCommand.class))) {
                        return record;
                    }
                    seen.add(record);
                }
            }
            throw new AssertionError("no matching command on `orders`; saw " + seen.size() + " records");
        }
    }
}
