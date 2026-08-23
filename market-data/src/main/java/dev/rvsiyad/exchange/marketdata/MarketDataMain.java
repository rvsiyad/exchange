package dev.rvsiyad.exchange.marketdata;

import dev.rvsiyad.exchange.common.Env;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class MarketDataMain {

    private static final Logger log = LoggerFactory.getLogger(MarketDataMain.class);

    public static void main(String[] args) throws Exception {
        String bootstrap = Env.get("KAFKA_BOOTSTRAP", "localhost:9092");

        log.info("market-data starting");

        try (AdminClient admin = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap))) {
            var topics = admin.listTopics().names().get();
            log.info("connected to Kafka at {}; topics: {}", bootstrap, topics);
        }

        log.info("market-data healthy — WebSocket fanout arrives in session 5");
    }
}
