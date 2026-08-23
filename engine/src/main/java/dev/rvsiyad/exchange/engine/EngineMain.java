package dev.rvsiyad.exchange.engine;

import dev.rvsiyad.exchange.common.Env;
import dev.rvsiyad.exchange.common.Topics;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class EngineMain {

    private static final Logger log = LoggerFactory.getLogger(EngineMain.class);

    public static void main(String[] args) throws Exception {
        String bootstrap = Env.get("KAFKA_BOOTSTRAP", "localhost:9092");

        log.info("engine starting");

        try (AdminClient admin = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap))) {
            var description = admin.describeTopics(java.util.List.of(Topics.ORDERS)).allTopicNames().get();
            int partitions = description.get(Topics.ORDERS).partitions().size();
            log.info("connected to Kafka at {}; `{}` has {} partitions (one book-owning thread each, from session 3)",
                    bootstrap, Topics.ORDERS, partitions);
        }

        log.info("engine healthy — matching logic arrives in session 2");
    }
}
