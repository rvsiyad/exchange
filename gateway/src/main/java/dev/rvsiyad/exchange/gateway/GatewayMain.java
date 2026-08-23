package dev.rvsiyad.exchange.gateway;

import dev.rvsiyad.exchange.common.Env;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.DriverManager;
import java.util.Map;

public class GatewayMain {

    private static final Logger log = LoggerFactory.getLogger(GatewayMain.class);

    public static void main(String[] args) throws Exception {
        String bootstrap = Env.get("KAFKA_BOOTSTRAP", "localhost:9092");
        String jdbcUrl = Env.get("POSTGRES_URL", "jdbc:postgresql://localhost:5432/exchange");
        String jdbcUser = Env.get("POSTGRES_USER", "exchange");
        String jdbcPassword = Env.get("POSTGRES_PASSWORD", "exchange");

        log.info("gateway starting");

        try (AdminClient admin = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap))) {
            var topics = admin.listTopics().names().get();
            log.info("connected to Kafka at {}; topics: {}", bootstrap, topics);
        }

        try (var conn = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword);
             var stmt = conn.createStatement();
             var rs = stmt.executeQuery("select version()")) {
            rs.next();
            log.info("connected to Postgres: {}", rs.getString(1).split(" on ")[0]);
        }

        log.info("gateway healthy — REST order entry arrives in session 3");
    }
}
