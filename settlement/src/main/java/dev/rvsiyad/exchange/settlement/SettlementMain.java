package dev.rvsiyad.exchange.settlement;

import com.tigerbeetle.Client;
import com.tigerbeetle.IdBatch;
import com.tigerbeetle.UInt128;
import dev.rvsiyad.exchange.common.Env;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SettlementMain {

    private static final Logger log = LoggerFactory.getLogger(SettlementMain.class);

    public static void main(String[] args) throws Exception {
        String bootstrap = Env.get("KAFKA_BOOTSTRAP", "localhost:9092");
        String tbAddress = Env.get("TIGERBEETLE_ADDRESS", "3000");

        log.info("settlement starting");

        try (AdminClient admin = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap))) {
            var topics = admin.listTopics().names().get();
            log.info("connected to Kafka at {}; topics: {}", bootstrap, topics);
        }

        try (Client tb = new Client(UInt128.asBytes(0), new String[]{tbAddress})) {
            IdBatch ids = new IdBatch(1);
            ids.add(UInt128.asBytes(1));
            var accounts = tb.lookupAccounts(ids);
            log.info("connected to TigerBeetle at {}; lookup round-trip ok ({} accounts exist yet)",
                    tbAddress, accounts.getLength());
        }

        log.info("settlement healthy — two-phase transfers arrive in session 4");
    }
}
