package dev.rvsiyad.exchange.settlement;

import dev.rvsiyad.exchange.common.Env;
import dev.rvsiyad.exchange.ledger.Ledger;

public class SettlementMain {

    public static void main(String[] args) throws Exception {
        var ledger = new Ledger(Env.get("TIGERBEETLE_ADDRESS", "3000"));
        // Idempotent: whichever of gateway and settlement boots first creates these.
        ledger.ensureVenueAccounts();

        var settlement = new SettlementService(Env.get("KAFKA_BOOTSTRAP", "localhost:9092"), ledger);
        settlement.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            settlement.close();
            ledger.close();
        }, "settlement-shutdown"));
        Thread.currentThread().join();
    }
}
