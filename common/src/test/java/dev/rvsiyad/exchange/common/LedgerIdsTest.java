package dev.rvsiyad.exchange.common;

import org.junit.jupiter.api.Test;

import java.util.HexFormat;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class LedgerIdsTest {

    @Test
    void idsAreDeterministic() {
        assertArrayEquals(LedgerIds.account("alice", "USD"), LedgerIds.account("alice", "USD"));
        assertArrayEquals(LedgerIds.reservation("o-1", 3), LedgerIds.reservation("o-1", 3));
        assertArrayEquals(LedgerIds.settlementPost("ETH-USD-7", "quote"), LedgerIds.settlementPost("ETH-USD-7", "quote"));
    }

    @Test
    void distinctSeedsYieldDistinctIds() {
        var ids = List.of(
                LedgerIds.account("alice", "USD"),
                LedgerIds.account("alice", "ETH"),
                LedgerIds.account("bob", "USD"),
                LedgerIds.escrow("USD"),
                LedgerIds.treasury("USD"),
                LedgerIds.reservation("o-1", 0),
                LedgerIds.reservation("o-1", 1),
                LedgerIds.reservation("o-2", 0),
                LedgerIds.settlementPost("ETH-USD-1", "base"),
                LedgerIds.settlementPost("ETH-USD-1", "quote"),
                LedgerIds.payout("ETH-USD-1", "base"),
                LedgerIds.payout("ETH-USD-1", "quote"),
                LedgerIds.voidReservation("o-1", 0),
                LedgerIds.faucet("alice", "USD"));
        var unique = ids.stream().map(HexFormat.of()::formatHex).distinct().count();
        assertEquals(ids.size(), unique);
    }

    @Test
    void idsAre128Bit() {
        assertEquals(16, LedgerIds.account("alice", "USD").length);
        assertEquals(16, LedgerIds.reservation("o-1", 0).length);
    }
}
