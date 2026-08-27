package dev.rvsiyad.exchange.ledger;

import dev.rvsiyad.exchange.common.Fill;
import dev.rvsiyad.exchange.common.Side;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The session-4 "scratch REPL against TigerBeetle" captured as tests: watch
 * the database itself refuse an overdraft, walk a pending -> post cycle and a
 * pending -> void cycle by hand, then settle full and partial fills through
 * the linked chain and redeliver one to see determinism eat the duplicate.
 *
 * Ordered because the scenarios share one ledger and build on each other,
 * the way the REPL session would.
 */
@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class LedgerTest {

    @Container
    static final GenericContainer<?> TIGERBEETLE = TigerBeetleContainers.create();

    static Ledger ledger;

    static final long ALICE_USD = 10_000_00;
    static final long BOB_ETH = 10;

    @BeforeAll
    static void bootstrap() {
        ledger = new Ledger(TigerBeetleContainers.address(TIGERBEETLE));
        ledger.ensureVenueAccounts();
        ledger.ensureVenueAccounts();   // idempotent: a second boot must be harmless
        for (var user : new String[]{"alice", "bob", "carol"}) {
            ledger.ensureUserAccounts(user);
        }
        ledger.fund("alice", "USD", ALICE_USD);
        ledger.fund("alice", "USD", ALICE_USD);   // same deterministic id: no double-funding
        ledger.fund("bob", "ETH", BOB_ETH);
        ledger.fund("carol", "USD", 20_000_00);
    }

    @AfterAll
    static void tearDown() {
        ledger.close();
    }

    @Test
    @Order(1)
    void fundingIsIdempotentAndVisible() {
        assertEquals(new Ledger.AssetBalance("USD", ALICE_USD, 0, ALICE_USD), ledger.balance("alice", "USD"));
        assertEquals(new Ledger.AssetBalance("ETH", BOB_ETH, 0, BOB_ETH), ledger.balance("bob", "ETH"));
    }

    @Test
    @Order(2)
    void theDatabaseItselfRejectsOverdrafts() {
        // No application balance check anywhere — debits_must_not_exceed_credits does this.
        assertEquals(Ledger.ReserveResult.INSUFFICIENT_FUNDS,
                ledger.reserve("o-too-big", "alice", "USD", ALICE_USD + 1));
        ledger.ensureUserAccounts("mallory");   // exists, but penniless
        assertEquals(Ledger.ReserveResult.INSUFFICIENT_FUNDS,
                ledger.reserve("o-broke", "mallory", "USD", 1));
        assertEquals(new Ledger.AssetBalance("USD", ALICE_USD, 0, ALICE_USD), ledger.balance("alice", "USD"));
    }

    @Test
    @Order(3)
    void reservingHoldsFundsWithoutMovingThem() {
        assertEquals(Ledger.ReserveResult.RESERVED, ledger.reserve("o-hold", "alice", "USD", 1_000_00));
        assertEquals(new Ledger.AssetBalance("USD", ALICE_USD, 1_000_00, ALICE_USD - 1_000_00),
                ledger.balance("alice", "USD"));
        // Held funds count: alice cannot promise the same dollars twice.
        assertEquals(Ledger.ReserveResult.INSUFFICIENT_FUNDS,
                ledger.reserve("o-again", "alice", "USD", ALICE_USD - 1_000_00 + 1));
    }

    @Test
    @Order(4)
    void voidReleasesTheHoldAndMoneyNeverMoved() {
        assertEquals(Ledger.VoidResult.VOIDED, ledger.voidReservation("o-hold", 0));
        // The identical retry hits the same deterministic id: idempotent, same answer.
        assertEquals(Ledger.VoidResult.VOIDED, ledger.voidReservation("o-hold", 0));
        assertEquals(new Ledger.AssetBalance("USD", ALICE_USD, 0, ALICE_USD), ledger.balance("alice", "USD"));
    }

    @Test
    @Order(5)
    void aFullFillSettlesBothLegsAtomically() {
        // Alice bids 2 ETH limit $21, bob asks 2 ETH at $20; trade at bob's price.
        assertEquals(Ledger.ReserveResult.RESERVED, ledger.reserve("b1", "alice", "USD", 2 * 21_00));
        assertEquals(Ledger.ReserveResult.RESERVED, ledger.reserve("s1", "bob", "ETH", 2));

        var fill = new Fill("ETH-USD-1", "ETH-USD", "b1", "s1", "alice", "bob",
                Side.BUY, 20_00, 2, 21_00, 0, 0, 1L);
        assertEquals(Ledger.SettleResult.SETTLED, ledger.settleFill(fill, 0, 0));

        // Alice paid $40 (the $2 price improvement came straight back), got 2 ETH.
        assertEquals(new Ledger.AssetBalance("USD", ALICE_USD - 40_00, 0, ALICE_USD - 40_00),
                ledger.balance("alice", "USD"));
        assertEquals(new Ledger.AssetBalance("ETH", 2, 0, 2), ledger.balance("alice", "ETH"));
        // Bob delivered 2 ETH, got $40.
        assertEquals(new Ledger.AssetBalance("ETH", BOB_ETH - 2, 0, BOB_ETH - 2), ledger.balance("bob", "ETH"));
        assertEquals(new Ledger.AssetBalance("USD", 40_00, 0, 40_00), ledger.balance("bob", "USD"));
    }

    @Test
    @Order(6)
    void aRedeliveredFillIsANoOp() {
        var fill = new Fill("ETH-USD-1", "ETH-USD", "b1", "s1", "alice", "bob",
                Side.BUY, 20_00, 2, 21_00, 0, 0, 1L);
        assertEquals(Ledger.SettleResult.ALREADY_SETTLED, ledger.settleFill(fill, 0, 0));
        assertEquals(new Ledger.AssetBalance("USD", ALICE_USD - 40_00, 0, ALICE_USD - 40_00),
                ledger.balance("alice", "USD"));
        assertEquals(new Ledger.AssetBalance("USD", 40_00, 0, 40_00), ledger.balance("bob", "USD"));
    }

    @Test
    @Order(7)
    void partialFillsWalkTheReservationGenerations() {
        // Bob asks 5 ETH at $20 and is filled 2, then 3, by different buyers.
        assertEquals(Ledger.ReserveResult.RESERVED, ledger.reserve("s2", "bob", "ETH", 5));
        assertEquals(Ledger.ReserveResult.RESERVED, ledger.reserve("b2", "alice", "USD", 2 * 20_00));
        assertEquals(Ledger.ReserveResult.RESERVED, ledger.reserve("b3", "carol", "USD", 3 * 20_00));
        long bobEth = ledger.balance("bob", "ETH").total();

        var first = new Fill("ETH-USD-2", "ETH-USD", "b2", "s2", "alice", "bob",
                Side.BUY, 20_00, 2, 20_00, 0, 3, 2L);
        assertEquals(Ledger.SettleResult.SETTLED, ledger.settleFill(first, 0, 0));
        // 2 delivered, and the open remainder of 3 is still held — as generation 1.
        assertEquals(new Ledger.AssetBalance("ETH", bobEth - 2, 3, bobEth - 5), ledger.balance("bob", "ETH"));

        var second = new Fill("ETH-USD-3", "ETH-USD", "b3", "s2", "carol", "bob",
                Side.BUY, 20_00, 3, 20_00, 0, 0, 3L);
        assertEquals(Ledger.SettleResult.SETTLED, ledger.settleFill(second, 0, 1));
        assertEquals(new Ledger.AssetBalance("ETH", bobEth - 5, 0, bobEth - 5), ledger.balance("bob", "ETH"));
        assertEquals(new Ledger.AssetBalance("ETH", 3, 0, 3), ledger.balance("carol", "ETH"));
        assertEquals(new Ledger.AssetBalance("USD", 5 * 20_00 + 40_00, 0, 5 * 20_00 + 40_00),
                ledger.balance("bob", "USD"));
    }

    @Test
    @Order(8)
    void cancellingAPartiallyFilledOrderVoidsItsCurrentGeneration() {
        // Bob asks 2 ETH, one fill of 1 advances the reservation to generation 1, then he cancels.
        assertEquals(Ledger.ReserveResult.RESERVED, ledger.reserve("s3", "bob", "ETH", 2));
        assertEquals(Ledger.ReserveResult.RESERVED, ledger.reserve("b4", "alice", "USD", 20_00));
        var fill = new Fill("ETH-USD-4", "ETH-USD", "b4", "s3", "alice", "bob",
                Side.BUY, 20_00, 1, 20_00, 0, 1, 4L);
        assertEquals(Ledger.SettleResult.SETTLED, ledger.settleFill(fill, 0, 0));
        assertEquals(1, ledger.balance("bob", "ETH").reserved());

        // Generation 0 is spent — posting consumed it — so voiding it reports so.
        assertEquals(Ledger.VoidResult.ALREADY_RELEASED, ledger.voidReservation("s3", 0));
        assertEquals(Ledger.VoidResult.VOIDED, ledger.voidReservation("s3", 1));
        assertEquals(0, ledger.balance("bob", "ETH").reserved());
    }

    @Test
    @Order(9)
    void theInvariantReadersBalanceTheWholeBook() {
        // Everything above settled or voided, so escrow keeps nothing...
        assertEquals(0, ledger.escrowPosted("USD"));
        assertEquals(0, ledger.escrowPosted("ETH"));
        assertEquals(0, ledger.escrowPending("USD"));
        assertEquals(0, ledger.escrowPending("ETH"));

        // ...and every unit the treasury issued sits in a user account: conservation.
        for (var asset : new String[]{"USD", "ETH"}) {
            long circulating = 0;
            for (var user : new String[]{"alice", "bob", "carol", "mallory"}) {
                circulating += ledger.balance(user, asset).total();
            }
            assertEquals(ledger.treasuryIssued(asset), circulating,
                    "conservation of " + asset);
        }

        assertTrue(ledger.fillSettled("ETH-USD-1"));
        assertFalse(ledger.fillSettled("ETH-USD-never-happened"));
    }
}
