package dev.rvsiyad.exchange.ledger;

import com.tigerbeetle.AccountBatch;
import com.tigerbeetle.AccountFlags;
import com.tigerbeetle.Client;
import com.tigerbeetle.CreateAccountResult;
import com.tigerbeetle.CreateTransferResult;
import com.tigerbeetle.CreateTransferResultBatch;
import com.tigerbeetle.IdBatch;
import com.tigerbeetle.TransferBatch;
import com.tigerbeetle.TransferFlags;
import com.tigerbeetle.UInt128;
import dev.rvsiyad.exchange.common.Assets;
import dev.rvsiyad.exchange.common.Fill;
import dev.rvsiyad.exchange.common.LedgerIds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * The venue's money core, on TigerBeetle. The design stance (ADR 0004):
 * invariants live in the database, not in application code.
 *
 * - User and escrow accounts carry debits_must_not_exceed_credits: the ledger
 *   itself rejects any transfer that would overdraw them. There is no balance
 *   check in this codebase to race past, because there is no balance check.
 * - An order reserves its worst-case cost as a PENDING transfer into escrow
 *   (reservation beats check-then-debit: the hold is itself atomic).
 * - A fill settles as one LINKED chain: post both reservations for the traded
 *   amount, re-reserve each remainder at the order's limit, pay both sides
 *   out of escrow. TigerBeetle applies the chain all-or-nothing.
 * - A cancel VOIDs the current reservation generation; the hold evaporates
 *   and money never moved.
 *
 * Every id is deterministic (LedgerIds), so redelivering any of these
 * operations hits TigerBeetle's id uniqueness and becomes a no-op.
 */
public final class Ledger implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(Ledger.class);

    /** Transfer codes: the reservation lifecycle (pending/post/void share one code — post and void must match their pending), escrow payouts, and demo funding. */
    static final int CODE_RESERVATION = 1;
    static final int CODE_PAYOUT = 2;
    static final int CODE_FAUCET = 3;
    static final int CODE_ACCOUNT = 1;

    private final Client client;

    public Ledger(String address) {
        this.client = new Client(UInt128.asBytes(0), new String[]{address});
    }

    public enum ReserveResult {
        RESERVED,
        INSUFFICIENT_FUNDS,
        FAILED
    }

    public enum SettleResult {
        SETTLED,
        ALREADY_SETTLED,
        FAILED
    }

    public enum VoidResult {
        VOIDED,
        ALREADY_RELEASED,
        FAILED
    }

    /** total = what the user owns; reserved = held by open orders; available = total - reserved. */
    public record AssetBalance(String asset, long total, long reserved, long available) {
    }

    /**
     * Escrow and treasury accounts for every asset. Escrow gets the same
     * overdraft protection as users — it can never pay out more than it
     * holds. The treasury deliberately does not: it is the demo's money
     * printer, the one account allowed a net debit balance.
     */
    public void ensureVenueAccounts() {
        var accounts = new AccountBatch(Assets.all().size() * 2);
        for (var asset : Assets.all()) {
            accounts.add();
            accounts.setId(LedgerIds.escrow(asset));
            accounts.setLedger(Assets.ledger(asset));
            accounts.setCode(CODE_ACCOUNT);
            accounts.setFlags(AccountFlags.DEBITS_MUST_NOT_EXCEED_CREDITS);
            accounts.add();
            accounts.setId(LedgerIds.treasury(asset));
            accounts.setLedger(Assets.ledger(asset));
            accounts.setCode(CODE_ACCOUNT);
            accounts.setFlags(AccountFlags.NONE);
        }
        createAccountsIdempotently(accounts, "venue accounts");
    }

    /** One account per asset for the user, each overdraft-protected by the database. */
    public void ensureUserAccounts(String userId) {
        var accounts = new AccountBatch(Assets.all().size());
        for (var asset : Assets.all()) {
            accounts.add();
            accounts.setId(LedgerIds.account(userId, asset));
            accounts.setLedger(Assets.ledger(asset));
            accounts.setCode(CODE_ACCOUNT);
            accounts.setFlags(AccountFlags.DEBITS_MUST_NOT_EXCEED_CREDITS);
        }
        createAccountsIdempotently(accounts, "accounts for " + userId);
    }

    /** Demo funding, treasury -> user. The deterministic id makes restarts double-fund-proof. */
    public void fund(String userId, String asset, long amount) {
        var transfers = new TransferBatch(1);
        transfers.add();
        transfers.setId(LedgerIds.faucet(userId, asset));
        transfers.setDebitAccountId(LedgerIds.treasury(asset));
        transfers.setCreditAccountId(LedgerIds.account(userId, asset));
        transfers.setAmount(amount);
        transfers.setLedger(Assets.ledger(asset));
        transfers.setCode(CODE_FAUCET);
        var errors = failures(client.createTransfers(transfers));
        // Exists is the idempotency working: this exact funding already happened.
        if (errors.stream().anyMatch(e -> e.result() != CreateTransferResult.Exists)) {
            throw new IllegalStateException("funding " + userId + ":" + asset + " failed: " + errors);
        }
    }

    /**
     * Holds an order's worst-case cost as pending generation 0. TigerBeetle
     * answers the only question that matters — "does this user actually have
     * the money, counting every other hold?" — atomically, at the database.
     */
    public ReserveResult reserve(String orderId, String userId, String asset, long amount) {
        var transfers = new TransferBatch(1);
        transfers.add();
        transfers.setId(LedgerIds.reservation(orderId, 0));
        transfers.setDebitAccountId(LedgerIds.account(userId, asset));
        transfers.setCreditAccountId(LedgerIds.escrow(asset));
        transfers.setAmount(amount);
        transfers.setLedger(Assets.ledger(asset));
        transfers.setCode(CODE_RESERVATION);
        transfers.setFlags(TransferFlags.PENDING);
        var errors = failures(client.createTransfers(transfers));
        if (errors.isEmpty() || errors.stream().allMatch(e -> e.result() == CreateTransferResult.Exists)) {
            return ReserveResult.RESERVED;
        }
        if (errors.stream().anyMatch(e -> e.result() == CreateTransferResult.ExceedsCredits)) {
            return ReserveResult.INSUFFICIENT_FUNDS;
        }
        log.error("reserving {} {} for order {} failed: {}", amount, asset, orderId, errors);
        return ReserveResult.FAILED;
    }

    /**
     * Settles one fill as a single linked chain (all-or-nothing):
     *
     *   1. post the buyer's pending reservation for price x quantity
     *   2. re-reserve the buyer's remainder at limit x remaining (generation n+1)
     *   3. post the seller's pending reservation for quantity
     *   4. re-reserve the seller's remainder (generation n+1)
     *   5. pay the base out of escrow to the buyer
     *   6. pay the quote out of escrow to the seller
     *
     * Posting releases whatever the fill did not consume (price improvement),
     * and the re-reserve in the same atomic chain means there is no instant
     * where an open order's funds are unheld. Generations are the caller's
     * (settlement's) cursor over each order's reservation chain.
     */
    public SettleResult settleFill(Fill fill, long buyerGeneration, long sellerGeneration) {
        var legs = TradeLegs.of(fill);
        // Chains are atomic, so any surviving member proves the whole chain
        // already ran — the cheap idempotency check for redelivered fills.
        if (transferExists(LedgerIds.payout(fill.fillId(), "quote"))) {
            return SettleResult.ALREADY_SETTLED;
        }

        var transfers = new TransferBatch(6);

        // 1. Buyer pays quote into escrow by posting the pending hold.
        transfers.add();
        transfers.setId(LedgerIds.settlementPost(fill.fillId(), "quote"));
        transfers.setPendingId(LedgerIds.reservation(legs.buyOrderId(), buyerGeneration));
        transfers.setAmount(legs.quoteAmount());
        transfers.setLedger(Assets.ledger(legs.quote()));
        transfers.setCode(CODE_RESERVATION);
        transfers.setFlags(TransferFlags.POST_PENDING_TRANSFER | TransferFlags.LINKED);

        // 2. Re-hold the buyer's still-open remainder at the buyer's limit.
        if (legs.buyerRemaining() > 0) {
            transfers.add();
            transfers.setId(LedgerIds.reservation(legs.buyOrderId(), buyerGeneration + 1));
            transfers.setDebitAccountId(LedgerIds.account(legs.buyerUserId(), legs.quote()));
            transfers.setCreditAccountId(LedgerIds.escrow(legs.quote()));
            transfers.setAmount(Math.multiplyExact(legs.buyerLimitTicks(), legs.buyerRemaining()));
            transfers.setLedger(Assets.ledger(legs.quote()));
            transfers.setCode(CODE_RESERVATION);
            transfers.setFlags(TransferFlags.PENDING | TransferFlags.LINKED);
        }

        // 3. Seller delivers base into escrow by posting the pending hold.
        transfers.add();
        transfers.setId(LedgerIds.settlementPost(fill.fillId(), "base"));
        transfers.setPendingId(LedgerIds.reservation(legs.sellOrderId(), sellerGeneration));
        transfers.setAmount(legs.baseAmount());
        transfers.setLedger(Assets.ledger(legs.base()));
        transfers.setCode(CODE_RESERVATION);
        transfers.setFlags(TransferFlags.POST_PENDING_TRANSFER | TransferFlags.LINKED);

        // 4. Re-hold the seller's still-open remainder.
        if (legs.sellerRemaining() > 0) {
            transfers.add();
            transfers.setId(LedgerIds.reservation(legs.sellOrderId(), sellerGeneration + 1));
            transfers.setDebitAccountId(LedgerIds.account(legs.sellerUserId(), legs.base()));
            transfers.setCreditAccountId(LedgerIds.escrow(legs.base()));
            transfers.setAmount(legs.sellerRemaining());
            transfers.setLedger(Assets.ledger(legs.base()));
            transfers.setCode(CODE_RESERVATION);
            transfers.setFlags(TransferFlags.PENDING | TransferFlags.LINKED);
        }

        // 5-6. Escrow pays each side what the other delivered. These come
        // after the posts on purpose: escrow is overdraft-protected too, and
        // within a chain balances are checked in order.
        transfers.add();
        transfers.setId(LedgerIds.payout(fill.fillId(), "base"));
        transfers.setDebitAccountId(LedgerIds.escrow(legs.base()));
        transfers.setCreditAccountId(LedgerIds.account(legs.buyerUserId(), legs.base()));
        transfers.setAmount(legs.baseAmount());
        transfers.setLedger(Assets.ledger(legs.base()));
        transfers.setCode(CODE_PAYOUT);
        transfers.setFlags(TransferFlags.LINKED);

        transfers.add();
        transfers.setId(LedgerIds.payout(fill.fillId(), "quote"));
        transfers.setDebitAccountId(LedgerIds.escrow(legs.quote()));
        transfers.setCreditAccountId(LedgerIds.account(legs.sellerUserId(), legs.quote()));
        transfers.setAmount(legs.quoteAmount());
        transfers.setLedger(Assets.ledger(legs.quote()));
        transfers.setCode(CODE_PAYOUT);
        transfers.setFlags(TransferFlags.NONE);

        var errors = failures(client.createTransfers(transfers));
        if (errors.isEmpty()) {
            return SettleResult.SETTLED;
        }
        if (errors.stream().anyMatch(e -> e.result() == CreateTransferResult.Exists)) {
            return SettleResult.ALREADY_SETTLED;
        }
        log.error("settling fill {} failed: {}", fill.fillId(), errors);
        return SettleResult.FAILED;
    }

    /** Voids the order's current reservation generation: the hold is released, money never moved. */
    public VoidResult voidReservation(String orderId, long generation) {
        var transfers = new TransferBatch(1);
        transfers.add();
        transfers.setId(LedgerIds.voidReservation(orderId, generation));
        transfers.setPendingId(LedgerIds.reservation(orderId, generation));
        transfers.setCode(CODE_RESERVATION);
        transfers.setFlags(TransferFlags.VOID_PENDING_TRANSFER);
        var errors = failures(client.createTransfers(transfers));
        if (errors.isEmpty() || errors.stream().allMatch(e -> e.result() == CreateTransferResult.Exists)) {
            return VoidResult.VOIDED;
        }
        var alreadyReleased = errors.stream().allMatch(e ->
                e.result() == CreateTransferResult.PendingTransferAlreadyPosted
                        || e.result() == CreateTransferResult.PendingTransferAlreadyVoided
                        || e.result() == CreateTransferResult.PendingTransferNotFound);
        if (alreadyReleased) {
            return VoidResult.ALREADY_RELEASED;
        }
        log.error("voiding reservation {} generation {} failed: {}", orderId, generation, errors);
        return VoidResult.FAILED;
    }

    public List<AssetBalance> balances(String userId) {
        var assets = List.copyOf(Assets.all());
        var ids = new IdBatch(assets.size());
        for (var asset : assets) {
            ids.add(LedgerIds.account(userId, asset));
        }
        var found = client.lookupAccounts(ids);
        var balances = new ArrayList<AssetBalance>(assets.size());
        while (found.next()) {
            var asset = assetForLedger(found.getLedger());
            long total = found.getCreditsPosted().subtract(found.getDebitsPosted()).longValueExact();
            long reserved = found.getDebitsPending().longValueExact();
            balances.add(new AssetBalance(asset, total, reserved, total - reserved));
        }
        return balances;
    }

    public AssetBalance balance(String userId, String asset) {
        return balances(userId).stream()
                .filter(b -> b.asset().equals(asset))
                .findFirst()
                .orElse(new AssetBalance(asset, 0, 0, 0));
    }

    // Read APIs for the system invariants (the order-storm test asserts them;
    // an auditor would run the same queries): conservation is
    // treasuryIssued == sum of user totals + escrowPosted, and a caught-up
    // system has escrowPosted == 0 — fills route money *through* escrow, but
    // every settlement chain nets it back out.

    /** How much of one asset the treasury has issued into circulation: its net debit balance. */
    public long treasuryIssued(String asset) {
        var view = accountView(LedgerIds.treasury(asset), asset + " treasury");
        return view.debitsPosted() - view.creditsPosted();
    }

    /** Escrow's settled balance; exactly zero whenever settlement is caught up. */
    public long escrowPosted(String asset) {
        var view = accountView(LedgerIds.escrow(asset), asset + " escrow");
        return view.creditsPosted() - view.debitsPosted();
    }

    /** The sum of every open reservation's hold for one asset (escrow's pending credits). */
    public long escrowPending(String asset) {
        return accountView(LedgerIds.escrow(asset), asset + " escrow").creditsPending();
    }

    /**
     * Whether a fill's settlement chain reached the ledger. The chain is
     * atomic, so its last member (the quote payout) existing proves all of it
     * ran — and deterministic ids mean it can never have run twice.
     */
    public boolean fillSettled(String fillId) {
        return transferExists(LedgerIds.payout(fillId, "quote"));
    }

    private record AccountView(long debitsPosted, long creditsPosted, long creditsPending) {
    }

    private AccountView accountView(byte[] accountId, String what) {
        var ids = new IdBatch(1);
        ids.add(accountId);
        var found = client.lookupAccounts(ids);
        if (!found.next()) {
            throw new IllegalStateException(what + " account not found");
        }
        return new AccountView(
                found.getDebitsPosted().longValueExact(),
                found.getCreditsPosted().longValueExact(),
                found.getCreditsPending().longValueExact());
    }

    private record Failure(int index, CreateTransferResult result) {
        @Override
        public String toString() {
            return "[" + index + "] " + result;
        }
    }

    private static List<Failure> failures(CreateTransferResultBatch results) {
        var failures = new ArrayList<Failure>();
        while (results.next()) {
            failures.add(new Failure(results.getIndex(), results.getResult()));
        }
        return failures;
    }

    private void createAccountsIdempotently(AccountBatch accounts, String what) {
        var results = client.createAccounts(accounts);
        while (results.next()) {
            if (results.getResult() != CreateAccountResult.Exists) {
                throw new IllegalStateException(
                        "creating " + what + " failed at [" + results.getIndex() + "]: " + results.getResult());
            }
        }
    }

    private boolean transferExists(byte[] id) {
        var ids = new IdBatch(1);
        ids.add(id);
        return client.lookupTransfers(ids).getLength() > 0;
    }

    private static String assetForLedger(int ledger) {
        return Assets.all().stream()
                .filter(asset -> Assets.ledger(asset) == ledger)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("account on unknown ledger " + ledger));
    }

    @Override
    public void close() {
        client.close();
    }
}
