package dev.rvsiyad.exchange.common;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

/**
 * Deterministic 128-bit TigerBeetle ids, derived by hashing a readable seed
 * string. Determinism is the whole idempotency story: a redelivered fill
 * derives the same transfer ids, so TigerBeetle's id uniqueness turns the
 * duplicate into a no-op — exactly-once settlement effect on at-least-once
 * delivery, with no bookkeeping table anywhere.
 *
 * Reservations are numbered per order ("generation"): the gateway creates
 * generation 0, and each partial fill atomically posts generation n and
 * re-reserves the remainder as generation n+1.
 */
public final class LedgerIds {

    private LedgerIds() {
    }

    /** One account per (user, asset) — alice:USD and alice:ETH are unrelated accounts. */
    public static byte[] account(String userId, String asset) {
        return derive("account:" + userId + ":" + asset);
    }

    /** The venue's per-asset escrow: reservations post into it, payouts leave from it. */
    public static byte[] escrow(String asset) {
        return derive("escrow:" + asset);
    }

    /** Per-asset funding source; the only account allowed a net debit balance. */
    public static byte[] treasury(String asset) {
        return derive("treasury:" + asset);
    }

    /** The pending transfer holding an order's unfilled reservation, one per generation. */
    public static byte[] reservation(String orderId, long generation) {
        return derive("reserve:" + orderId + ":" + generation);
    }

    /** The transfer that posts (fully or partially) a reservation for one fill leg ("base"/"quote"). */
    public static byte[] settlementPost(String fillId, String leg) {
        return derive("post:" + fillId + ":" + leg);
    }

    /** The transfer paying one side of a fill out of escrow ("base"/"quote"). */
    public static byte[] payout(String fillId, String leg) {
        return derive("payout:" + fillId + ":" + leg);
    }

    /** The transfer voiding a cancelled order's current reservation generation. */
    public static byte[] voidReservation(String orderId, long generation) {
        return derive("void:" + orderId + ":" + generation);
    }

    /** Demo funding treasury -> user; deterministic so a restart never double-funds. */
    public static byte[] faucet(String userId, String asset) {
        return derive("faucet:" + userId + ":" + asset);
    }

    private static byte[] derive(String seed) {
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
        byte[] id = Arrays.copyOf(digest.digest(seed.getBytes(StandardCharsets.UTF_8)), 16);
        // TigerBeetle reserves 0 and 2^128-1; a SHA-256 prefix hitting either is
        // cryptographically unreachable, but the guard costs nothing.
        if (allMatch(id, (byte) 0x00)) {
            id[0] = 0x01;
        } else if (allMatch(id, (byte) 0xFF)) {
            id[0] = (byte) 0xFE;
        }
        return id;
    }

    private static boolean allMatch(byte[] id, byte value) {
        for (byte b : id) {
            if (b != value) {
                return false;
            }
        }
        return true;
    }
}
