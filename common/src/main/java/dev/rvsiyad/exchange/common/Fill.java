package dev.rvsiyad.exchange.common;

/**
 * Emitted by the engine on the `fills` topic when two orders cross.
 * fillId is the settlement idempotency anchor: TigerBeetle transfer ids are
 * derived deterministically from it, so a redelivered Fill settles as a no-op.
 */
public record Fill(
        String fillId,
        String symbol,
        String takerOrderId,
        String makerOrderId,
        String takerUserId,
        String makerUserId,
        Side takerSide,
        long priceTicks,
        long quantity,
        long timestampNanos) {
}
