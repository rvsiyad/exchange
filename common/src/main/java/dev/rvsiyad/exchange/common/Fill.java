package dev.rvsiyad.exchange.common;

/**
 * Emitted by the engine on the `fills` topic when two orders cross.
 * fillId is the settlement idempotency anchor: TigerBeetle transfer ids are
 * derived deterministically from it, so a redelivered Fill settles as a no-op.
 *
 * priceTicks is the trade price (always the maker's price); takerPriceTicks is
 * the taker's limit. Settlement needs both plus the post-fill remainders,
 * because reservations are sized at an order's own limit price: each fill
 * posts the cost actually traded and re-reserves limit x remaining for
 * whatever is still open.
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
        long takerPriceTicks,
        long takerRemaining,
        long makerRemaining,
        long timestampNanos) {
}
