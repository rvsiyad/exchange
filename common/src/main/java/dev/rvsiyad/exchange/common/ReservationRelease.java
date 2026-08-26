package dev.rvsiyad.exchange.common;

/**
 * Emitted by the engine on the `fills` topic when a cancel removes a resting
 * order: whatever the order still had reserved can be released (settlement
 * voids the pending transfer). Cancels that remove nothing emit nothing.
 *
 * It shares the fills topic on purpose: fills and releases for a symbol must
 * reach settlement in the order the engine decided them (same topic + same
 * key = same partition). On a topic of its own, a release could overtake the
 * fill that preceded it and void a reservation the fill still needs.
 */
public record ReservationRelease(
        String orderId,
        String userId,
        String symbol,
        Side side,
        long priceTicks,
        long remainingQuantity,
        long timestampNanos) implements FillsTopicEvent {
}
