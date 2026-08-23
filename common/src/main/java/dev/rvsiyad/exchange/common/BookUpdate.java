package dev.rvsiyad.exchange.common;

/**
 * A depth delta on the `book-updates` topic: the new aggregate quantity resting
 * at one price level (0 means the level is gone). Consumers rebuild the book by
 * applying deltas to a snapshot — the standard market-data feed pattern.
 */
public record BookUpdate(
        String symbol,
        Side side,
        long priceTicks,
        long newQuantity,
        long timestampNanos) {
}
