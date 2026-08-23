package dev.rvsiyad.exchange.common;

/**
 * A command flowing gateway -> engine on the `orders` topic, keyed by symbol.
 *
 * Prices are in ticks (integer minor units, e.g. cents) and quantities in lots:
 * money never touches floating point, and the matching hot path stays on longs.
 * For a CANCEL, price and quantity are 0 and only orderId/symbol matter.
 */
public record OrderCommand(
        CommandType type,
        String orderId,
        String userId,
        String symbol,
        Side side,
        long priceTicks,
        long quantity,
        long timestampNanos) {

    public static OrderCommand newOrder(String orderId, String userId, String symbol,
                                        Side side, long priceTicks, long quantity, long timestampNanos) {
        return new OrderCommand(CommandType.NEW, orderId, userId, symbol, side, priceTicks, quantity, timestampNanos);
    }

    public static OrderCommand cancel(String orderId, String userId, String symbol, long timestampNanos) {
        return new OrderCommand(CommandType.CANCEL, orderId, userId, symbol, null, 0, 0, timestampNanos);
    }
}
