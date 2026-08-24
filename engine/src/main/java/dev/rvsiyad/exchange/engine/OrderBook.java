package dev.rvsiyad.exchange.engine;

import dev.rvsiyad.exchange.common.CommandType;
import dev.rvsiyad.exchange.common.Fill;
import dev.rvsiyad.exchange.common.OrderCommand;
import dev.rvsiyad.exchange.common.Side;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.HashSet;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

/**
 * A price-time-priority limit order book for one symbol. Pure logic: no I/O, no
 * clock, no randomness — fills are timestamped from the taker's command and fill
 * ids come from a per-book sequence, so replaying the same commands into a fresh
 * book yields byte-identical fills. That determinism is what snapshot+replay
 * recovery relies on.
 *
 * Commands are idempotent under redelivery: a NEW whose orderId has been seen
 * before is a no-op, as is a CANCEL for an unknown or already-gone order.
 *
 * Deliberately naive data structures (TreeMap of price levels, FIFO deque per
 * level, id map for cancel lookup). Cancel unlinks in O(level size); a real
 * engine uses intrusive linked nodes for O(1) — deferred optimization.
 */
public final class OrderBook {

    private final String symbol;
    // Both maps order themselves best-price-first: highest bid, lowest ask.
    private final NavigableMap<Long, Deque<RestingOrder>> bids = new TreeMap<>(Comparator.reverseOrder());
    private final NavigableMap<Long, Deque<RestingOrder>> asks = new TreeMap<>();
    private final Map<String, RestingOrder> byOrderId = new HashMap<>();
    private final Set<String> seenOrderIds = new HashSet<>();
    private long fillSequence;

    public OrderBook(String symbol) {
        this.symbol = symbol;
    }

    public List<Fill> apply(OrderCommand command) {
        if (!symbol.equals(command.symbol())) {
            throw new IllegalArgumentException(
                    "command for " + command.symbol() + " routed to " + symbol + " book");
        }
        if (command.type() == CommandType.CANCEL) {
            cancel(command.orderId());
            return List.of();
        }
        if (!seenOrderIds.add(command.orderId())) {
            return List.of();
        }
        return match(command);
    }

    private List<Fill> match(OrderCommand taker) {
        var fills = new ArrayList<Fill>();
        var opposite = taker.side() == Side.BUY ? asks : bids;
        long remaining = taker.quantity();

        while (remaining > 0 && !opposite.isEmpty() && crosses(taker.side(), taker.priceTicks(), opposite.firstKey())) {
            var level = opposite.firstEntry();
            var maker = level.getValue().peekFirst();
            long traded = Math.min(remaining, maker.remaining);

            fills.add(new Fill(
                    symbol + "-" + ++fillSequence,
                    symbol,
                    taker.orderId(), maker.orderId,
                    taker.userId(), maker.userId,
                    taker.side(),
                    level.getKey(),
                    traded,
                    taker.timestampNanos()));

            remaining -= traded;
            maker.remaining -= traded;
            if (maker.remaining == 0) {
                level.getValue().pollFirst();
                byOrderId.remove(maker.orderId);
            }
            if (level.getValue().isEmpty()) {
                opposite.pollFirstEntry();
            }
        }

        if (remaining > 0) {
            var resting = new RestingOrder(taker.orderId(), taker.userId(), taker.side(), taker.priceTicks(), remaining);
            sideOf(taker.side()).computeIfAbsent(taker.priceTicks(), p -> new ArrayDeque<>()).addLast(resting);
            byOrderId.put(taker.orderId(), resting);
        }
        return fills;
    }

    private static boolean crosses(Side takerSide, long limitTicks, long bestOppositeTicks) {
        return takerSide == Side.BUY ? limitTicks >= bestOppositeTicks : limitTicks <= bestOppositeTicks;
    }

    private void cancel(String orderId) {
        var order = byOrderId.remove(orderId);
        if (order == null) {
            return;
        }
        var levels = sideOf(order.side);
        var queue = levels.get(order.priceTicks);
        queue.remove(order);
        if (queue.isEmpty()) {
            levels.remove(order.priceTicks);
        }
    }

    private NavigableMap<Long, Deque<RestingOrder>> sideOf(Side side) {
        return side == Side.BUY ? bids : asks;
    }

    public Long bestBid() {
        return bids.isEmpty() ? null : bids.firstKey();
    }

    public Long bestAsk() {
        return asks.isEmpty() ? null : asks.firstKey();
    }

    /** Total quantity resting at one price level; 0 if the level does not exist. */
    public long depthAt(Side side, long priceTicks) {
        var queue = sideOf(side).get(priceTicks);
        if (queue == null) {
            return 0;
        }
        long total = 0;
        for (var order : queue) {
            total += order.remaining;
        }
        return total;
    }

    private static final class RestingOrder {
        final String orderId;
        final String userId;
        final Side side;
        final long priceTicks;
        long remaining;

        RestingOrder(String orderId, String userId, Side side, long priceTicks, long remaining) {
            this.orderId = orderId;
            this.userId = userId;
            this.side = side;
            this.priceTicks = priceTicks;
            this.remaining = remaining;
        }
    }
}
