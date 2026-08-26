package dev.rvsiyad.exchange.engine;

import dev.rvsiyad.exchange.common.BookUpdate;
import dev.rvsiyad.exchange.common.CommandType;
import dev.rvsiyad.exchange.common.Fill;
import dev.rvsiyad.exchange.common.OrderCommand;
import dev.rvsiyad.exchange.common.ReservationRelease;
import dev.rvsiyad.exchange.common.Side;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashSet;
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
 * Each applied command also reports the depth deltas it caused (one BookUpdate
 * per touched price level), so a market-data consumer can maintain a live book
 * without ever seeing this data structure.
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

    /**
     * Everything one command did to the book: the fills it produced, a depth
     * delta per touched price level, and — for a cancel that removed a resting
     * order — the reservation release settlement needs to void the funds hold.
     */
    public record ApplyResult(List<Fill> fills, List<BookUpdate> bookUpdates, List<ReservationRelease> releases) {

        private static final ApplyResult EMPTY = new ApplyResult(List.of(), List.of(), List.of());
    }

    public ApplyResult apply(OrderCommand command) {
        if (!symbol.equals(command.symbol())) {
            throw new IllegalArgumentException(
                    "command for " + command.symbol() + " routed to " + symbol + " book");
        }
        if (command.type() == CommandType.CANCEL) {
            return cancel(command.orderId(), command.timestampNanos());
        }
        if (!seenOrderIds.add(command.orderId())) {
            return ApplyResult.EMPTY;
        }
        return match(command);
    }

    private ApplyResult match(OrderCommand taker) {
        var fills = new ArrayList<Fill>();
        var opposite = taker.side() == Side.BUY ? asks : bids;
        var oppositeSide = taker.side() == Side.BUY ? Side.SELL : Side.BUY;
        var touchedOppositeTicks = new LinkedHashSet<Long>();
        long remaining = taker.quantity();

        while (remaining > 0 && !opposite.isEmpty() && crosses(taker.side(), taker.priceTicks(), opposite.firstKey())) {
            var level = opposite.firstEntry();
            var maker = level.getValue().peekFirst();
            long traded = Math.min(remaining, maker.remaining);
            remaining -= traded;
            maker.remaining -= traded;

            fills.add(new Fill(
                    symbol + "-" + ++fillSequence,
                    symbol,
                    taker.orderId(), maker.orderId,
                    taker.userId(), maker.userId,
                    taker.side(),
                    level.getKey(),
                    traded,
                    taker.priceTicks(),
                    remaining,
                    maker.remaining,
                    taker.timestampNanos()));

            touchedOppositeTicks.add(level.getKey());
            if (maker.remaining == 0) {
                level.getValue().pollFirst();
                byOrderId.remove(maker.orderId);
            }
            if (level.getValue().isEmpty()) {
                opposite.pollFirstEntry();
            }
        }

        var updates = new ArrayList<BookUpdate>();
        for (long ticks : touchedOppositeTicks) {
            updates.add(new BookUpdate(symbol, oppositeSide, ticks, depthAt(oppositeSide, ticks), taker.timestampNanos()));
        }
        if (remaining > 0) {
            var resting = new RestingOrder(taker.orderId(), taker.userId(), taker.side(), taker.priceTicks(), remaining);
            sideOf(taker.side()).computeIfAbsent(taker.priceTicks(), p -> new ArrayDeque<>()).addLast(resting);
            byOrderId.put(taker.orderId(), resting);
            updates.add(new BookUpdate(
                    symbol, taker.side(), taker.priceTicks(), depthAt(taker.side(), taker.priceTicks()), taker.timestampNanos()));
        }
        return new ApplyResult(fills, updates, List.of());
    }

    private static boolean crosses(Side takerSide, long limitTicks, long bestOppositeTicks) {
        return takerSide == Side.BUY ? limitTicks >= bestOppositeTicks : limitTicks <= bestOppositeTicks;
    }

    private ApplyResult cancel(String orderId, long timestampNanos) {
        var order = byOrderId.remove(orderId);
        if (order == null) {
            return ApplyResult.EMPTY;
        }
        var levels = sideOf(order.side);
        var queue = levels.get(order.priceTicks);
        queue.remove(order);
        if (queue.isEmpty()) {
            levels.remove(order.priceTicks);
        }
        return new ApplyResult(
                List.of(),
                List.of(new BookUpdate(symbol, order.side, order.priceTicks, depthAt(order.side, order.priceTicks), timestampNanos)),
                List.of(new ReservationRelease(
                        order.orderId, order.userId, symbol, order.side, order.priceTicks, order.remaining, timestampNanos)));
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

    /** One aggregated price level of book depth. */
    public record Level(long priceTicks, long quantity) {
    }

    /** Aggregated depth for one side, best price first, at most maxLevels levels. */
    public List<Level> depth(Side side, int maxLevels) {
        var levels = new ArrayList<Level>();
        for (var entry : sideOf(side).entrySet()) {
            if (levels.size() == maxLevels) {
                break;
            }
            long total = 0;
            for (var order : entry.getValue()) {
                total += order.remaining;
            }
            levels.add(new Level(entry.getKey(), total));
        }
        return levels;
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

    /**
     * Serializable state of the whole book for snapshot + restore. Resting
     * orders are listed in side, then price, then FIFO order, so restoring
     * preserves time priority exactly.
     */
    public record BookState(String symbol, long fillSequence, Set<String> seenOrderIds,
                            List<RestingOrderState> restingOrders) {
    }

    public record RestingOrderState(String orderId, String userId, Side side, long priceTicks, long remaining) {
    }

    public BookState snapshot() {
        var resting = new ArrayList<RestingOrderState>();
        for (var levels : List.of(bids, asks)) {
            for (var queue : levels.values()) {
                for (var order : queue) {
                    resting.add(new RestingOrderState(
                            order.orderId, order.userId, order.side, order.priceTicks, order.remaining));
                }
            }
        }
        return new BookState(symbol, fillSequence, Set.copyOf(seenOrderIds), resting);
    }

    public static OrderBook restore(BookState state) {
        var book = new OrderBook(state.symbol());
        book.fillSequence = state.fillSequence();
        book.seenOrderIds.addAll(state.seenOrderIds());
        for (var order : state.restingOrders()) {
            var resting = new RestingOrder(
                    order.orderId(), order.userId(), order.side(), order.priceTicks(), order.remaining());
            book.sideOf(order.side()).computeIfAbsent(order.priceTicks(), p -> new ArrayDeque<>()).addLast(resting);
            book.byOrderId.put(order.orderId(), resting);
        }
        return book;
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
