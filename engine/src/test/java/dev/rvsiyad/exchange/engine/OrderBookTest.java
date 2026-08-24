package dev.rvsiyad.exchange.engine;

import dev.rvsiyad.exchange.common.Fill;
import dev.rvsiyad.exchange.common.OrderCommand;
import dev.rvsiyad.exchange.common.Side;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OrderBookTest {

    private OrderBook book;
    private long clock;

    @BeforeEach
    void setUp() {
        book = new OrderBook("BTC-USD");
        clock = 0;
    }

    private List<Fill> buy(String orderId, String userId, long priceTicks, long quantity) {
        return book.apply(OrderCommand.newOrder(orderId, userId, "BTC-USD", Side.BUY, priceTicks, quantity, ++clock));
    }

    private List<Fill> sell(String orderId, String userId, long priceTicks, long quantity) {
        return book.apply(OrderCommand.newOrder(orderId, userId, "BTC-USD", Side.SELL, priceTicks, quantity, ++clock));
    }

    private List<Fill> cancel(String orderId, String userId) {
        return book.apply(OrderCommand.cancel(orderId, userId, "BTC-USD", ++clock));
    }

    @Test
    void orderOnEmptyBookRestsWithoutFills() {
        var fills = buy("b1", "alice", 100_00, 3);
        assertTrue(fills.isEmpty());
        assertEquals(100_00, book.bestBid());
        assertNull(book.bestAsk());
        assertEquals(3, book.depthAt(Side.BUY, 100_00));
    }

    @Test
    void nonCrossingOrdersLeaveASpread() {
        buy("b1", "alice", 100_00, 3);
        var fills = sell("s1", "bob", 100_50, 2);
        assertTrue(fills.isEmpty());
        assertEquals(100_00, book.bestBid());
        assertEquals(100_50, book.bestAsk());
    }

    @Test
    void exactCrossFillsBothAndEmptiesBook() {
        sell("s1", "bob", 100_00, 2);
        var fills = buy("b1", "alice", 100_00, 2);

        assertEquals(1, fills.size());
        var fill = fills.get(0);
        assertEquals(100_00, fill.priceTicks());
        assertEquals(2, fill.quantity());
        assertEquals("b1", fill.takerOrderId());
        assertEquals("s1", fill.makerOrderId());
        assertEquals("alice", fill.takerUserId());
        assertEquals("bob", fill.makerUserId());
        assertEquals(Side.BUY, fill.takerSide());
        assertNull(book.bestBid());
        assertNull(book.bestAsk());
    }

    @Test
    void takerTradesAtMakersPrice() {
        // Bob advertises $100.50; Dave is willing to pay up to $101 but pays Bob's price.
        sell("s1", "bob", 100_50, 2);
        var fills = buy("b1", "dave", 101_00, 2);
        assertEquals(1, fills.size());
        assertEquals(100_50, fills.get(0).priceTicks());
    }

    @Test
    void incomingLargerThanMakerRestsTheRemainder() {
        sell("s1", "bob", 100_00, 2);
        var fills = buy("b1", "alice", 100_00, 5);

        assertEquals(1, fills.size());
        assertEquals(2, fills.get(0).quantity());
        assertEquals(100_00, book.bestBid());
        assertEquals(3, book.depthAt(Side.BUY, 100_00));
        assertNull(book.bestAsk());
    }

    @Test
    void makerLargerThanIncomingKeepsResting() {
        sell("s1", "bob", 100_00, 5);
        var fills = buy("b1", "alice", 100_00, 2);

        assertEquals(1, fills.size());
        assertEquals(2, fills.get(0).quantity());
        assertEquals(100_00, book.bestAsk());
        assertEquals(3, book.depthAt(Side.SELL, 100_00));
        assertNull(book.bestBid());
    }

    @Test
    void buySweepsMultiplePriceLevels() {
        // The briefing scenario: Bob 2 @ $100.50, Carol 5 @ $101; Dave buys 4 limit $101.
        sell("s1", "bob", 100_50, 2);
        sell("s2", "carol", 101_00, 5);
        var fills = buy("b1", "dave", 101_00, 4);

        assertEquals(2, fills.size());
        assertEquals(100_50, fills.get(0).priceTicks());
        assertEquals(2, fills.get(0).quantity());
        assertEquals("bob", fills.get(0).makerUserId());
        assertEquals(101_00, fills.get(1).priceTicks());
        assertEquals(2, fills.get(1).quantity());
        assertEquals("carol", fills.get(1).makerUserId());
        assertEquals(3, book.depthAt(Side.SELL, 101_00));
        assertNull(book.bestBid());
    }

    @Test
    void sweepStopsAtTheLimitPrice() {
        sell("s1", "bob", 100_50, 2);
        sell("s2", "carol", 101_00, 5);
        var fills = buy("b1", "dave", 100_75, 4);

        assertEquals(1, fills.size());
        assertEquals(100_50, fills.get(0).priceTicks());
        assertEquals(2, fills.get(0).quantity());
        // The unfilled 2 rest as the new best bid, inside Carol's ask.
        assertEquals(100_75, book.bestBid());
        assertEquals(2, book.depthAt(Side.BUY, 100_75));
        assertEquals(101_00, book.bestAsk());
    }

    @Test
    void sellSweepsBidsMirrorOfBuy() {
        // The check-yourself question: Alice bids 3 @ $100, Carol asks 3 @ $101;
        // a sell for 10 limit $99 takes Alice at her price and rests the rest.
        buy("b1", "alice", 100_00, 3);
        sell("s1", "carol", 101_00, 3);
        var fills = sell("s2", "erin", 99_00, 10);

        assertEquals(1, fills.size());
        assertEquals(100_00, fills.get(0).priceTicks());
        assertEquals(3, fills.get(0).quantity());
        assertEquals(Side.SELL, fills.get(0).takerSide());
        assertNull(book.bestBid());
        assertEquals(99_00, book.bestAsk());
        assertEquals(7, book.depthAt(Side.SELL, 99_00));
    }

    @Test
    void fifoWithinAPriceLevel() {
        sell("s1", "bob", 100_00, 2);
        sell("s2", "carol", 100_00, 2);
        var fills = buy("b1", "alice", 100_00, 3);

        assertEquals(2, fills.size());
        assertEquals("bob", fills.get(0).makerUserId());
        assertEquals(2, fills.get(0).quantity());
        assertEquals("carol", fills.get(1).makerUserId());
        assertEquals(1, fills.get(1).quantity());
        assertEquals(1, book.depthAt(Side.SELL, 100_00));
    }

    @Test
    void cancelRemovesRestingOrder() {
        sell("s1", "bob", 100_00, 2);
        cancel("s1", "bob");

        assertNull(book.bestAsk());
        var fills = buy("b1", "alice", 100_00, 2);
        assertTrue(fills.isEmpty());
        assertEquals(100_00, book.bestBid());
    }

    @Test
    void cancelOfOneOrderLeavesRestOfLevelIntact() {
        sell("s1", "bob", 100_00, 2);
        sell("s2", "carol", 100_00, 3);
        cancel("s1", "bob");

        assertEquals(100_00, book.bestAsk());
        assertEquals(3, book.depthAt(Side.SELL, 100_00));
        var fills = buy("b1", "alice", 100_00, 2);
        assertEquals("carol", fills.get(0).makerUserId());
    }

    @Test
    void cancelUnknownOrderIsANoOp() {
        sell("s1", "bob", 100_00, 2);
        cancel("nope", "mallory");
        assertEquals(100_00, book.bestAsk());
    }

    @Test
    void cancelAfterFullFillIsANoOp() {
        sell("s1", "bob", 100_00, 2);
        buy("b1", "alice", 100_00, 2);
        cancel("s1", "bob");
        assertNull(book.bestAsk());
        assertNull(book.bestBid());
    }

    @Test
    void redeliveredNewCommandIsANoOp() {
        var cmd = OrderCommand.newOrder("b1", "alice", "BTC-USD", Side.BUY, 100_00, 3, ++clock);
        book.apply(cmd);
        var fills = book.apply(cmd);

        assertTrue(fills.isEmpty());
        assertEquals(3, book.depthAt(Side.BUY, 100_00));
    }

    @Test
    void reusedOrderIdIsANoOpEvenAfterTheOriginalFilled() {
        sell("s1", "bob", 100_00, 2);
        buy("b1", "alice", 100_00, 2);
        var fills = buy("b1", "alice", 100_00, 2);

        assertTrue(fills.isEmpty());
        assertNull(book.bestBid());
    }

    @Test
    void fillTimestampsComeFromTheTakerCommand() {
        sell("s1", "bob", 100_00, 2);
        var taker = OrderCommand.newOrder("b1", "alice", "BTC-USD", Side.BUY, 100_00, 2, 42_000L);
        var fills = book.apply(taker);
        assertEquals(42_000L, fills.get(0).timestampNanos());
    }

    @Test
    void replayingTheSameCommandsProducesIdenticalFills() {
        var commands = List.of(
                OrderCommand.newOrder("s1", "bob", "BTC-USD", Side.SELL, 100_50, 2, 1),
                OrderCommand.newOrder("s2", "carol", "BTC-USD", Side.SELL, 101_00, 5, 2),
                OrderCommand.newOrder("b1", "dave", "BTC-USD", Side.BUY, 101_00, 4, 3),
                OrderCommand.cancel("s2", "carol", "BTC-USD", 4),
                OrderCommand.newOrder("b2", "alice", "BTC-USD", Side.BUY, 99_00, 1, 5));

        var firstRun = new ArrayList<Fill>();
        var secondRun = new ArrayList<Fill>();
        var bookA = new OrderBook("BTC-USD");
        var bookB = new OrderBook("BTC-USD");
        for (var cmd : commands) {
            firstRun.addAll(bookA.apply(cmd));
        }
        for (var cmd : commands) {
            secondRun.addAll(bookB.apply(cmd));
        }

        assertFalse(firstRun.isEmpty());
        assertEquals(firstRun, secondRun);
    }
}
