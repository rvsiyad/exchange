package dev.rvsiyad.exchange.common;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class JsonRoundTripTest {

    @Test
    void orderCommandRoundTrips() {
        var cmd = OrderCommand.newOrder("o-1", "alice", "BTC-USD", Side.BUY, 65_000_00, 5, 123L);
        var back = Json.fromBytes(Json.toBytes(cmd), OrderCommand.class);
        assertEquals(cmd, back);
    }

    @Test
    void cancelCarriesNoPrice() {
        var cancel = OrderCommand.cancel("o-1", "alice", "BTC-USD", 456L);
        var back = Json.fromBytes(Json.toBytes(cancel), OrderCommand.class);
        assertEquals(CommandType.CANCEL, back.type());
        assertNull(back.side());
        assertEquals(0, back.priceTicks());
    }

    @Test
    void fillRoundTrips() {
        var fill = new Fill("f-1", "BTC-USD", "o-1", "o-2", "alice", "bob", Side.BUY, 65_000_00, 3, 65_500_00, 1, 0, 789L);
        assertEquals(fill, Json.fromBytes(Json.toBytes(fill), Fill.class));
    }

    @Test
    void bookUpdateRoundTrips() {
        var update = new BookUpdate("BTC-USD", Side.SELL, 65_100_00, 42, 101112L);
        assertEquals(update, Json.fromBytes(Json.toBytes(update), BookUpdate.class));
    }

    @Test
    void unknownFieldsAreIgnoredForForwardCompatibility() {
        var json = """
                {"symbol":"BTC-USD","side":"BUY","priceTicks":1,"newQuantity":2,"timestampNanos":3,"futureField":"x"}
                """.getBytes();
        var update = Json.fromBytes(json, BookUpdate.class);
        assertEquals("BTC-USD", update.symbol());
    }
}
