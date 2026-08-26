package dev.rvsiyad.exchange.common;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FillsTopicTest {

    @Test
    void dispatchesFillsAndReleasesByShape() {
        var fill = new Fill("f-1", "BTC-USD", "o-1", "o-2", "alice", "bob", Side.BUY, 65_000_00, 3, 65_500_00, 1, 0, 789L);
        var release = new ReservationRelease("o-9", "carol", "BTC-USD", Side.SELL, 66_000_00, 4, 790L);

        assertEquals(fill, FillsTopic.decode(Json.toBytes(fill)));
        assertEquals(release, FillsTopic.decode(Json.toBytes(release)));
    }

    @Test
    void oldFillRecordsWithoutTheNewFieldsStillDecodeAsFills() {
        var legacy = """
                {"fillId":"BTC-USD-1","symbol":"BTC-USD","takerOrderId":"b1","makerOrderId":"s1",
                 "takerUserId":"alice","makerUserId":"bob","takerSide":"BUY",
                 "priceTicks":6500000,"quantity":3,"timestampNanos":789}""";
        var decoded = FillsTopic.decode(legacy.getBytes());
        assertEquals(new Fill("BTC-USD-1", "BTC-USD", "b1", "s1", "alice", "bob", Side.BUY, 65_000_00, 3, 0, 0, 0, 789L), decoded);
    }

    @Test
    void unrecognizedPayloadIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> FillsTopic.decode("{\"what\":1}".getBytes()));
    }
}
