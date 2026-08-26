package dev.rvsiyad.exchange.common;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AssetsTest {

    @Test
    void parsesKnownSymbolsIntoBaseAndQuote() {
        var instrument = Assets.parseSymbol("ETH-USD").orElseThrow();
        assertEquals("ETH", instrument.base());
        assertEquals("USD", instrument.quote());
        assertEquals("ETH-USD", instrument.symbol());
    }

    @Test
    void rejectsUnknownOrMalformedSymbols() {
        assertTrue(Assets.parseSymbol(null).isEmpty());
        assertTrue(Assets.parseSymbol("ETHUSD").isEmpty());
        assertTrue(Assets.parseSymbol("ETH-USD-PERP").isEmpty());
        assertTrue(Assets.parseSymbol("DOGE-USD").isEmpty());
        assertTrue(Assets.parseSymbol("ETH-DOGE").isEmpty());
        assertTrue(Assets.parseSymbol("USD-USD").isEmpty());
    }

    @Test
    void everyAssetHasItsOwnLedger() {
        var ledgers = Assets.all().stream().map(Assets::ledger).distinct().toList();
        assertEquals(Assets.all().size(), ledgers.size());
        assertTrue(ledgers.stream().allMatch(ledger -> ledger > 0));
    }

    @Test
    void unknownAssetHasNoLedger() {
        assertNotEquals(0, Assets.ledger("USD"));
        assertThrows(IllegalArgumentException.class, () -> Assets.ledger("DOGE"));
    }
}
