package dev.rvsiyad.exchange.common;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * The venue's asset registry. Every tradable symbol is base-quote ("ETH-USD":
 * you buy ETH, you pay USD), and every asset gets its own TigerBeetle ledger
 * number — transfers can only move value between accounts on the same ledger,
 * so USD can never be credited to a BTC account by construction.
 *
 * A fixed allowlist, not a hash of the asset code: an unknown asset is a
 * rejected order, never a silently minted ledger.
 */
public final class Assets {

    private static final Map<String, Integer> LEDGERS = Map.of(
            "USD", 1,
            "BTC", 2,
            "ETH", 3,
            "SOL", 4);

    /** A tradable symbol split into what is bought (base) and what it is paid with (quote). */
    public record Instrument(String symbol, String base, String quote) {
    }

    private Assets() {
    }

    public static Set<String> all() {
        return LEDGERS.keySet();
    }

    public static boolean isKnown(String asset) {
        return asset != null && LEDGERS.containsKey(asset);
    }

    public static int ledger(String asset) {
        Integer ledger = LEDGERS.get(asset);
        if (ledger == null) {
            throw new IllegalArgumentException("unknown asset: " + asset);
        }
        return ledger;
    }

    /** Empty for anything that is not BASE-QUOTE with both assets in the registry. */
    public static Optional<Instrument> parseSymbol(String symbol) {
        if (symbol == null) {
            return Optional.empty();
        }
        String[] parts = symbol.split("-");
        if (parts.length != 2 || !isKnown(parts[0]) || !isKnown(parts[1]) || parts[0].equals(parts[1])) {
            return Optional.empty();
        }
        return Optional.of(new Instrument(symbol, parts[0], parts[1]));
    }
}
