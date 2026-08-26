package dev.rvsiyad.exchange.ledger;

import dev.rvsiyad.exchange.common.Assets;
import dev.rvsiyad.exchange.common.Fill;
import dev.rvsiyad.exchange.common.Side;

/**
 * A fill re-expressed as money movements. Every trade has exactly one buyer
 * and one seller regardless of who was the taker; the buyer pays quote and
 * receives base, the seller the reverse.
 *
 * The buyer's reservation is sized at the buyer's own limit price (the trade
 * price when the buyer was the maker — makers always trade at their limit),
 * which is why the buyer's limit rides along: after a partial fill the
 * remainder is re-reserved at limit x remaining, not at the trade price.
 */
public record TradeLegs(
        String buyOrderId,
        String buyerUserId,
        long buyerLimitTicks,
        long buyerRemaining,
        String sellOrderId,
        String sellerUserId,
        long sellerRemaining,
        String base,
        String quote,
        long baseAmount,
        long quoteAmount) {

    public static TradeLegs of(Fill fill) {
        var instrument = Assets.parseSymbol(fill.symbol())
                .orElseThrow(() -> new IllegalArgumentException("fill for unknown symbol " + fill.symbol()));
        long quoteAmount = Math.multiplyExact(fill.priceTicks(), fill.quantity());
        if (fill.takerSide() == Side.BUY) {
            return new TradeLegs(
                    fill.takerOrderId(), fill.takerUserId(), fill.takerPriceTicks(), fill.takerRemaining(),
                    fill.makerOrderId(), fill.makerUserId(), fill.makerRemaining(),
                    instrument.base(), instrument.quote(), fill.quantity(), quoteAmount);
        }
        return new TradeLegs(
                fill.makerOrderId(), fill.makerUserId(), fill.priceTicks(), fill.makerRemaining(),
                fill.takerOrderId(), fill.takerUserId(), fill.takerRemaining(),
                instrument.base(), instrument.quote(), fill.quantity(), quoteAmount);
    }
}
