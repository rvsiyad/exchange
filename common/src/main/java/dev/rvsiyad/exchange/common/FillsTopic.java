package dev.rvsiyad.exchange.common;

/**
 * Decodes a `fills`-topic record into its concrete event. Dispatch is by
 * distinguishing field rather than a type tag: fills predate releases on this
 * topic, so historical records have no tag to read — presence of `fillId`
 * (never absent from a Fill, never present on a release) is the discriminator
 * that works for old and new records alike.
 */
public final class FillsTopic {

    private FillsTopic() {
    }

    public static FillsTopicEvent decode(byte[] bytes) {
        var node = Json.tree(bytes);
        if (node.has("fillId")) {
            return Json.fromBytes(bytes, Fill.class);
        }
        if (node.has("orderId")) {
            return Json.fromBytes(bytes, ReservationRelease.class);
        }
        throw new IllegalArgumentException("unrecognized fills-topic payload");
    }
}
