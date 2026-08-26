package dev.rvsiyad.exchange.common;

/**
 * Everything the engine publishes on the `fills` topic. Two shapes share the
 * topic (see ReservationRelease for why); consumers dispatch with
 * FillsTopic.decode and switch over the sealed type.
 */
public sealed interface FillsTopicEvent permits Fill, ReservationRelease {
}
