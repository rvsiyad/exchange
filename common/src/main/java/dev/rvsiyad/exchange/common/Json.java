package dev.rvsiyad.exchange.common;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * Single shared serde for every event on the wire.
 *
 * JSON first, on purpose: events stay human-readable in Redpanda Console while
 * we build. Unknown fields are ignored so producers can add fields before
 * consumers upgrade. Upgrade path if this were production: Avro/protobuf with a
 * schema registry — smaller payloads and enforced compatibility, at the cost of
 * tooling and opacity.
 */
public final class Json {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private Json() {
    }

    public static byte[] toBytes(Object value) {
        try {
            return MAPPER.writeValueAsBytes(value);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static <T> T fromBytes(byte[] bytes, Class<T> type) {
        try {
            return MAPPER.readValue(bytes, type);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static JsonNode tree(byte[] bytes) {
        try {
            return MAPPER.readTree(bytes);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
