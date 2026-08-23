package dev.rvsiyad.exchange.common;

public final class Env {

    private Env() {
    }

    public static String get(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }
}
