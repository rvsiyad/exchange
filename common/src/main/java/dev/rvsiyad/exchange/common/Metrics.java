package dev.rvsiyad.exchange.common;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.DoubleSupplier;

/**
 * A deliberately tiny Prometheus registry: counters, gauges, and the text
 * exposition format — nothing else. The services need exactly three shapes of
 * fact ("this happened N times", "this is currently X", broken out by label),
 * and hand-rolling them keeps the hot paths on LongAdder and the dependency
 * tree empty. What a real client library adds (histograms, exemplars, protobuf
 * exposition) arrives with the session-7 HdrHistogram benchmark if needed.
 *
 * Registration is idempotent: asking for the same series twice returns the
 * same instance, so call sites just declare what they increment and never
 * coordinate. One process-wide registry, matching Prometheus's own model of
 * one scrape endpoint per process.
 */
public final class Metrics {

    /** family name -> help + series; sorted so scrapes render deterministically. */
    private static final ConcurrentSkipListMap<String, Family> FAMILIES = new ConcurrentSkipListMap<>();

    private Metrics() {
    }

    /** A monotonically increasing count. Prometheus rate() turns it into a per-second panel. */
    public static final class Counter {
        private final LongAdder value = new LongAdder();

        public void increment() {
            value.increment();
        }

        public void add(long amount) {
            value.add(amount);
        }

        public long value() {
            return value.sum();
        }
    }

    /** A value read at scrape time — current state, not history. */
    public static final class Gauge {
        private volatile DoubleSupplier supplier;

        public void set(double value) {
            supplier = () -> value;
        }

        double read() {
            var current = supplier;
            return current == null ? 0 : current.getAsDouble();
        }
    }

    /** Labels are name-value pairs: counter("x_total", "...", "reason", "bad_symbol"). */
    public static Counter counter(String name, String help, String... labels) {
        var series = family(name, help, "counter").series(labels);
        if (series.metric == null) {
            series.metric = new Counter();
        }
        return (Counter) series.metric;
    }

    public static Gauge gauge(String name, String help, String... labels) {
        var series = family(name, help, "gauge").series(labels);
        if (series.metric == null) {
            series.metric = new Gauge();
        }
        return (Gauge) series.metric;
    }

    /** A gauge computed at scrape time, e.g. the size of a live collection. */
    public static void gaugeOf(String name, String help, DoubleSupplier supplier, String... labels) {
        gauge(name, help, labels).supplier = supplier;
    }

    /** The Prometheus text exposition (version 0.0.4) of every registered series. */
    public static String scrape() {
        var out = new StringBuilder();
        for (var familyEntry : FAMILIES.entrySet()) {
            var family = familyEntry.getValue();
            out.append("# HELP ").append(familyEntry.getKey()).append(' ').append(family.help).append('\n');
            out.append("# TYPE ").append(familyEntry.getKey()).append(' ').append(family.type).append('\n');
            for (var seriesEntry : family.series.entrySet()) {
                out.append(familyEntry.getKey()).append(seriesEntry.getKey()).append(' ');
                if (seriesEntry.getValue().metric instanceof Counter counter) {
                    out.append(counter.value());
                } else if (seriesEntry.getValue().metric instanceof Gauge gauge) {
                    out.append(render(gauge.read()));
                }
                out.append('\n');
            }
        }
        return out.toString();
    }

    /** Sum across every labeled series of a counter family; 0 if none registered. */
    public static long counterTotal(String name) {
        var family = FAMILIES.get(name);
        if (family == null) {
            return 0;
        }
        long total = 0;
        for (var series : family.series.values()) {
            if (series.metric instanceof Counter counter) {
                total += counter.value();
            }
        }
        return total;
    }

    /** Test hook: a fresh registry, because the JVM-wide one outlives any one test. */
    public static void clear() {
        FAMILIES.clear();
    }

    private static Family family(String name, String help, String type) {
        var family = FAMILIES.computeIfAbsent(name, n -> new Family(help, type));
        if (!family.type.equals(type)) {
            throw new IllegalArgumentException(name + " is already registered as a " + family.type);
        }
        return family;
    }

    private static String render(double value) {
        return value == Math.rint(value) && !Double.isInfinite(value)
                ? String.valueOf((long) value)
                : String.valueOf(value);
    }

    private static final class Family {
        final String help;
        final String type;
        final ConcurrentSkipListMap<String, Series> series = new ConcurrentSkipListMap<>();

        Family(String help, String type) {
            this.help = help;
            this.type = type;
        }

        Series series(String... labels) {
            return series.computeIfAbsent(labelSet(labels), k -> new Series());
        }
    }

    private static final class Series {
        volatile Object metric;
    }

    /** {"reason","bad_symbol"} -> {reason="bad_symbol"}; empty labels -> empty string. */
    private static String labelSet(String... labels) {
        if (labels.length == 0) {
            return "";
        }
        if (labels.length % 2 != 0) {
            throw new IllegalArgumentException("labels must be name-value pairs");
        }
        var pairs = new ArrayList<String>(labels.length / 2);
        for (int i = 0; i < labels.length; i += 2) {
            pairs.add(labels[i] + "=\"" + labels[i + 1] + "\"");
        }
        return "{" + String.join(",", pairs) + "}";
    }
}
