package com.wizenoze.prometheus;

import static com.wizenoze.prometheus.MetricNameAndGroupingKey.parseMetric;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metered;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import io.prometheus.client.CollectorRegistry;
import java.io.IOException;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A reporter which publishes metric values to a Prometheus Gateway.
 *
 * This implementation is based upon {@code com.codahale.metrics.graphite.GraphiteReporter}.
 *
 * @see <a href="https://github.com/prometheus/pushgateway">Push acceptor for ephemeral and batch
 * jobs</a>
 * @see <a href="https://metrics.dropwizard.io/3.1.0/apidocs/com/codahale/metrics/graphite/GraphiteReporter.html">com.codahale.metrics.graphite.GraphiteReporter</a>
 */
public class PrometheusReporter extends ScheduledReporter {

    private static final Logger LOGGER = LoggerFactory.getLogger(PrometheusReporter.class);

    private static final String JOB_NAME = "storm";

    private final PushGatewayWrapper pushGatewayWrapper;
    private final Clock clock;
    private final String prefix;

    private PrometheusReporter(MetricRegistry registry,
            PushGatewayWrapper pushGatewayWrapper,
            Clock clock,
            String prefix,
            TimeUnit rateUnit,
            TimeUnit durationUnit,
            MetricFilter filter) {
        super(registry, "prometheus-reporter", filter, rateUnit, durationUnit);
        this.pushGatewayWrapper = pushGatewayWrapper;
        this.clock = clock;
        this.prefix = prefix;
    }

    /**
     * Returns a new {@link PrometheusReporter.Builder} for {@link PrometheusReporter}.
     *
     * @param registry the registry to report
     * @return a {@link PrometheusReporter.Builder} instance for a {@link PrometheusReporter}
     */
    public static PrometheusReporter.Builder forRegistry(MetricRegistry registry) {
        return new PrometheusReporter.Builder(registry);
    }

    private static void append(StringBuilder builder, String part) {
        if (part != null && !part.isEmpty()) {
            if (builder.length() > 0) {
                builder.append('_');
            }
            builder.append(part);
        }
    }

    @Override
    public void report(SortedMap<String, Gauge> gauges,
            SortedMap<String, Counter> counters,
            SortedMap<String, Histogram> histograms,
            SortedMap<String, Meter> meters,
            SortedMap<String, Timer> timers) {

        CollectorRegistry registry = new CollectorRegistry();

        for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
            try {
                pushGauge(entry.getKey(), entry.getValue());
            }
            catch (UnsupportedMetricName e) {
                LOGGER.warn("Metric name is unsupported, dropped: {}", entry.getKey());
            }
            catch (IllegalArgumentException e) {
                LOGGER.error("Unable to push metric, dropped: {}", entry.getKey(), e);
            }
        }

        for (Map.Entry<String, Counter> entry : counters.entrySet()) {
            try {
                pushCounter(entry.getKey(), entry.getValue());
            }
            catch (UnsupportedMetricName e) {
                LOGGER.warn("Metric name is unsupported, dropped: {}", entry.getKey());
            }
            catch (IllegalArgumentException e) {
                LOGGER.error("Unable to push metric, dropped: {}", entry.getKey(), e);
            }
        }

        for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
            try {
                pushHistogram(entry.getKey(), entry.getValue());
            }
            catch (UnsupportedMetricName e) {
                LOGGER.warn("Metric name is unsupported, dropped: {}", entry.getKey());
            }
            catch (IllegalArgumentException e) {
                LOGGER.error("Unable to push metric, dropped: {}", entry.getKey(), e);
            }
        }

        for (Map.Entry<String, Meter> entry : meters.entrySet()) {
            try {
                pushMetered(entry.getKey(), entry.getValue());
            }
            catch (UnsupportedMetricName e) {
                LOGGER.warn("Metric name is unsupported, dropped: {}", entry.getKey());
            }
            catch (IllegalArgumentException e) {
                LOGGER.error("Unable to push metric, dropped: {}", entry.getKey(), e);
            }
        }

        for (Map.Entry<String, Timer> entry : timers.entrySet()) {
            try {
                pushTimer(entry.getKey(), entry.getValue());
            }
            catch (UnsupportedMetricName e) {
                LOGGER.warn("Metric name is unsupported, dropped: {}", entry.getKey());
            }
            catch (IllegalArgumentException e) {
                LOGGER.error("Unable to push metric, dropped: {}", entry.getKey(), e);
            }
        }
    }

    private void pushGauge(String originalName, Gauge gauge) {
        MetricNameAndGroupingKey metric = parseMetric(originalName);
        CollectorRegistry registry = new CollectorRegistry();
        registerGauge(registry, prefix(metric.getName()), originalName, gauge.getValue());
        pushMetrics(registry, metric.getGroupingKey());
    }

    private void pushCounter(String originalName, Counter counter) {
        MetricNameAndGroupingKey metric = parseMetric(originalName);
        CollectorRegistry registry = new CollectorRegistry();
        registerGauge(registry, prefix(metric.getName(), "count"), originalName,
                counter.getCount());
        pushMetrics(registry, metric.getGroupingKey());
    }

    private void pushHistogram(String originalName, Histogram histogram) {
        MetricNameAndGroupingKey metric = parseMetric(originalName);

        CollectorRegistry registry = new CollectorRegistry();
        String name = metric.getName();

        registerGauge(registry, prefix(name, "count"), originalName, histogram.getCount());

        Snapshot snapshot = histogram.getSnapshot();
        registerGauge(registry, prefix(name, "max"), originalName, snapshot.getMax());
        registerGauge(registry, prefix(name, "mean"), originalName, snapshot.getMean());
        registerGauge(registry, prefix(name, "min"), originalName, snapshot.getMin());
        registerGauge(registry, prefix(name, "stddev"), originalName, snapshot.getStdDev());
        registerGauge(registry, prefix(name, "p50"), originalName, snapshot.getMedian());
        registerGauge(registry, prefix(name, "p75"), originalName, snapshot.get75thPercentile());
        registerGauge(registry, prefix(name, "p95"), originalName, snapshot.get95thPercentile());
        registerGauge(registry, prefix(name, "p98"), originalName, snapshot.get98thPercentile());
        registerGauge(registry, prefix(name, "p99"), originalName, snapshot.get99thPercentile());
        registerGauge(registry, prefix(name, "p999"), originalName, snapshot.get999thPercentile());

        pushMetrics(registry, metric.getGroupingKey());
    }

    private void pushMetered(String originalName, Metered meter) {
        MetricNameAndGroupingKey metric = parseMetric(originalName);

        CollectorRegistry registry = new CollectorRegistry();
        String name = metric.getName();

        doRegisterMetered(registry, name, originalName, meter);

        pushMetrics(registry, metric.getGroupingKey());
    }

    private void pushTimer(String originalName, Timer timer) {
        MetricNameAndGroupingKey metric = parseMetric(originalName);

        CollectorRegistry registry = new CollectorRegistry();
        String name = metric.getName();
        Snapshot snapshot = timer.getSnapshot();

        registerGauge(registry, prefix(name, "max"), originalName,
                convertDuration(snapshot.getMax()));
        registerGauge(registry, prefix(name, "mean"), originalName,
                convertDuration(snapshot.getMean()));
        registerGauge(registry, prefix(name, "min"), originalName,
                convertDuration(snapshot.getMin()));
        registerGauge(registry, prefix(name, "stddev"), originalName,
                convertDuration(snapshot.getStdDev()));
        registerGauge(registry, prefix(name, "p50"), originalName,
                convertDuration(snapshot.getMedian()));
        registerGauge(registry, prefix(name, "p75"), originalName,
                convertDuration(snapshot.get75thPercentile()));
        registerGauge(registry, prefix(name, "p95"), originalName,
                convertDuration(snapshot.get95thPercentile()));
        registerGauge(registry, prefix(name, "p98"), originalName,
                convertDuration(snapshot.get98thPercentile()));
        registerGauge(registry, prefix(name, "p99"), originalName,
                convertDuration(snapshot.get99thPercentile()));
        registerGauge(registry, prefix(name, "p999"), originalName,
                convertDuration(snapshot.get999thPercentile()));

        doRegisterMetered(registry, name, originalName, timer);

        pushMetrics(registry, metric.getGroupingKey());
    }

    private void doRegisterMetered(CollectorRegistry registry, String name, String originalName,
            Metered meter) {
        registerGauge(registry, prefix(name, "count"), originalName, meter.getCount());
        registerGauge(registry, prefix(name, "m1_rate"), originalName,
                convertRate(meter.getOneMinuteRate()));
        registerGauge(registry, prefix(name, "m5_rate"), originalName,
                convertRate(meter.getFiveMinuteRate()));
        registerGauge(registry, prefix(name, "m15_rate"), originalName,
                convertRate(meter.getFifteenMinuteRate()));
        registerGauge(registry, prefix(name, "mean_rate"), originalName,
                convertRate(meter.getMeanRate()));
    }

    private void registerGauge(CollectorRegistry registry, String name, String help, Number value) {
        assert (value != null);

        io.prometheus.client.Gauge gauge = io.prometheus.client.Gauge.build()
                .name(name).help(help).register(registry);

        gauge.set(value.doubleValue());
    }

    private void registerGauge(CollectorRegistry registry, String name, String help, Object value) {
        assert (value instanceof Number);
        registerGauge(registry, name, help, (Number) value);
    }

    private String prefix(String... components) {
        final StringBuilder builder = new StringBuilder();

        append(builder, prefix);

        if (components != null) {
            for (String s : components) {
                append(builder, s);
            }
        }

        return builder.toString();
    }

    private void pushMetrics(CollectorRegistry registry, Map<String, String> groupingKey) {
        try {
            pushGatewayWrapper.pushAdd(registry, JOB_NAME, groupingKey);
        } catch (IOException e) {
            LOGGER.error("Unable to push to Prometheus", e);
        }
    }

    /**
     * A builder for {@link PrometheusReporter} instances. Defaults to not using a prefix, using the
     * default clock, converting rates to events/second, converting durations to milliseconds, and
     * not filtering metrics.
     */
    public static class Builder {

        private final MetricRegistry registry;
        private Clock clock;
        private String prefix;
        private TimeUnit rateUnit;
        private TimeUnit durationUnit;
        private MetricFilter filter;

        private Builder(MetricRegistry registry) {
            this.registry = registry;
            this.clock = Clock.defaultClock();
            this.prefix = null;
            this.rateUnit = TimeUnit.SECONDS;
            this.durationUnit = TimeUnit.MILLISECONDS;
            this.filter = MetricFilter.ALL;
        }

        /**
         * Use the given {@link Clock} instance for the time.
         *
         * @param clock a {@link Clock} instance
         * @return {@code this}
         */
        public PrometheusReporter.Builder withClock(Clock clock) {
            this.clock = clock;
            return this;
        }

        /**
         * Prefix all metric names with the given string.
         *
         * @param prefix the prefix for all metric names
         * @return {@code this}
         */
        public PrometheusReporter.Builder prefixedWith(String prefix) {
            this.prefix = prefix;
            return this;
        }

        /**
         * Convert rates to the given time unit.
         *
         * @param rateUnit a unit of time
         * @return {@code this}
         */
        public PrometheusReporter.Builder convertRatesTo(TimeUnit rateUnit) {
            this.rateUnit = rateUnit;
            return this;
        }

        /**
         * Convert durations to the given time unit.
         *
         * @param durationUnit a unit of time
         * @return {@code this}
         */
        public PrometheusReporter.Builder convertDurationsTo(TimeUnit durationUnit) {
            this.durationUnit = durationUnit;
            return this;
        }

        /**
         * Only report metrics which match the given filter.
         *
         * @param filter a {@link MetricFilter}
         * @return {@code this}
         */
        public PrometheusReporter.Builder filter(MetricFilter filter) {
            this.filter = filter;
            return this;
        }

        /**
         * Builds a {@link PrometheusReporter} with the given properties, sending metrics using the
         * given {@link PushGatewayWrapper}.
         *
         * @param pushGatewayWrapper a {@link PushGatewayWrapper}
         * @return a {@link PrometheusReporter}
         */
        public PrometheusReporter build(PushGatewayWrapper pushGatewayWrapper) {
            return new PrometheusReporter(registry,
                    pushGatewayWrapper,
                    clock,
                    prefix,
                    rateUnit,
                    durationUnit,
                    filter);
        }
    }

}
