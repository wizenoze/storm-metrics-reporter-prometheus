package nl.wizenoze.prometheus;

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

    private static final String JOB_NAME = "storm";

    /**
     * Returns a new {@link PrometheusReporter.Builder} for {@link PrometheusReporter}.
     *
     * @param registry the registry to report
     * @return a {@link PrometheusReporter.Builder} instance for a {@link PrometheusReporter}
     */
    public static PrometheusReporter.Builder forRegistry(MetricRegistry registry) {
        return new PrometheusReporter.Builder(registry);
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

    private static final Logger LOGGER = LoggerFactory.getLogger(PrometheusReporter.class);

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

    @Override
    public void report(SortedMap<String, Gauge> gauges,
            SortedMap<String, Counter> counters,
            SortedMap<String, Histogram> histograms,
            SortedMap<String, Meter> meters,
            SortedMap<String, Timer> timers) {

        CollectorRegistry registry = new CollectorRegistry();

        for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
            registerGauge(registry, entry.getKey(), entry.getValue());
        }

        for (Map.Entry<String, Counter> entry : counters.entrySet()) {
            registerCounter(registry, entry.getKey(), entry.getValue());
        }

        for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
            registerHistogram(registry, entry.getKey(), entry.getValue());
        }

        for (Map.Entry<String, Meter> entry : meters.entrySet()) {
            registerMetered(registry, entry.getKey(), entry.getValue());
        }

        for (Map.Entry<String, Timer> entry : timers.entrySet()) {
            registerTimer(registry, entry.getKey(), entry.getValue());
        }

        try {
            pushGatewayWrapper.pushAdd(registry, JOB_NAME);
        } catch (IOException e) {
            LOGGER.warn("Unable to report to Prometheus", pushGatewayWrapper, e);
        }
    }

    private void registerTimer(CollectorRegistry registry, String name, Timer timer) {
        final Snapshot snapshot = timer.getSnapshot();

        addGauge(registry, prefix(name, "max"), convertDuration(snapshot.getMax()));
        addGauge(registry, prefix(name, "mean"), convertDuration(snapshot.getMean()));
        addGauge(registry, prefix(name, "min"), convertDuration(snapshot.getMin()));
        addGauge(registry, prefix(name, "stddev"), convertDuration(snapshot.getStdDev()));
        addGauge(registry, prefix(name, "p50"), convertDuration(snapshot.getMedian()));
        addGauge(registry, prefix(name, "p75"), convertDuration(snapshot.get75thPercentile()));
        addGauge(registry, prefix(name, "p95"), convertDuration(snapshot.get95thPercentile()));
        addGauge(registry, prefix(name, "p98"), convertDuration(snapshot.get98thPercentile()));
        addGauge(registry, prefix(name, "p99"), convertDuration(snapshot.get99thPercentile()));
        addGauge(registry, prefix(name, "p999"), convertDuration(snapshot.get999thPercentile()));

        registerMetered(registry, name, timer);
    }

    private void registerMetered(CollectorRegistry registry, String name, Metered meter) {
        addGauge(registry, prefix(name, "count"), meter.getCount());
        addGauge(registry, prefix(name, "m1_rate"), convertRate(meter.getOneMinuteRate()));
        addGauge(registry, prefix(name, "m5_rate"), convertRate(meter.getFiveMinuteRate()));
        addGauge(registry, prefix(name, "m15_rate"), convertRate(meter.getFifteenMinuteRate()));
        addGauge(registry, prefix(name, "mean_rate"), convertRate(meter.getMeanRate()));
    }

    private void registerHistogram(CollectorRegistry registry, String name, Histogram histogram) {
        final Snapshot snapshot = histogram.getSnapshot();

        addGauge(registry, prefix(name, "count"), histogram.getCount());
        addGauge(registry, prefix(name, "max"), snapshot.getMax());
        addGauge(registry, prefix(name, "mean"), snapshot.getMean());
        addGauge(registry, prefix(name, "min"), snapshot.getMin());
        addGauge(registry, prefix(name, "stddev"), snapshot.getStdDev());
        addGauge(registry, prefix(name, "p50"), snapshot.getMedian());
        addGauge(registry, prefix(name, "p75"), snapshot.get75thPercentile());
        addGauge(registry, prefix(name, "p95"), snapshot.get95thPercentile());
        addGauge(registry, prefix(name, "p98"), snapshot.get98thPercentile());
        addGauge(registry, prefix(name, "p99"), snapshot.get99thPercentile());
        addGauge(registry, prefix(name, "p999"), snapshot.get999thPercentile());
    }

    private void registerCounter(CollectorRegistry registry, String name, Counter counter) {
        addGauge(registry, prefix(name, "count"), counter.getCount());
    }

    private void registerGauge(CollectorRegistry registry, String name, Gauge gauge) {
        addGauge(registry, prefix(name), gauge.getValue());
    }

    private void addGauge(CollectorRegistry registry, String name, Number value) {
        assert (value != null);

        io.prometheus.client.Gauge gauge = io.prometheus.client.Gauge.build()
                .name(name).register(registry);

        gauge.set(value.doubleValue());
    }

    private void addGauge(CollectorRegistry registry, String name, Object value) {
        assert (value instanceof Number);
        addGauge(registry, name, (Number) value);
    }

    private String prefix(String... components) {
        return MetricRegistry.name(prefix, components);
    }

}
