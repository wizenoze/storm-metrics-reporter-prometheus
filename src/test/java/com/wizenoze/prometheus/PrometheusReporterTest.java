package com.wizenoze.prometheus;

import static com.wizenoze.test.MetricRegistryBuilder.COUNTER_NAME;
import static com.wizenoze.test.MetricRegistryBuilder.GAUGE_NAME;
import static com.wizenoze.test.MetricRegistryBuilder.HISTOGRAM_NAME;
import static com.wizenoze.test.MetricRegistryBuilder.METER_NAME;
import static com.wizenoze.test.MetricRegistryBuilder.TIMER_NAME;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.wizenoze.test.MetricRegistryBuilder;
import io.prometheus.client.CollectorRegistry;
import java.io.IOException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class PrometheusReporterTest {

    private Histogram histogram;
    private Counter counter;
    private Meter meter;
    private Timer timer;
    private Gauge<Integer> gauge;

    @Mock
    private PushGatewayWrapper pushGatewayWrapper;

    private PrometheusReporter prometheusReporter;

    @BeforeEach
    void setUp() {
        MetricRegistry metricRegistry = new MetricRegistryBuilder()
                .updateHistogram(1)
                .incrementCount()
                .markMeter()
                .updateTimer(1)
                .setGaugeValue(1)
                .build();

        histogram = metricRegistry.histogram(HISTOGRAM_NAME);
        counter = metricRegistry.counter(COUNTER_NAME);
        meter = metricRegistry.meter(METER_NAME);
        timer = metricRegistry.timer(TIMER_NAME);
        gauge = (Gauge<Integer>) metricRegistry.getMetrics().get(GAUGE_NAME);

        prometheusReporter = PrometheusReporter.forRegistry(metricRegistry)
                .prefixedWith("test").convertDurationsTo(NANOSECONDS).build(pushGatewayWrapper);
    }

    @Test
    void testReport() throws IOException {
        prometheusReporter.report();

        ArgumentCaptor<CollectorRegistry> collectorRegistryCaptor =
                ArgumentCaptor.forClass(CollectorRegistry.class);

        ArgumentCaptor<String> jobNameCaptor =
                ArgumentCaptor.forClass(String.class);

        verify(pushGatewayWrapper)
                .pushAdd(collectorRegistryCaptor.capture(), jobNameCaptor.capture());

        CollectorRegistry collectorRegistry = collectorRegistryCaptor.getValue();

        assertHistogram(collectorRegistry);
        assertCounter(collectorRegistry);
        assertMeter(collectorRegistry);
        assertTimer(collectorRegistry);
        assertGauge(collectorRegistry);
    }

    private void assertHistogram(CollectorRegistry collectorRegistry) {
        final Snapshot snapshot = histogram.getSnapshot();

        assertEquals(histogram.getCount(),
                collectorRegistry.getSampleValue("test_histogram_count").longValue(),
                "test_histogram_count");
        assertEquals(snapshot.getMax(),
                collectorRegistry.getSampleValue("test_histogram_max").longValue(),
                "test_histogram_max");
        assertEquals(snapshot.getMean(),
                collectorRegistry.getSampleValue("test_histogram_mean").doubleValue(),
                "test_histogram_mean");
        assertEquals(snapshot.getMin(),
                collectorRegistry.getSampleValue("test_histogram_min").longValue(),
                "test_histogram_min");
        assertEquals(snapshot.getStdDev(),
                collectorRegistry.getSampleValue("test_histogram_stddev").doubleValue(),
                "test_histogram_stddev");
        assertEquals(snapshot.getMedian(),
                collectorRegistry.getSampleValue("test_histogram_p50").doubleValue(),
                "test_histogram_p50");
        assertEquals(snapshot.get75thPercentile(),
                collectorRegistry.getSampleValue("test_histogram_p75").doubleValue(),
                "test_histogram_p75");
        assertEquals(snapshot.get95thPercentile(),
                collectorRegistry.getSampleValue("test_histogram_p95").doubleValue(),
                "test_histogram_p95");
        assertEquals(snapshot.get98thPercentile(),
                collectorRegistry.getSampleValue("test_histogram_p98").doubleValue(),
                "test_histogram_p98");
        assertEquals(snapshot.get99thPercentile(),
                collectorRegistry.getSampleValue("test_histogram_p99").doubleValue(),
                "test_histogram_p99");
        assertEquals(snapshot.get999thPercentile(),
                collectorRegistry.getSampleValue("test_histogram_p999").doubleValue(),
                "test_histogram_p999");
    }

    private void assertCounter(CollectorRegistry collectorRegistry) {
        assertEquals(counter.getCount(),
                collectorRegistry.getSampleValue("test_counter_count").longValue(),
                "test_counter_count");
    }

    private void assertMeter(CollectorRegistry collectorRegistry) {
        assertEquals(meter.getCount(),
                collectorRegistry.getSampleValue("test_meter_count").longValue(),
                "test_meter_count");
        assertEquals(meter.getOneMinuteRate(),
                collectorRegistry.getSampleValue("test_meter_m1_rate").doubleValue(),
                "test_meter_m1_rate");
        assertEquals(meter.getFiveMinuteRate(),
                collectorRegistry.getSampleValue("test_meter_m5_rate").doubleValue(),
                "test_meter_m5_rate");
        assertEquals(meter.getFifteenMinuteRate(),
                collectorRegistry.getSampleValue("test_meter_m15_rate").doubleValue(),
                "test_meter_m15_rate");
        assertEquals(meter.getMeanRate(),
                collectorRegistry.getSampleValue("test_meter_mean_rate").doubleValue(),
                "test_meter_mean_rate");
    }

    private void assertTimer(CollectorRegistry collectorRegistry) {
        final Snapshot snapshot = timer.getSnapshot();

        assertEquals(snapshot.getMax(),
                collectorRegistry.getSampleValue("test_timer_max").longValue(), "test_timer_max");
        assertEquals(snapshot.getMean(),
                collectorRegistry.getSampleValue("test_timer_mean").doubleValue(),
                "test_timer_mean");
        assertEquals(snapshot.getMin(),
                collectorRegistry.getSampleValue("test_timer_min").longValue(), "test_timer_min");
        assertEquals(snapshot.getStdDev(),
                collectorRegistry.getSampleValue("test_timer_stddev").doubleValue(),
                "test_timer_stddev");
        assertEquals(snapshot.getMedian(),
                collectorRegistry.getSampleValue("test_timer_p50").doubleValue(), "test_timer_p50");
        assertEquals(snapshot.get75thPercentile(),
                collectorRegistry.getSampleValue("test_timer_p75").doubleValue(), "test_timer_p75");
        assertEquals(snapshot.get95thPercentile(),
                collectorRegistry.getSampleValue("test_timer_p95").doubleValue(), "test_timer_p95");
        assertEquals(snapshot.get98thPercentile(),
                collectorRegistry.getSampleValue("test_timer_p98").doubleValue(), "test_timer_p98");
        assertEquals(snapshot.get99thPercentile(),
                collectorRegistry.getSampleValue("test_timer_p99").doubleValue(), "test_timer_p99");
        assertEquals(snapshot.get999thPercentile(),
                collectorRegistry.getSampleValue("test_timer_p999").doubleValue(),
                "test_timer_p999");
    }

    private void assertGauge(CollectorRegistry collectorRegistry) {
        assertEquals(gauge.getValue().intValue(),
                collectorRegistry.getSampleValue("test_gauge").intValue(), "test_gauge");
    }


}
