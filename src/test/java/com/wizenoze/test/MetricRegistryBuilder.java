package com.wizenoze.test;

import static java.util.concurrent.TimeUnit.SECONDS;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import java.util.ArrayList;
import java.util.List;
import org.apache.storm.metrics2.SimpleGauge;

public class MetricRegistryBuilder {

    public static final String HISTOGRAM_NAME = "histogram";
    public static final String COUNTER_NAME = "counter";
    public static final String METER_NAME = "meter";
    public static final String TIMER_NAME = "timer";
    public static final String GAUGE_NAME = "gauge";

    private final List<Integer> histogramUpdates;
    private final List<Long> meterMarks;
    private final List<Long> timerUpdates;

    private int counterIncrements;
    private int gaugeValue;

    public MetricRegistryBuilder() {
        histogramUpdates = new ArrayList<>();
        meterMarks = new ArrayList<>();
        timerUpdates = new ArrayList<>();
    }

    public MetricRegistryBuilder updateHistogram(int value) {
        histogramUpdates.add(value);
        return this;
    }

    public MetricRegistryBuilder incrementCount() {
        counterIncrements++;
        return this;
    }

    public MetricRegistryBuilder markMeter() {
        return markMeter(1);
    }

    public MetricRegistryBuilder markMeter(long events) {
        meterMarks.add(events);
        return this;
    }

    public MetricRegistryBuilder updateTimer(long duration) {
        timerUpdates.add(duration);
        return this;
    }

    public MetricRegistryBuilder setGaugeValue(int value) {
        gaugeValue = value;
        return this;
    }

    public MetricRegistry build() {
        MetricRegistry metricRegistry = new MetricRegistry();

        Histogram histogram = metricRegistry.histogram(HISTOGRAM_NAME);
        for (Integer value : histogramUpdates) {
            histogram.update(value);
        }

        Counter counter = metricRegistry.counter(COUNTER_NAME);
        for (int index = 0; index < counterIncrements; index++) {
            counter.inc();
        }

        Meter meter = new Meter(new FixedClock());
        metricRegistry.register(METER_NAME, meter);
        for (Long events : meterMarks) {
            meter.mark(events);
        }

        Timer timer = metricRegistry.timer(TIMER_NAME);
        for (Long duration : timerUpdates) {
            timer.update(duration, SECONDS);
        }

        Gauge<Integer> gauge = new SimpleGauge<>(gaugeValue);
        metricRegistry.register(GAUGE_NAME, gauge);

        return metricRegistry;
    }

    private static class FixedClock extends Clock {

        @Override
        public long getTick() {
            return 1;
        }

    }

}
