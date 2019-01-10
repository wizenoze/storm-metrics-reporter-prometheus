package com.wizenoze.storm.metrics2.reporters;

import static java.util.Collections.unmodifiableMap;
import static org.apache.storm.Config.STORM_METRICS_REPORTERS;
import static org.apache.storm.cluster.DaemonType.WORKER;
import static org.apache.storm.utils.Utils.hostname;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

import com.codahale.metrics.Counter;
import io.prometheus.client.exporter.PushGateway;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.Callable;
import java.util.concurrent.ForkJoinPool;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.storm.metrics2.DisruptorMetrics;
import org.apache.storm.metrics2.StormMetricRegistry;
import org.apache.storm.utils.Utils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@TestInstance(PER_CLASS)
class PrometheusStormReporterIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(PrometheusStormReporterIT.class);

    private static final Pattern STORM_METRIC_LINE_PATTERN =
            Pattern.compile("storm_worker_([\\p{Alnum}[_]]+)\\{\\p{Graph}+\\}\\p{Space}(\\d+)$");

    private static final String[] INCLUDED_METRIC_NAMES = {
            "emitted", "acked",
            "disruptor-executor[1 1]-send-queue-percent-full",
            "disruptor-executor[1 1]-send-queue-overflow"
    };

    private static final String[] EXCLUDED_METRIC_NAMES = {
            "transferred",
            "disruptor-executor[1 1]-send-queue-arrival-rate",
            "disruptor-executor[1 1]-send-queue-capacity"
    };

    private static final Integer EXPECTED_VALUE = 10;

    private final Map<String, Object> stormConfig;
    private final Map<String, String> groupingKey;
    private final URL prometheusUrl;
    private final URL metricsUrl;

    public PrometheusStormReporterIT() throws Exception {
        stormConfig = Utils.findAndReadConfigFile("test-storm.yaml");

        List<Map<String, Object>> reporterList =
                (List<Map<String, Object>>) stormConfig.get(STORM_METRICS_REPORTERS);

        Map<String, Object> reporterConfig = reporterList.get(0);

        Map<String, String> groupingKey = new LinkedHashMap<>();

        String hostName = hostname();
        groupingKey.put("instance", hostName);
        groupingKey.put("host_name", hostName.replaceAll("\\.", "_"));

        groupingKey.put("topology_id", "test-topology");
        groupingKey.put("component_id", "test-component");
        groupingKey.put("stream_id", "test-stream");
        groupingKey.put("task_id", "1");
        groupingKey.put("worker_port", "6700");

        this.groupingKey = unmodifiableMap(groupingKey);

        String scheme = (String) reporterConfig.get("prometheus.scheme");
        String host = (String) reporterConfig.get("prometheus.host");
        Integer port = (Integer) reporterConfig.get("prometheus.port");

        prometheusUrl = new URL(scheme, host, port, "");
        metricsUrl = new URL(scheme, host, port, "/metrics");

    }

    @BeforeAll
    void start() {
        StormMetricRegistry.start(stormConfig, WORKER);

        startCounters();
    }

    @AfterAll
    void stop() throws IOException {
        StormMetricRegistry.stop();

        new PushGateway(prometheusUrl).delete("storm", groupingKey);
    }

    @Test
    void givenIncludedMetricNames_whenReported_thenValuePushed() throws IOException {
        String response = getResponse();
        LOGGER.info(response);

        assertMetricValue(EXPECTED_VALUE, "emitted_count", response);
        assertMetricValue(EXPECTED_VALUE, "acked_count", response);

        assertMetricValue(
                EXPECTED_VALUE,
                "disruptor_executor_1_1__send_queue_percent_full",
                response
        );

        assertMetricValue(EXPECTED_VALUE, "disruptor_executor_1_1__send_queue_overflow", response);
    }

    @Test
    void givenExcludedMetricNames_whenReported_thenValuePushed() throws IOException {
        String response = getResponse();
        LOGGER.info(response);

        assertMetricValue(null, "transferred_count", response);
        assertMetricValue(null, "disruptor_executor_1_1__send_queue_arrival_rate", response);
        assertMetricValue(null, "disruptor_executor_1_1__send_queue_capacity", response);
    }

    private void assertMetricValue(Integer expectedValue, String name, String response) {
        Integer actualValue = getMetricValue(response, name);

        if (expectedValue == null) {
            assertNull(actualValue, name);
        } else {
            assertEquals(expectedValue, actualValue, name);
        }
    }

    private String getResponse() throws IOException {
        HttpURLConnection connection = (HttpURLConnection) metricsUrl.openConnection();
        try {
            connection.connect();
            Scanner scanner = new Scanner(connection.getInputStream()).useDelimiter("\\A");
            return scanner.next();
        } finally {
            connection.disconnect();
        }
    }

    private Integer getMetricValue(String response, String name) {
        Scanner scanner = new Scanner(response);
        Integer actualValue = null;

        while (scanner.hasNextLine()) {
            String line = scanner.nextLine();
            Matcher lineMatcher = STORM_METRIC_LINE_PATTERN.matcher(line);

            if (!lineMatcher.matches()) {
                continue;
            }

            if (name.equals(lineMatcher.group(1))) {
                actualValue = Integer.valueOf(lineMatcher.group(2));
                break;
            }
        }

        scanner.close();

        return actualValue;
    }

    private void startCounters() {
        Collection<MetricChanger> metricChangers =
                new ArrayList<>(INCLUDED_METRIC_NAMES.length + EXCLUDED_METRIC_NAMES.length);

        for (String name : INCLUDED_METRIC_NAMES) {
            if (name.startsWith("disruptor")) {
                metricChangers.add(new DisruptorMetricsChanger(name));
            } else {
                metricChangers.add(new CounterIncrementer(name));
            }
        }

        for (String name : EXCLUDED_METRIC_NAMES) {
            if (name.startsWith("disruptor")) {
                metricChangers.add(new DisruptorMetricsChanger(name));
            } else {
                metricChangers.add(new CounterIncrementer(name));
            }
        }

        ForkJoinPool forkJoinPool = new ForkJoinPool(metricChangers.size());

        forkJoinPool.invokeAll(metricChangers);
    }

    private abstract class MetricChanger implements Callable<Void> {

        final String name;

        MetricChanger(String name) {
            this.name = name;
        }

        abstract void changeMetric();

        @Override
        public Void call() {
            changeMetric();
            return null;
        }

    }

    private class CounterIncrementer extends MetricChanger {

        final Counter counter;

        CounterIncrementer(String name) {
            super(name);

            counter = StormMetricRegistry.counter(
                    name,
                    groupingKey.get("topology_id"),
                    groupingKey.get("component_id"),
                    Integer.valueOf(groupingKey.get("task_id")),
                    Integer.valueOf(groupingKey.get("worker_port")),
                    groupingKey.get("stream_id")
            );
        }

        @Override
        void changeMetric() {
            for (int index = 0; index < EXPECTED_VALUE; index++) {
                counter.inc();
                LOGGER.info("Incremented {}, current value: {}.", name, counter.getCount());
                Utils.sleep(500);
            }
        }

    }

    private class DisruptorMetricsChanger extends MetricChanger {

        final DisruptorMetrics disruptorMetrics;

        DisruptorMetricsChanger(String name) {
            super(name);

            disruptorMetrics = StormMetricRegistry.disruptorMetrics(
                    name,
                    groupingKey.get("topology_id"),
                    groupingKey.get("component_id"),
                    Integer.valueOf(groupingKey.get("task_id")),
                    Integer.valueOf(groupingKey.get("worker_port"))
            );
        }

        @Override
        void changeMetric() {
            disruptorMetrics.setArrivalRate(Double.valueOf(EXPECTED_VALUE));
            disruptorMetrics.setCapacity(Long.valueOf(EXPECTED_VALUE));
            disruptorMetrics.setOverflow(Long.valueOf(EXPECTED_VALUE));
            disruptorMetrics.setPercentFull(Float.valueOf(EXPECTED_VALUE));
        }

    }

}
