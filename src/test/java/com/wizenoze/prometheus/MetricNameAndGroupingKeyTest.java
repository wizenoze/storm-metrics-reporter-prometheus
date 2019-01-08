package com.wizenoze.prometheus;

import static com.wizenoze.prometheus.MetricNameAndGroupingKey.parseMetric;
import static org.apache.storm.metrics2.StormMetricRegistry.metricName;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.hasEntry;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class MetricNameAndGroupingKeyTest {

    // storm.worker.siteTestCrawlIndexDelete-9-1544624008.499a88998a53.documentExtractor.status.19.6701-emitted
    private static final String COMPONENT_METRIC_NAME =
            metricName("emitted", "siteTestCrawlIndexDelete-9-1544624008", "documentExtractor",
                    "status", 19, 6701);

    // storm.worker.siteTestCrawlIndexDelete-9-1544624008.499a88998a53.documentExtractor.28.6701-disruptor-executor[28 28]-send-queue-capacity
    private static final String DISRUPTOR_METRIC_NAME =
            metricName("disruptor-executor[28 28]-send-queue-capacity",
                    "siteTestCrawlIndexDelete-9-1544624008", "documentExtractor", 28, 6701);

    @Test
    void givenComponentMetricName_whenParseMetric_thenNameEscaped() {
        MetricNameAndGroupingKey metricNameAndGroupingKey = parseMetric(COMPONENT_METRIC_NAME);

        assertEquals("storm_worker_emitted", metricNameAndGroupingKey.getName());

        assertThat(
                metricNameAndGroupingKey.getGroupingKey(),
                allOf(
                        hasEntry("topology_id", "siteTestCrawlIndexDelete-9-1544624008"),
                        hasEntry("host_name", "null"),
                        hasEntry("component_id", "documentExtractor"),
                        hasEntry("stream_id", "status"),
                        hasEntry("task_id", "19"),
                        hasEntry("worker_port", "6701")
                )
        );
    }

    @Test
    void givenDisruptorMetricName_whenParseMetric_thenNameEscaped() {
        MetricNameAndGroupingKey metricNameAndGroupingKey = parseMetric(DISRUPTOR_METRIC_NAME);

        assertEquals(
                "storm_worker_disruptor_executor_28_28__send_queue_capacity",
                metricNameAndGroupingKey.getName()
        );

        assertThat(
                metricNameAndGroupingKey.getGroupingKey(),
                allOf(
                        hasEntry("topology_id", "siteTestCrawlIndexDelete-9-1544624008"),
                        hasEntry("host_name", "null"),
                        hasEntry("component_id", "documentExtractor"),
                        hasEntry("task_id", "28"),
                        hasEntry("worker_port", "6701")
                )
        );
    }

}
