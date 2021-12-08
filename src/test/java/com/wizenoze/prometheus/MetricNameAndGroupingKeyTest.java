package com.wizenoze.prometheus;

import static com.wizenoze.prometheus.MetricNameAndGroupingKey.parseMetric;
import static org.apache.storm.metrics2.StormMetricRegistry.metricName;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class MetricNameAndGroupingKeyTest {

    // storm.worker.siteTestCrawlIndexDelete-9-1544624008.499a88998a53.documentExtractor.status.19.6701-emitted
    private static final String COMPONENT_METRIC_NAME =
            metricName("emitted", "siteTestCrawlIndexDelete-9-1544624008", "documentExtractor",
                    "status", 19, 6701);

    // storm.worker.siteTestCrawlIndexDelete-9-1544624008.499a88998a53.documentExtractor.28.6701-disruptor-executor[28 28]-send-queue-capacity
    private static final String DISRUPTOR_EXECUTOR_SEND_QUEUE_METRIC_NAME =
            metricName("disruptor-executor[28 28]-send-queue-capacity",
                    "siteTestCrawlIndexDelete-9-1544624008", "documentExtractor", 28, 6701);

    // storm.worker.siteTestCrawlIndexDelete-9-1544624008.499a88998a53.__system.-1.6700-disruptor-receive-queue[74 74]-population
    private static final String DISRUPTOR_RECEIVE_QUEUE_METRIC_NAME =
            metricName("disruptor-receive-queue[74 74]-population",
                    "siteTestCrawlIndexDelete-9-1544624008", "__system", -1, 6700);

    // storm.worker.siteTestCrawlIndexDelete-9-1544624008.499a88998a53.__system.-1.6700-disruptor-worker-transfer-queue-write-position
    private static final String DISRUPTOR_TRANSFER_QUEUE_METRIC_NAME =
            metricName("disruptor-worker-transfer-queue-write-position",
                    "siteTestCrawlIndexDelete-9-1544624008", "__system", -1, 6700);

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
                        hasEntry("worker_port", "6701"),
                        not(hasKey("thread_id"))
                )
        );
    }

    @Test
    void givenDisruptorESQ_MetricName_whenParseMetric_thenNameEscaped() {
        MetricNameAndGroupingKey metricNameAndGroupingKey = parseMetric(
                DISRUPTOR_EXECUTOR_SEND_QUEUE_METRIC_NAME);

        assertEquals(
                "storm_worker_disruptor_executor_send_queue_capacity",
                metricNameAndGroupingKey.getName()
        );

        assertThat(
                metricNameAndGroupingKey.getGroupingKey(),
                allOf(
                        hasEntry("topology_id", "siteTestCrawlIndexDelete-9-1544624008"),
                        hasEntry("host_name", "null"),
                        hasEntry("component_id", "documentExtractor"),
                        not(hasKey("stream_id")),
                        hasEntry("task_id", "28"),
                        hasEntry("worker_port", "6701"),
                        hasEntry("thread_id", "28 28")
                )
        );
    }

    @Test
    void givenDisruptorRQ_MetricNameWithNegativeTaskId_whenParseMetric_thenNameEscaped() {
        MetricNameAndGroupingKey metricNameAndGroupingKey =
                parseMetric(DISRUPTOR_RECEIVE_QUEUE_METRIC_NAME);

        assertEquals(
                "storm_worker_disruptor_receive_queue_population",
                metricNameAndGroupingKey.getName()
        );

        assertThat(
                metricNameAndGroupingKey.getGroupingKey(),
                allOf(
                        hasEntry("topology_id", "siteTestCrawlIndexDelete-9-1544624008"),
                        hasEntry("host_name", "null"),
                        hasEntry("component_id", "__system"),
                        not(hasKey("stream_id")),
                        hasEntry("task_id", "-1"),
                        hasEntry("worker_port", "6700"),
                        hasEntry("thread_id", "74 74")
                )
        );
    }

    @Test
    void givenDisruptorTQ_MetricNameWithNegativeTaskId_whenParseMetric_thenNameEscaped() {
        MetricNameAndGroupingKey metricNameAndGroupingKey =
                parseMetric(DISRUPTOR_TRANSFER_QUEUE_METRIC_NAME);

        assertEquals(
                "storm_worker_disruptor_worker_transfer_queue_write_position",
                metricNameAndGroupingKey.getName()
        );

        assertThat(
                metricNameAndGroupingKey.getGroupingKey(),
                allOf(
                        hasEntry("topology_id", "siteTestCrawlIndexDelete-9-1544624008"),
                        hasEntry("host_name", "null"),
                        hasEntry("component_id", "__system"),
                        not(hasKey("stream_id")),
                        hasEntry("task_id", "-1"),
                        hasEntry("worker_port", "6700"),
                        not(hasKey("thread_id"))
                )
        );
    }

}
