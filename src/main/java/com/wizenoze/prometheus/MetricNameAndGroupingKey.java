package com.wizenoze.prometheus;

import static java.lang.Character.isAlphabetic;
import static java.lang.Character.isDigit;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class MetricNameAndGroupingKey {

    private static final Logger LOGGER = LoggerFactory.getLogger(MetricNameAndGroupingKey.class);

    // storm.worker.(topologyId).(hostName).(componentId).(streamId).(taskId).(workerPort)-(name)
    // storm.worker.(topologyId).(hostName).(componentId).(taskId).(workerPort)-(name)
    // storm.topology.(topologyId).(hostName).(componentId).(taskId).(workerPort)-(name)
    // storm.topology.production-topology-1-1636728165.ip-10-113-22-201_us-east-2_compute_internal.__system.-1.6700-CGroupCpu.sys-ms
    // storm.worker.production-topology-1-1636728165.ip-10-113-22-201_us-east-2_compute_internal.wordGenerator.15.6701-__skipped-backpressure-ms.m1_rate
    // storm.topology.production-topology-1-1636728165.ip-10-113-22-201_us-east-2_compute_internal.__system.-1.6701-memory.pools.CodeHeap-'profiled-nmethods'.usage
    // storm.worker.production-topology-1-1636728165.ip-10-113-22-201_us-east-2_compute_internal.intermediateRanker.default.8.6700-__ack-count-counter:default
    private static final Pattern STORM_WORKER_METRIC_NAME_PATTERN =
            Pattern.compile("storm\\."
                    + "(?<type>worker|topology)\\."
                    + "(?<topologyId>[\\p{Alnum}[-_]]+)\\."
                    + "(?<hostName>[\\p{Alnum}[-_]]+)\\."
                    + "(?<componentId>[\\p{Alnum}[-_]]+)\\."
                    + "(?:(?<streamId>[\\p{Alnum}[-_]]+)\\.)?"
                    + "(?<taskId>-?[\\d]+)\\."
                    + "(?<workerPort>[\\d]+)-"
                    + "(?<name>(disruptor-[\\p{Alnum}[-_]]+\\[(?<threadId>-?[\\d]+\\p{Space}-?[\\d]+)\\]-[\\p{Alnum}[-_]]+)|.+)");

    private final String name;
    private final Map<String, String> groupingKey;

    private MetricNameAndGroupingKey(String name, Map<String, String> groupingKey) {
        this.name = name;
        this.groupingKey = groupingKey;
    }

    static MetricNameAndGroupingKey parseMetric(String originalName) {
        Matcher matcher = STORM_WORKER_METRIC_NAME_PATTERN.matcher(originalName);
        if (!matcher.matches()) {
            throw new UnsupportedMetricName(
                    originalName + " didn't match with the supported patterns.");
        }

        Map<String, String> groupingKey = new LinkedHashMap<>();

        addToGroupingKey("topologyId", matcher, groupingKey);
        addToGroupingKey("hostName", matcher, groupingKey);
        addToGroupingKey("componentId", matcher, groupingKey);
        addToGroupingKey("streamId", matcher, groupingKey);
        addToGroupingKey("taskId", matcher, groupingKey);
        addToGroupingKey("workerPort", matcher, groupingKey);

        String threadId = matcher.group("threadId");
        String name = matcher.group("name");

        // This is a dirty hack to make disruptor metrics usable
        if (threadId != null) {
            int tidStartPos = name.indexOf('[');
            int tidEndPost = name.lastIndexOf(']');
            if (tidStartPos > 0 && tidEndPost > 0) {
                name = name.substring(0, tidStartPos) + name.substring(tidEndPost + 1);
            }

            addToGroupingKey("threadId", matcher, groupingKey);
        }

        String metricName = "storm_" + matcher.group("type") + "_" + escapeName(name);

        return new MetricNameAndGroupingKey(metricName, groupingKey);
    }

    private static String escapeName(String name) {
        char[] charArray = name.toCharArray();
        for (int index = 0; index < charArray.length; index++) {
            char c = charArray[index];
            if (!isAlphabetic(c) && !isDigit(c) && c != '_') {
                charArray[index] = '_';
            }
        }

        return String.valueOf(charArray);
    }

    private static void addToGroupingKey(
            String groupName, Matcher matcher, Map<String, String> groupingKey) {

        String value = null;
        try {
            value = matcher.group(groupName);
        } catch (IllegalArgumentException e) {
            LOGGER.debug(e.getMessage(), e);
        }

        if (value == null || value.length() == 0) {
            return;
        }

        String name = groupName.replaceAll("(\\p{Upper})", "_$1").toLowerCase();
        groupingKey.put(name, value);
    }

    String getName() {
        return name;
    }

    Map<String, String> getGroupingKey() {
        return groupingKey;
    }

}
