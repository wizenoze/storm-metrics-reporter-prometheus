package com.wizenoze.prometheus;

import static java.lang.Character.isAlphabetic;
import static java.lang.Character.isDigit;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class MetricNameAndGroupingKey {

    private static final Logger LOGGER = LoggerFactory.getLogger(MetricNameAndGroupingKey.class);

    // storm.worker.(topologyId).(hostName).(componentId).(taskId).(workerPort)-(name)
    private static final Pattern STORM_WORKER_METRIC_NAME_PATTERN_SHORT =
            Pattern.compile("storm\\."
                    + "(?<type>worker)\\."
                    + "(?<topologyId>[\\p{Alnum}[-_]]+)\\."
                    + "(?<hostName>[\\p{Alnum}[-_]]+)\\."
                    + "(?<componentId>[\\p{Alnum}[-_]]+)\\."
                    + "(?<taskId>-?[\\d]+)\\."
                    + "(?<workerPort>[\\d]+)-"
                    + "(?<name>[\\p{Print}]+)");

    // storm.worker.(topologyId).(hostName).(componentId).(streamId).(taskId).(workerPort)-(name)
    private static final Pattern STORM_WORKER_METRIC_NAME_PATTERN_LONG =
            Pattern.compile("storm\\."
                    + "(?<type>worker)\\."
                    + "(?<topologyId>[\\p{Alnum}[-_]]+)\\."
                    + "(?<hostName>[\\p{Alnum}[-_]]+)\\."
                    + "(?<componentId>[\\p{Alnum}[-_]]+)\\."
                    + "(?<streamId>[\\p{Alnum}[-_]]+)\\."
                    + "(?<taskId>-?[\\d]+)\\."
                    + "(?<workerPort>[\\d]+)-"
                    + "(?<name>[\\p{Print}]+)");

    // storm.worker.(topologyId).(hostName).(componentId).(taskId).(workerPort)-(name)
    private static final Pattern STORM_TOPOLOGY_METRIC_NAME_PATTERN =
            Pattern.compile("storm\\."
                    + "(?<type>topology)\\."
                    + "(?<topologyId>[\\p{Alnum}[-_]]+)\\."
                    + "(?<hostName>[\\p{Alnum}[-_]]+)\\."
                    + "(?<componentId>[\\p{Alnum}[-_]]+)\\."
                    + "(?<taskId>-?[\\d]+)\\."
                    + "(?<workerPort>[\\d]+)-"
                    + "(?<name>[\\p{Print}]+)");

    private static final List<Pattern> PATTERNS;

    static {
        PATTERNS = Arrays.asList(
                STORM_TOPOLOGY_METRIC_NAME_PATTERN,
                STORM_WORKER_METRIC_NAME_PATTERN_SHORT,
                STORM_WORKER_METRIC_NAME_PATTERN_LONG
        );
    }

    private final String name;
    private final Map<String, String> groupingKey;

    private MetricNameAndGroupingKey(String name, Map<String, String> groupingKey) {
        this.name = name;
        this.groupingKey = groupingKey;
    }

    static MetricNameAndGroupingKey parseMetric(String originalName) {
        Matcher matcher = null;

        for (Pattern pattern : PATTERNS) {
            matcher = pattern.matcher(originalName);

            if (matcher.matches()) {
                break;
            } else {
                matcher = null;
            }
        }

        if (matcher == null) {
            throw new IllegalArgumentException(
                    originalName + " didn't match with the supported patterns.");
        }

        Map<String, String> groupingKey = new LinkedHashMap<>();

        addToGroupingKey("topologyId", matcher, groupingKey);
        addToGroupingKey("hostName", matcher, groupingKey);
        addToGroupingKey("componentId", matcher, groupingKey);
        addToGroupingKey("streamId", matcher, groupingKey);
        addToGroupingKey("taskId", matcher, groupingKey);
        addToGroupingKey("workerPort", matcher, groupingKey);

        String name = "storm_" + matcher.group("type") + "_" + escapeName(matcher.group("name"));

        return new MetricNameAndGroupingKey(name, groupingKey);
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

        try {
            String value = matcher.group(groupName);
            String name = groupName.replaceAll("(\\p{Upper})", "_$1").toLowerCase();
            groupingKey.put(name, value);
        } catch (IllegalArgumentException e) {
            LOGGER.debug(e.getMessage(), e);
        }
    }

    public String getName() {
        return name;
    }

    public Map<String, String> getGroupingKey() {
        return groupingKey;
    }

}
