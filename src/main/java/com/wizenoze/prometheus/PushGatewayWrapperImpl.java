package com.wizenoze.prometheus;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.PushGateway;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PushGatewayWrapperImpl implements PushGatewayWrapper {

    private static final Logger LOG = LoggerFactory.getLogger(PushGatewayWrapperImpl.class);

    private final PushGateway pushGateway;
    private final Map<String, String> instanceGroupingKey;

    public PushGatewayWrapperImpl(String httpAddress) {
        try {
            pushGateway = new PushGateway(new URL(httpAddress));
            instanceGroupingKey = Collections.unmodifiableMap(instanceGroupingKey());
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void pushAdd(CollectorRegistry registry, String job) throws IOException {
        pushGateway.pushAdd(registry, job, instanceGroupingKey);
    }

    private Map<String, String> instanceGroupingKey() {
        Map<String, String> groupingKey = new HashMap<>();

        String hostname = "";
        try {
            hostname = Utils.hostname();
        } catch (UnknownHostException e) {
            LOG.warn("Couldn't get hostname.", e);
        }

        groupingKey.put("instance", hostname);
        return groupingKey;
    }

}
