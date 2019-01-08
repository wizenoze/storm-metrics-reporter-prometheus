package com.wizenoze.prometheus;

import static java.util.Collections.singletonMap;
import static org.apache.storm.utils.Utils.hostname;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.PushGateway;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.LinkedHashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PushGatewayWrapperImpl implements PushGatewayWrapper {

    private static final Logger LOG = LoggerFactory.getLogger(PushGatewayWrapperImpl.class);

    private final PushGateway pushGateway;
    private final String hostname;

    public PushGatewayWrapperImpl(String httpAddress) {
        pushGateway = createPushGateway(httpAddress);
        hostname = getHostName();
    }

    @Override
    public void pushAdd(CollectorRegistry registry, String job) throws IOException {
        pushGateway.pushAdd(registry, job, singletonMap("instance", hostname));
    }

    @Override
    public void pushAdd(CollectorRegistry registry, String job, Map<String, String> groupingKey)
            throws IOException {

        Map<String, String> newGroupingKey = new LinkedHashMap<>(groupingKey);
        newGroupingKey.put("instance", hostname);

        pushGateway.pushAdd(registry, job, newGroupingKey);
    }

    private PushGateway createPushGateway(String httpAddress) {
        try {
            return new PushGateway(new URL(httpAddress));
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    private String getHostName() {
        String hostname = "";
        try {
            hostname = hostname();
        } catch (UnknownHostException e) {
            LOG.warn("Couldn't get hostname.", e);
        }

        return hostname;
    }

}