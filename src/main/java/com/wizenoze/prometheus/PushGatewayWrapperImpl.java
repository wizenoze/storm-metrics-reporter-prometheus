package com.wizenoze.prometheus;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.PushGateway;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

public class PushGatewayWrapperImpl implements PushGatewayWrapper {

    private final PushGateway pushGateway;

    public PushGatewayWrapperImpl(String httpAddress) {
        try {
            pushGateway = new PushGateway(new URL(httpAddress));
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void pushAdd(CollectorRegistry registry, String job) throws IOException {
        pushGateway.pushAdd(registry, job);
    }

}
