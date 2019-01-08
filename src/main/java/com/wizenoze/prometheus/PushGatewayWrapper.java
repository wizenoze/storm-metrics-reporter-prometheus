package com.wizenoze.prometheus;

import io.prometheus.client.CollectorRegistry;
import java.io.IOException;
import java.util.Map;

public interface PushGatewayWrapper {

    void pushAdd(CollectorRegistry registry, String job) throws IOException;

    void pushAdd(CollectorRegistry registry, String job, Map<String, String> groupingKey)
            throws IOException;

}
