package com.wizenoze.prometheus;

import io.prometheus.client.CollectorRegistry;
import java.io.IOException;

public interface PushGatewayWrapper {

    void pushAdd(CollectorRegistry registry, String job) throws IOException;

}
