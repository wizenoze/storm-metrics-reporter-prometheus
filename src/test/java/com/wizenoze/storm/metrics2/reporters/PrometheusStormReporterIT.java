package com.wizenoze.storm.metrics2.reporters;

import static org.apache.storm.Config.STORM_METRICS_REPORTERS;
import static org.apache.storm.cluster.DaemonType.WORKER;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

import com.codahale.metrics.Counter;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import org.apache.storm.metrics2.StormMetricRegistry;
import org.apache.storm.utils.Utils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(PER_CLASS)
class PrometheusStormReporterIT {

    private final Map<String, Object> stormConfig;
    private final URL prometheusUrl;

    public PrometheusStormReporterIT() throws MalformedURLException {
        stormConfig = Utils.findAndReadConfigFile("test-storm.yaml");

        List<Map<String, Object>> reporterList =
                (List<Map<String, Object>>) stormConfig.get(STORM_METRICS_REPORTERS);

        Map<String, Object> reporterConfig = reporterList.get(0);

        String scheme = (String) reporterConfig.get("prometheus.scheme");
        String host = (String) reporterConfig.get("prometheus.host");
        Integer port = (Integer) reporterConfig.get("prometheus.port");

        prometheusUrl = new URL(scheme, host, port, "/metrics");
    }

    @BeforeAll
    void start() {
        StormMetricRegistry.start(stormConfig, WORKER);
    }

    @AfterAll
    void stop() {
        StormMetricRegistry.stop();
    }

    @Test
    void reports() throws IOException {
        Counter counter = StormMetricRegistry.counter(
                "test-counter",
                "test-topology",
                "test-component",
                1,
                6700,
                "default"
        );

        for (int index = 0; index < 10; index++) {
            counter.inc();
            Utils.sleep(500);
        }

        // Sanity check
        Assertions.assertEquals(10, counter.getCount());

        HttpURLConnection connection = (HttpURLConnection) prometheusUrl.openConnection();
        try {
            connection.connect();
            Scanner scanner = new Scanner(connection.getInputStream()).useDelimiter("\\A");
            String response = scanner.next();
            assertResponse(response);
        } finally {
            connection.disconnect();
        }
    }

    private void assertResponse(String response) {
        // TODO
    }

}
