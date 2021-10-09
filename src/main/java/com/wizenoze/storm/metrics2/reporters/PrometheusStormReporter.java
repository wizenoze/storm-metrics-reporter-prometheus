package com.wizenoze.storm.metrics2.reporters;

import com.codahale.metrics.MetricRegistry;
import com.wizenoze.prometheus.PrometheusReporter;
import com.wizenoze.prometheus.PushGatewayWrapper;
import com.wizenoze.prometheus.PushGatewayWrapperImpl;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.storm.daemon.metrics.ClientMetricsUtils;
import org.apache.storm.metrics2.filters.StormMetricsFilter;
import org.apache.storm.metrics2.reporters.ScheduledStormReporter;
import org.apache.storm.utils.ObjectReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PrometheusStormReporter extends ScheduledStormReporter {

    private final static Logger LOGGER = LoggerFactory.getLogger(PrometheusStormReporter.class);

    private static final String PROMETHEUS_PREFIXED_WITH = "prometheus.prefixed.with";
    private static final String PROMETHEUS_HOST = "prometheus.host";
    private static final String PROMETHEUS_PORT = "prometheus.port";
    private static final String PROMETHEUS_SCHEME = "prometheus.scheme";

    private static String getMetricsPrefixedWith(Map reporterConf) {
        return ObjectReader.getString(reporterConf.get(PROMETHEUS_PREFIXED_WITH), null);
    }

    private static String getMetricsTargetHost(Map reporterConf) {
        return ObjectReader.getString(reporterConf.get(PROMETHEUS_HOST), "localhost");
    }

    private static Integer getMetricsTargetPort(Map reporterConf) {
        return ObjectReader.getInt(reporterConf.get(PROMETHEUS_PORT), 9091);
    }

    private static String getMetricsTargetScheme(Map reporterConf) {
        return ObjectReader.getString(reporterConf.get(PROMETHEUS_SCHEME), "http");
    }

    @Override
    public void prepare(MetricRegistry metricsRegistry, Map stormConf, Map reporterConf) {
        LOGGER.info("Preparing...");
        PrometheusReporter.Builder builder = PrometheusReporter.forRegistry(metricsRegistry);

        TimeUnit durationUnit = ClientMetricsUtils.getMetricsDurationUnit(reporterConf);
        if (durationUnit != null) {
            builder.convertDurationsTo(durationUnit);
        }
        TimeUnit rateUnit = ClientMetricsUtils.getMetricsRateUnit(reporterConf);
        if (rateUnit != null) {
            builder.convertRatesTo(rateUnit);
        }

        StormMetricsFilter filter = getMetricsFilter(reporterConf);
        if (filter != null) {
            builder.filter(filter);
        }
        String prefix = getMetricsPrefixedWith(reporterConf);
        if (prefix != null) {
            builder.prefixedWith(prefix);
        }

        //defaults to 10
        reportingPeriod = getReportPeriod(reporterConf);

        //defaults to seconds
        reportingPeriodUnit = getReportPeriodUnit(reporterConf);

        // Not exposed:
        // * withClock(Clock)

        String host = getMetricsTargetHost(reporterConf);
        Integer port = getMetricsTargetPort(reporterConf);
        String scheme = getMetricsTargetScheme(reporterConf);

        String httpAddress = scheme + "://" + host + ":" + port;

        PushGatewayWrapper pushGatewayWrapper = new PushGatewayWrapperImpl(httpAddress);
        reporter = builder.build(pushGatewayWrapper);
    }

}
