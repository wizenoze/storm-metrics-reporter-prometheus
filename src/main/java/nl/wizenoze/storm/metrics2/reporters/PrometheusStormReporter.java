package nl.wizenoze.storm.metrics2.reporters;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteSender;
import com.codahale.metrics.graphite.GraphiteUDP;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import nl.wizenoze.prometheus.PrometheusReporter;
import org.apache.storm.daemon.metrics.MetricsUtils;
import org.apache.storm.metrics2.filters.StormMetricsFilter;
import org.apache.storm.metrics2.reporters.ScheduledStormReporter;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PrometheusStormReporter extends ScheduledStormReporter {

    private final static Logger LOG = LoggerFactory.getLogger(PrometheusStormReporter.class);

    public static final String PROMETHEUS_PREFIXED_WITH = "prometheus.prefixed.with";
    public static final String PROMETHEUS_HOST = "prometheus.host";
    public static final String PROMETHEUS_PORT = "prometheus.port";
    public static final String PROMETHEUS_TRANSPORT = "prometheus.transport";

    @Override
    public void prepare(MetricRegistry metricsRegistry, Map stormConf, Map reporterConf) {
        LOG.debug("Preparing...");
        PrometheusReporter.Builder builder = PrometheusReporter.forRegistry(metricsRegistry);

        TimeUnit durationUnit = MetricsUtils.getMetricsDurationUnit(reporterConf);
        if (durationUnit != null) {
            builder.convertDurationsTo(durationUnit);
        }

        TimeUnit rateUnit = MetricsUtils.getMetricsRateUnit(reporterConf);
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
        String transport = getMetricsTargetTransport(reporterConf);
        GraphiteSender sender = null;
        if (transport.equalsIgnoreCase("udp")) {
            sender = new GraphiteUDP(host, port);
        } else {
            sender = new Graphite(host, port);
        }
        reporter = builder.build(sender);
    }

    private static String getMetricsPrefixedWith(Map reporterConf) {
        return Utils.getString(reporterConf.get(PROMETHEUS_PREFIXED_WITH), null);
    }

    private static String getMetricsTargetHost(Map reporterConf) {
        return Utils.getString(reporterConf.get(PROMETHEUS_HOST), null);
    }

    private static Integer getMetricsTargetPort(Map reporterConf) {
        return Utils.getInt(reporterConf.get(PROMETHEUS_PORT), null);
    }

    private static String getMetricsTargetTransport(Map reporterConf) {
        return Utils.getString(reporterConf.get(PROMETHEUS_TRANSPORT), "tcp");
    }

}
