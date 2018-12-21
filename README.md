# storm-metrics-reporter-prometheus
Storm metrics reporter module that supports [Prometheus Push Gateway](https://github.com/prometheus/pushgateway)

## Motivation

Apache Storm supports the following [metrics reporters](http://storm.apache.org/releases/2.0.0-SNAPSHOT/metrics_v2.html) at the time of writing.

* Console Reporter (`org.apache.storm.metrics2.reporters.ConsoleStormReporter`): Reports metrics to System.out.
* CSV Reporter (`org.apache.storm.metrics2.reporters.CsvReporter`): Reports metrics to a CSV file.
* Ganglia Reporter (`org.apache.storm.metrics2.reporters.GagliaStormReporter`): Reports metrics to a Ganglia server.
* Graphite Reporter (`org.apache.storm.metrics2.reporters.GraphiteStormReporter`): Reports metrics to a Graphite server.
* JMX Reporter (`org.apache.storm.metrics2.reporters.JmxStormReporter`): Exposes metrics via JMX.

The closest which could be used to push data to Prometheus is `org.apache.storm.metrics2.reporters.JmxStormReporter`. That could be put together with [Prometheus JMX exporter](https://github.com/prometheus/jmx_exporter) in theory, there's one pitfall thought.

Prometheus provides a Java agent which spins up a lightweight HTTP server. That doesn't fit well with Storm's architecture, because the supervisor might create multiple worker processes on a single node and those workers would try to open the same HTTP port.

Having looked into `org.apache.storm.metrics2.reporters.GraphiteStormReporter`, it was pretty close what we actually need, but with Prometheus.

## Installation

Download `storm-metrics-reporter-prometheus-1.0.0.jar` from here and put into underneath `{STORM_DIR}/extlib` and/or `${STORM_DIR}/extlib-daemon` depending upon the metrics of which process(es) you want to send to Prometheus.

Add enable Prometheus Metrics Reporter in `storm.yaml`.

```
storm.metrics.reporters:
  # Graphite Reporter
  - class: "nl.wizenoze.storm.metrics2.reporters.PrometheusStormReporter"
    daemons:
        - "supervisor"
        - "nimbus"
        - "worker"
    report.period: 60
    report.period.units: "SECONDS"
    prometheus.scheme: "http"
    prometheus.host: "localhost"
    prometheus.port: 9091
```

Point `prometheus.host` and `prometheus.port` to your [Prometheus Push Gateway](https://github.com/prometheus/pushgateway). You may also adjust `report.period` and `report.period.units` to make it aligned with Prometheus' scrape interval.
