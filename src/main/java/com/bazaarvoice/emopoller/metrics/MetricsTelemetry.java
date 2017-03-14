package com.bazaarvoice.emopoller.metrics;

import com.bazaarvoice.emopoller.util.JsonUtil;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AbstractScheduledService;
import io.dropwizard.lifecycle.Managed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

public class MetricsTelemetry extends AbstractScheduledService implements Managed {
    private static final Logger LOG = LoggerFactory.getLogger(MetricsTelemetry.class);
    private final MetricRegistry metricRegistry;
    private final ImmutableMap<String, String> tags;
    private Scheduler schedule;

    public MetricsTelemetry(final MetricRegistry metricRegistry,
                            final ImmutableMap<String, String> tags,
                            final Scheduler schedule) {
        this.metricRegistry = metricRegistry;
        this.tags = tags;
        this.schedule = schedule;
    }

    @Override protected void runOneIteration() {
        emit();
    }

    public void emit() {
        try {
            final ObjectNode telem = getTelemetry();
            System.out.println(JsonUtil.mapper().writeValueAsString(telem));
        } catch (Exception e) {
            LOG.error("uncaught", e);
        }
    }

    public ObjectNode getTelemetry() {
        final ObjectNode gauges = JsonUtil.obj();
        metricRegistry.getGauges().entrySet().stream()
            .filter((kv) ->
                kv.getKey().startsWith("emo_lambda_fanout") ||
                    kv.getKey().startsWith("jvm.memory.heap") ||
                    kv.getKey().startsWith("jvm.memory.non-heap") ||
                    kv.getKey().startsWith("jvm.memory.total") ||
                    kv.getKey().startsWith("jvm.gc")
            )
            .forEach((kv) -> gauges.putPOJO(kv.getKey(), kv.getValue()));

        final ObjectNode timers = JsonUtil.obj();
        metricRegistry.getTimers().entrySet().stream()
            .filter((kv) ->
                kv.getKey().startsWith("emo_lambda_fanout") ||
                    kv.getKey().startsWith("com.bazaarvoice.emopoller")
            )
            .forEach((kv) -> {
                final Timer timer = kv.getValue();
                final JsonNode node = JsonUtil.toTree(timer);
                if (node.path("snapshot").isObject()) ((ObjectNode) node.path("snapshot")).remove("values");
                timers.put(kv.getKey(), node);
            });

        final ObjectNode meters = JsonUtil.obj();
        metricRegistry.getMeters().entrySet().stream()
            .filter((kv) ->
                kv.getKey().startsWith("ch.qos.logback.core.Appender")
            )
            .forEach((kv) -> meters.putPOJO(kv.getKey(), kv.getValue()));

        final ObjectNode histograms = JsonUtil.obj();
        metricRegistry.getHistograms().entrySet().stream()
            .filter((kv) ->
                kv.getKey().startsWith("emo_lambda_fanout")
            )
            .forEach((kv) -> {
                final Histogram histo = kv.getValue();
                final JsonNode node = JsonUtil.toTree(histo);
                if (node.path("snapshot").isObject()) ((ObjectNode) node.path("snapshot")).remove("values");
                histograms.put(kv.getKey(), node);
            });

        final ObjectNode counters = JsonUtil.obj();
        metricRegistry.getCounters().entrySet().stream()
            .filter((kv) ->
                kv.getKey().startsWith("emo_lambda_fanout")
            )
            .forEach((kv) -> counters.putPOJO(kv.getKey(), kv.getValue()));


        return JsonUtil.obj(
            "type", "metric",
            "time", Instant.now().toString(),
            "tags", JsonUtil.toTree(tags),
            "gauges", gauges,
            "timers", timers,
            "meters", meters,
            "histograms", histograms,
            "counters", counters,
            "system", JsonUtil.obj(
                "load", safeLoad(),
                "cpu", safeStat(),
                "mem", safeMem()
            )
        );
    }

    private JsonNode safeStat() {
        try {
            return JsonUtil.toTree(LinuxSysStatCollector.getCPUStat());
        } catch (Exception e) {
            LOG.error("Couldn't collect cpu stats", e);
            return JsonUtil.nul();
        }
    }

    private JsonNode safeLoad() {
        try {
            return JsonUtil.toTree(LinuxSysStatCollector.getLoad());
        } catch (Exception e) {
            LOG.error("Couldn't collect system load", e);
            return JsonUtil.nul();
        }
    }

    private JsonNode safeMem() {
        try {
            return JsonUtil.toTree(LinuxSysStatCollector.getMemStats());
        } catch (Exception e) {
            LOG.error("Couldn't collect memory stats", e);
            return JsonUtil.nul();
        }
    }

    @Override protected Scheduler scheduler() {
        return schedule;
    }

    @Override public void start() throws Exception {
        LOG.info("starting");
        this.startAsync().awaitRunning();
        LOG.info("started");
    }

    @Override public void stop() throws Exception {
        LOG.info("stopping");
        this.stopAsync().awaitTerminated();
        LOG.info("stopped");
    }
}
