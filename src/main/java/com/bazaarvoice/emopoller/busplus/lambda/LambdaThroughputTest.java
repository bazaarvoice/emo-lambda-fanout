package com.bazaarvoice.emopoller.busplus.lambda;

import com.amazonaws.services.lambda.AWSLambdaClient;
import com.bazaarvoice.emopoller.busplus.util.BlockingBoundedExecutorFactory;
import com.bazaarvoice.emopoller.metrics.LinuxSysStatCollector;
import com.bazaarvoice.emopoller.metrics.MetricRegistrar;
import com.bazaarvoice.emopoller.metrics.MetricsTelemetry;
import com.bazaarvoice.emopoller.util.JsonUtil;
import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.common.util.concurrent.Service;
import io.dropwizard.cli.Command;
import io.dropwizard.setup.Bootstrap;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class LambdaThroughputTest extends Command {
    private static Logger LOG = LoggerFactory.getLogger(LambdaThroughputTest.class);

    public LambdaThroughputTest() {
        super("lambda-throughput", "test the throughput of Lambda");
    }

    @Override public void configure(final Subparser subparser) {
        subparser.addArgument("--threads");
        subparser.addArgument("--seconds");
        subparser.addArgument("--event");
        subparser.addArgument("--lambda_arn");
    }

    @Override public void run(final Bootstrap<?> bootstrap, final Namespace namespace) throws Exception {
        final int threads = Integer.parseInt(namespace.getString("threads"));
        final int seconds = Integer.parseInt(namespace.getString("seconds"));
        final String event = namespace.getString("event");
        final String arn = namespace.getString("lambda_arn");
        System.out.println("Threads: " + threads);
        System.out.println("Seconds: " + seconds);
        System.out.println("EventFile: " + event);
        System.out.println("LambdaArn: " + arn);

        final File file = new File(event);
        final JsonNode jsonNode = JsonUtil.mapper().readTree(file);
        System.out.println();
        System.out.println("Event:");
        System.out.println(jsonNode);
        System.out.println();

        final AWSLambdaInvocationImpl awsLambdaInvocation = new AWSLambdaInvocationImpl(new AWSLambdaClient());
        final ThreadPoolExecutor executor = BlockingBoundedExecutorFactory.newExecutor(threads, threads, threads, true, Duration.ofMinutes(1), "tput", (t, e) -> LOG.error("err in tput", e), (exec, buffer) -> {
        });
        final MetricRegistry metricRegistry = new MetricRegistry();
        final MetricRegistrar metricRegistrar = new MetricRegistrar(metricRegistry);
        final MetricsTelemetry metricsTelemetry = new MetricsTelemetry(metricRegistry, ImmutableMap.of(), AbstractScheduledService.Scheduler.newFixedDelaySchedule(10, 10, TimeUnit.SECONDS));

        LinuxSysStatCollector.netMonitor.start();

        final Service networkMetrics = new AbstractScheduledService() {
            @Override protected void runOneIteration() throws Exception {
                for (Map.Entry<String, LinuxSysStatCollector.LinuxNetStats.IfaceStats> entry : LinuxSysStatCollector.netMonitor.getNetStats().getStats().entrySet()) {
                    metricRegistrar.histogram("emo_lambda_fanout.network_throughput", ImmutableMap.of("iface", entry.getKey(), "direction", "down")).update((long) entry.getValue().getRxBytesPerSec());
                    metricRegistrar.histogram("emo_lambda_fanout.network_throughput", ImmutableMap.of("iface", entry.getKey(), "direction", "up")).update((long) entry.getValue().getTxBytesPerSec());
                }
            }

            @Override protected Scheduler scheduler() {
                return Scheduler.newFixedRateSchedule(1, 1, TimeUnit.SECONDS);
            }
        };

        networkMetrics.startAsync().awaitRunning();


        metricsTelemetry.start();

        System.out.println("CHECKING PERMS");
        try {
            awsLambdaInvocation.check(arn);
        } catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
            e.printStackTrace(System.out);
            metricsTelemetry.stop();
            networkMetrics.stopAsync().awaitTerminated();
            return;
        }
        System.out.println("CHECKED PERMS");

        final Timer overallTimer = metricRegistrar.timer("emo_lambda_fanout.lambda_time_overall", ImmutableMap.of());
        final Counter counter = metricRegistry.counter("emo_lambda_fanout.current_invocations");
        final long start = System.currentTimeMillis();
        for (int i = 0; i < threads; i++) {
            executor.submit(() -> {
                while (System.currentTimeMillis() - start < seconds * 1000) {
                    final Timer.Context overallTime = overallTimer.time();
                    final Timer.Context threadTimer = metricRegistrar.timer("emo_lambda_fanout.lambda_time", ImmutableMap.of("thread", Thread.currentThread().getName())).time();
                    final long l = System.currentTimeMillis();
                    try {
                        counter.inc();
                        awsLambdaInvocation.invoke(arn, jsonNode);
                        counter.dec();
                    } catch (NoSuchFunctionException | FunctionErrorException | InsufficientPermissionsException e) {
                        throw new RuntimeException(e);
                    }
                    threadTimer.stop();
                    overallTime.stop();
                }
            });
        }

        executor.shutdown();
        executor.awaitTermination(seconds + 30, TimeUnit.SECONDS);
        metricsTelemetry.stop();
        metricsTelemetry.awaitTerminated();
        networkMetrics.stopAsync().awaitTerminated();
        LinuxSysStatCollector.netMonitor.stop();

        System.out.println();
        System.out.println("Results:");
        final ObjectNode telemetry = metricsTelemetry.getTelemetry();
        System.out.println(JsonUtil.mapper().writerWithDefaultPrettyPrinter().writeValueAsString(telemetry));
    }
}
