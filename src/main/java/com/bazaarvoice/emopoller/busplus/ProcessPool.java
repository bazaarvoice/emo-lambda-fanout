package com.bazaarvoice.emopoller.busplus;

import com.bazaarvoice.emopoller.EmoPollerConfiguration;
import com.bazaarvoice.emopoller.busplus.lambda.AWSLambdaInvocationImpl;
import com.bazaarvoice.emopoller.busplus.lambda.FunctionErrorException;
import com.bazaarvoice.emopoller.busplus.lambda.InsufficientPermissionsException;
import com.bazaarvoice.emopoller.busplus.lambda.LambdaInvocation;
import com.bazaarvoice.emopoller.busplus.lambda.LambdaSubscriptionDAO;
import com.bazaarvoice.emopoller.busplus.lambda.NoSuchFunctionException;
import com.bazaarvoice.emopoller.busplus.lambda.model.LambdaSubscription;
import com.bazaarvoice.emopoller.busplus.util.BlockingBoundedExecutorFactory;
import com.bazaarvoice.emopoller.metrics.MetricRegistrar;
import com.bazaarvoice.emopoller.util.JsonUtil;
import com.codahale.metrics.Timer;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import io.dropwizard.lifecycle.Managed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ProcessPool implements Managed {
    private static final Logger LOG = LoggerFactory.getLogger(ProcessPool.class);
    private final ListeningExecutorService processExecutor;
    private final LambdaInvocation lambdaInvocation;
    private final LambdaSubscriptionDAO lambdaSubscriptionDAO;
    private final MetricRegistrar metricRegistrar;
    private ConcurrentHashMap<String, AtomicInteger> errorCount = new ConcurrentHashMap<>();

    @Inject public ProcessPool(final EmoPollerConfiguration.LambdaConfiguration lambdaConfiguration,
                               final LambdaInvocation lambdaInvocation,
                               final LambdaSubscriptionDAO lambdaSubscriptionDAO,
                               final MetricRegistrar metricRegistrar) {
        this.lambdaInvocation = lambdaInvocation;
        this.lambdaSubscriptionDAO = lambdaSubscriptionDAO;
        this.metricRegistrar = metricRegistrar;
        {
            final ThreadPoolExecutor processExecutorService =
                BlockingBoundedExecutorFactory.newExecutor(
                    lambdaConfiguration.getProcessPoolSize(), lambdaConfiguration.getProcessPoolSize(), lambdaConfiguration.getProcessQueueSize(), //core, max pool size, buffer
                    true, // fair
                    Duration.ofSeconds(60L),
                    "poll-sub",
                    (thread, throwable) -> LOG.error("[" + thread.getName() + "] Uncaught exception draining subscription.", throwable),
                    (executor, bufferedWork) -> {
                        if (metricRegistrar != null) {
                            metricRegistrar.register("emo_lambda_fanout.poll.pool.max", executor::getMaximumPoolSize);
                            metricRegistrar.register("emo_lambda_fanout.poll.pool.largest", executor::getLargestPoolSize);
                            metricRegistrar.register("emo_lambda_fanout.poll.pool.core", executor::getCorePoolSize);
                            metricRegistrar.register("emo_lambda_fanout.poll.pool.current", executor::getPoolSize);
                            metricRegistrar.register("emo_lambda_fanout.poll.pool.queue", bufferedWork::size);
                        }
                    }
                );
            this.processExecutor = MoreExecutors.listeningDecorator(processExecutorService);
        }
    }

    @Override public void start() throws Exception { }

    @Override public void stop() throws Exception {
        processExecutor.shutdown();
        while (!processExecutor.isShutdown()) {
            LOG.info("waiting on processExecutor to shut down...");
            Thread.sleep(500);
        }
    }

    void resetErrorCount(final String subscriptionId) {
        errorCount.putIfAbsent(subscriptionId, new AtomicInteger(0));
        errorCount.get(subscriptionId).set(0);
    }

    void submit(final LambdaSubscription lambdaSubscription, final List<JsonNode> batch, final OnBatchCompleteCallback onBatchCompleteCallback) {
        processExecutor.submit(() -> process(lambdaSubscription, batch, onBatchCompleteCallback));
    }

    interface OnBatchCompleteCallback {
        void onComplete();
    }

    @Timed(name = "emo_lambda_fanout.poll.time")
    private void process(final LambdaSubscription lambdaSubscription, final List<JsonNode> batch, final OnBatchCompleteCallback onBatchCompleteCallback) {
        // we may have been queued up for a while, so check again if this subscription is active
        if (!lambdaSubscriptionDAO.get(lambdaSubscription.getEnvironment(), lambdaSubscription.getLambdaArn()).isActive()) {
            LOG.info("Started processing [{}], which is currently deactivated. Bombing out instead...", lambdaSubscription.getLambdaArn());
            return;
        }

        errorCount.putIfAbsent(lambdaSubscription.getId(), new AtomicInteger(0));
        metricRegistrar.register(
            "emo_lambda_fanout.execution.functionErrors",
            ImmutableMap.of("lambda_arn", lambdaSubscription.getLambdaArn().replaceAll("[:]", "_")),
            () -> errorCount.get(lambdaSubscription.getId()).get()
        );

        final long pollStart = System.currentTimeMillis();


        LOG.debug(
            "Starting [{}] events for arn[{}] sub[{}]",
            batch.size(),
            lambdaSubscription.getLambdaArn(),
            lambdaSubscription.getSubscriptionName()
        );

        final Timer.Context totalTimer = metricRegistrar
            .timer("emo_lambda_fanout.execution.totalTime", ImmutableMap.of("lambda_arn", lambdaSubscription.getLambdaArn().replaceAll("[:]", "_")))
            .time();
        LOG.debug("Starting [{}] for sub[{}]. Batch of [{}]", lambdaSubscription.getLambdaArn(), lambdaSubscription.getSubscriptionName(), batch.size());
        try {
            try {
                // INVOCATION
                final long start = System.currentTimeMillis();
                final ArrayNode contents = JsonUtil.arr(batch.stream().map(jn -> ((ObjectNode)jn).put("~fanout.env", lambdaSubscription.getEnvironment())));
                final AWSLambdaInvocationImpl.InvocationResult result = lambdaInvocation.invoke(lambdaSubscription.getLambdaArn(), contents);
                final long durationMillis = System.currentTimeMillis() - start;
                metricRegistrar
                    .timer("emo_lambda_fanout.execution.time", ImmutableMap.of("lambda_arn", lambdaSubscription.getLambdaArn().replaceAll("[:]", "_")))
                    .update(durationMillis, TimeUnit.MILLISECONDS);
                optionallyReportFunctionTime(lambdaSubscription.getLambdaArn(), result, durationMillis);

                onBatchCompleteCallback.onComplete();

                errorCount.get(lambdaSubscription.getId()).set(0);

                LOG.debug(String.format("Successfully ran [%s] for sub[%s] in [%dms]", lambdaSubscription.getLambdaArn(), lambdaSubscription.getSubscriptionName(), durationMillis));
            } catch (NoSuchFunctionException e) {
                LOG.info(String.format("Deactivating [%s] due to missing function [%s]", lambdaSubscription.getSubscriptionName(), lambdaSubscription.getLambdaArn()), e);
                lambdaSubscriptionDAO.deactivate(lambdaSubscription.getId());
            } catch (FunctionErrorException e) {
                final int errors = errorCount.get(lambdaSubscription.getId()).incrementAndGet();
                LOG.warn(String.format("Error #[%d] running [%s] for sub[%s]", errors, lambdaSubscription.getLambdaArn(), lambdaSubscription.getSubscriptionName()), e);
                if (errors > 100000) {
                    LOG.warn(String.format("Deactivating due to > 100,000 of: Error running [%s] for sub[%s]", lambdaSubscription.getLambdaArn(), lambdaSubscription.getSubscriptionName()), e);
                    lambdaSubscriptionDAO.deactivate(lambdaSubscription.getId());
                }
                metricRegistrar.counter("emo_lambda_fanout.execution.totalFunctionErrors", ImmutableMap.of("lambda_arn", lambdaSubscription.getLambdaArn().replaceAll("[:]", "_"))).inc();
            } catch (InsufficientPermissionsException e) {
                LOG.info(String.format("Deactivating: Error running [%s] for sub[%s]", lambdaSubscription.getLambdaArn(), lambdaSubscription.getSubscriptionName()), e);
                lambdaSubscriptionDAO.deactivate(lambdaSubscription.getId());
            }
        } catch (Exception e) { // so we're sure to also catch any exceptions thrown in the catch blocks
            LOG.error(String.format("Unhandled exception invoking function [%s] for sub[%s]", lambdaSubscription.getLambdaArn(), lambdaSubscription.getSubscriptionName()), e);
        }
        totalTimer.stop();

        final long duration = System.currentTimeMillis() - pollStart;

        LOG.debug("Handled [{}] events for arn[{}] sub[{}] in [{}]ms", batch.size(), lambdaSubscription.getLambdaArn(), lambdaSubscription.getSubscriptionName(), duration);
    }

    // Cooperative functions can return Json with an "executionTimeMs" property
    private void optionallyReportFunctionTime(final String lambdaArn, final AWSLambdaInvocationImpl.InvocationResult result, final long subjectiveDuration) {
        final String resultPayload = result.getResultPayload();
        try {
            final JsonNode jsonNode = JsonUtil.mapper().readTree(resultPayload);
            final JsonNode node = jsonNode.path("executionTimeMs");
            if (node.isNumber()) {
                metricRegistrar
                    .timer("emo_lambda_fanout.execution.lambdaTime", ImmutableMap.of("lambda_arn", lambdaArn.replaceAll("[:]", "_")))
                    .update(node.asInt(), TimeUnit.MILLISECONDS);
                metricRegistrar
                    .timer("emo_lambda_fanout.execution.invocationOverhead", ImmutableMap.of("lambda_arn", lambdaArn.replaceAll("[:]", "_")))
                    .update(subjectiveDuration - node.asInt(), TimeUnit.MILLISECONDS);
            }
        } catch (IOException e) {
            // no biggie
        }
    }
}
