package com.bazaarvoice.emopoller.busplus;

import com.bazaarvoice.emodb.sor.api.AuditBuilder;
import com.bazaarvoice.emodb.sor.api.Intrinsic;
import com.bazaarvoice.emodb.sor.api.TableOptionsBuilder;
import com.bazaarvoice.emodb.sor.condition.Condition;
import com.bazaarvoice.emodb.sor.condition.Conditions;
import com.bazaarvoice.emopoller.EmoPollerConfiguration;
import com.bazaarvoice.emopoller.busplus.kms.ApiKeyCrypto;
import com.bazaarvoice.emopoller.busplus.lambda.AWSLambdaInvocationImpl;
import com.bazaarvoice.emopoller.busplus.lambda.FunctionErrorException;
import com.bazaarvoice.emopoller.busplus.lambda.InsufficientPermissionsException;
import com.bazaarvoice.emopoller.busplus.lambda.LambdaInvocation;
import com.bazaarvoice.emopoller.busplus.lambda.LambdaSubscriptionDAO;
import com.bazaarvoice.emopoller.busplus.lambda.NoSuchFunctionException;
import com.bazaarvoice.emopoller.busplus.util.BlockingBoundedExecutorFactory;
import com.bazaarvoice.emopoller.emo.DataBusClient;
import com.bazaarvoice.emopoller.emo.DataStoreClient;
import com.bazaarvoice.emopoller.emo.DataStoreUtils;
import com.bazaarvoice.emopoller.metrics.MetricRegistrar;
import com.bazaarvoice.emopoller.util.JsonUtil;
import com.bazaarvoice.emopoller.util.HashUtil;
import com.codahale.metrics.Timer;
import com.codahale.metrics.annotation.Timed;
import com.codahale.metrics.health.HealthCheck;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.dropwizard.lifecycle.Managed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vvcephei.occ_map.OCCHashMap;
import org.vvcephei.occ_map.VersionConflictException;
import org.vvcephei.occ_map.Versioned;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Spliterator;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.bazaarvoice.emopoller.busplus.lambda.LambdaSubscriptionDAO.activateDelta;
import static com.bazaarvoice.emopoller.busplus.lambda.LambdaSubscriptionDAO.createDelta;
import static com.bazaarvoice.emopoller.busplus.lambda.LambdaSubscriptionDAO.deactivateDelta;
import static com.bazaarvoice.emopoller.busplus.lambda.LambdaSubscriptionDAO.getActive;
import static com.bazaarvoice.emopoller.busplus.lambda.LambdaSubscriptionDAO.getDelegateApiKeyArgon2Hash;
import static com.bazaarvoice.emopoller.busplus.lambda.LambdaSubscriptionDAO.getDelegateApiKeyHash;
import static com.bazaarvoice.emopoller.busplus.lambda.LambdaSubscriptionDAO.getId;
import static com.bazaarvoice.emopoller.busplus.lambda.LambdaSubscriptionDAO.getLambdaArn;
import static com.bazaarvoice.emopoller.busplus.lambda.LambdaSubscriptionDAO.getSubscriptionName;
import static com.google.common.collect.ImmutableMap.of;

public class LambdaSubscriptionManager implements Managed {


    private final DataStoreClient dataStoreClient;
    private final DataBusClient dataBusClient;
    private final EmoPollerConfiguration.LambdaConfiguration lambdaConfiguration;
    private final MetricRegistrar metricRegistrar;
    private final LambdaInvocation lambdaInvocation;
    private final ApiKeyCrypto apiKeyCrypto;
    private final String managerApiKey;

    private static final Duration SUBSCRIPTION_TTL = Duration.ofDays(2);
    private static final Duration EVENT_TTL = Duration.ofDays(30);
    private static final Logger LOG = LoggerFactory.getLogger(LambdaSubscriptionManager.class);


    private boolean started = false;
    private final Object startedMutex = new Object();
    private Service ticker;
    private Service metadataPoller;
    private ListeningExecutorService processExecutor;
    private final String subscriptionTable;
    private final String subscriptionPrefix;

    private static class EmoJson implements Versioned {
        private final JsonNode jsonNode;

        public EmoJson(JsonNode jsonNode) {
            this.jsonNode = jsonNode;
        }

        @Override public long getVersion() {
            return jsonNode.get("~version").asLong();
        }

        public String getId() {
            return jsonNode.get("~id").asText();
        }

        public JsonNode getJsonNode() {
            return jsonNode;
        }
    }

    private OCCHashMap<String, EmoJson> subscriptions = new OCCHashMap<>();

    @Inject
    public LambdaSubscriptionManager(final DataStoreClient dataStoreClient,
                                     final DataBusClient dataBusClient,
                                     final EmoPollerConfiguration.EmoConfiguration emoConfiguration,
                                     final EmoPollerConfiguration.LambdaConfiguration lambdaConfiguration,
                                     final LambdaInvocation lambdaInvocation,
                                     final ApiKeyCrypto apiKeyCrypto,
                                     final MetricRegistrar metricRegistrar) {
        this.dataStoreClient = dataStoreClient;
        this.dataBusClient = dataBusClient;
        this.lambdaConfiguration = lambdaConfiguration;
        this.metricRegistrar = metricRegistrar;
        this.subscriptionTable = emoConfiguration.getSubscriptionTable();
        this.subscriptionPrefix = emoConfiguration.getSubscriptionPrefix();
        this.managerApiKey = apiKeyCrypto.decryptCustom(emoConfiguration.getApiKey(), of("app", "emo-lambda-fanout"));
        this.lambdaInvocation = lambdaInvocation;
        this.apiKeyCrypto = apiKeyCrypto;
    }

    @Override public void start() throws Exception {
        LOG.info("starting");
        synchronized (startedMutex) {
            Preconditions.checkState(!started, "start() may only be called once.");
            started = true;

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

            ticker = new AbstractScheduledService() {
                private final ListeningExecutorService executorService = MoreExecutors.listeningDecorator(
                    Executors.newCachedThreadPool(new ThreadFactoryBuilder()
                        .setNameFormat("pollall-%d")
                        .setUncaughtExceptionHandler((thread, throwable) -> LOG.error("[" + thread.getName() + "] Uncaught exception draining subscription.", throwable))
                        .build()
                    )
                );

                @Override protected void shutDown() throws Exception {
                    super.shutDown();
                    executorService.shutdown();
                }

                @Override protected void runOneIteration() throws Exception {
                    final List<ListenableFuture<?>> futures = new ArrayList<>(lambdaConfiguration.getPollAllConcurrency());
                    for (int i = 0; i < lambdaConfiguration.getPollAllConcurrency(); i++) {
                        futures.add(executorService.submit(() -> pollAll()));
                    }
                    Futures.allAsList(futures).get();
                }

                @Override protected Scheduler scheduler() {
                    return Scheduler.newFixedDelaySchedule(10000, 1, TimeUnit.MILLISECONDS);
                }
            };

            metadataPoller = new AbstractScheduledService() {
                @Override protected void runOneIteration() throws Exception {
                    dataBusClient.subscribe(subscriptionPrefix + "-subscriptions", Conditions.intrinsic(Intrinsic.TABLE, subscriptionTable), Duration.ofDays(7), Duration.ofDays(1000), managerApiKey);
                    final List<JsonNode> poll = dataBusClient.poll(subscriptionPrefix + "-subscriptions", Duration.ofMinutes(10), 999, managerApiKey);
                    poll.parallelStream()
                        .forEach(jsonNode -> {
                            final EmoJson emoJson = new EmoJson(jsonNode.get("content"));
                            try {
                                subscriptions.put(emoJson.getId(), emoJson);
                            } catch (VersionConflictException e) {
                                // ignore
                            }
                            dataBusClient.acknowledge(subscriptionPrefix + "-subscriptions", jsonNode.get("eventKey").asText(), managerApiKey);
                        });
                    LOG.info("Polling subscription metadata returned [{}] events", poll.size());
                }

                @Override protected Scheduler scheduler() {
                    return Scheduler.newFixedDelaySchedule(0, 1, TimeUnit.SECONDS);
                }
            };

            dataStoreClient.updateTable(
                subscriptionTable,
                new TableOptionsBuilder().setPlacement("app_global:default").build(),
                JsonUtil.obj(),
                new AuditBuilder().setComment("Ensuring table exists").build(),
                managerApiKey);

            dataBusClient.subscribe(subscriptionPrefix + "-subscriptions", Conditions.intrinsic(Intrinsic.TABLE, subscriptionTable), Duration.ofDays(7), Duration.ofDays(1000), managerApiKey);
            metadataPoller.startAsync().awaitRunning();
            StreamSupport.stream(DataStoreUtils.pscan(dataStoreClient, subscriptionTable, managerApiKey).spliterator(), true)
                .map(EmoJson::new)
                .forEach(emoJson -> {
                    try {
                        subscriptions.put(emoJson.getId(), emoJson);
                    } catch (VersionConflictException e) {
                        // ignore
                    }
                });

            ticker.startAsync().awaitRunning();
            LOG.info("started");
        }
    }

    @Override public void stop() throws Exception {
        LOG.info("stopping");
        ticker.stopAsync().awaitTerminated();
        metadataPoller.stopAsync().awaitTerminated();
        LOG.info("stopped");
    }

    public HealthCheck healthcheck() {
        return new HealthCheck() {
            @Override protected Result check() throws Exception {
                if (ticker.isRunning()) {
                    return Result.healthy();
                } else {
                    return Result.unhealthy("ticker is not running");
                }
            }
        };
    }

    public void register(final String lambdaArn, final Condition condition, final Duration claimTtl, final Integer batchSize, final String delegateApiKey)
        throws NoSuchFunctionException, InsufficientPermissionsException {
        lambdaInvocation.check(lambdaArn);

        final JsonNode jsonNode = internalGet(lambdaArn);

        final String subscriptionName = getSubscriptionName(jsonNode, subscriptionPrefix + "-" + UUID.randomUUID());

        final String cypherTextDelegateApiKey = apiKeyCrypto.encrypt(delegateApiKey, subscriptionName, lambdaArn);

        dataBusClient.subscribe(subscriptionName, condition, SUBSCRIPTION_TTL, EVENT_TTL, delegateApiKey);

        final String subscriptionId = HashUtil.sha512hash_base16(lambdaArn);
        dataStoreClient.update(
            subscriptionTable,
            subscriptionId, // yeah, we should only hand events from a subscription out to a single lambda at a time.
            createDelta(new LambdaSubscriptionDAO.LambdaSubscription(subscriptionId, subscriptionName, lambdaArn, condition, claimTtl, batchSize, delegateApiKey, cypherTextDelegateApiKey)),
            new AuditBuilder().setComment("Registering lambda[" + lambdaArn + "] for subscription").build(),
            ImmutableList.of(),
            managerApiKey
        );

        // (re)set error count to zero on (re)registration
        errorCount.putIfAbsent(subscriptionId, new AtomicInteger(0));
        errorCount.get(subscriptionId).set(0);
    }

    public void deactivate(final String lambdaArn, final String delegateApiKey) {
        final JsonNode jsonNode = get(lambdaArn, delegateApiKey);
        deactivate(getId(jsonNode));
    }

    public void activate(final String lambdaArn, final String delegateApiKey) {
        final JsonNode jsonNode = get(lambdaArn, delegateApiKey);
        activate(getId(jsonNode));
    }

    public static class UnauthorizedException extends RuntimeException {}

    public JsonNode getAll(final String key) {
        final ArrayNode result = JsonUtil.arr();
        for (JsonNode jsonNode : DataStoreUtils.pscan(dataStoreClient, subscriptionTable, managerApiKey)) {
            if (jsonNode != null) {
                if (!getDelegateApiKeyArgon2Hash(jsonNode).isEmpty()) {
                    final String delegateApiKeyHash = getDelegateApiKeyArgon2Hash(jsonNode);
                    if (HashUtil.verifyArgon2(delegateApiKeyHash, key)) {
                        result.add(jsonNode);
                    }
                } else {
                    final String delegateApiKeyHash = getDelegateApiKeyHash(jsonNode);
                    @SuppressWarnings("deprecation") final String keyHash = HashUtil.sha512hash_base64(key);
                    if (delegateApiKeyHash.equals(keyHash)) {
                        result.add(jsonNode);
                    }
                }
            }
        }
        return result;
    }

    public Integer size(final String lambdaArn, final String delegateApiKey) {
        final JsonNode jsonNode = get(lambdaArn, delegateApiKey);
        return dataBusClient.size(getSubscriptionName(jsonNode), 1000, delegateApiKey);
    }

    public JsonNode get(final String lambdaArn, final String delegateApiKey) {
        final JsonNode jsonNode = internalGet(lambdaArn);
        if (! getDelegateApiKeyArgon2Hash(jsonNode).isEmpty()) {
            if (HashUtil.verifyArgon2(getDelegateApiKeyArgon2Hash(jsonNode), delegateApiKey)) {
                return jsonNode;
            } else {
                throw new UnauthorizedException();
            }
        } else {
            @SuppressWarnings("deprecation") final String toVerify = HashUtil.sha512hash_base64(delegateApiKey);
            if (getDelegateApiKeyHash(jsonNode).equals(toVerify)) {
                return jsonNode;
            } else {
                throw new UnauthorizedException();
            }
        }
    }

    private JsonNode internalGet(final String lambdaArn) {return dataStoreClient.get(subscriptionTable, HashUtil.sha512hash_base16(lambdaArn), managerApiKey);}

    private void deactivate(final String id) {
        dataStoreClient.update(
            subscriptionTable,
            id,
            deactivateDelta(),
            new AuditBuilder().setComment("deactivating subscription").build(),
            ImmutableList.of(),
            managerApiKey
        );
    }

    private void activate(final String id) {
        dataStoreClient.update(
            subscriptionTable,
            id,
            activateDelta(),
            new AuditBuilder().setComment("activating subscription").build(),
            ImmutableList.of(),
            managerApiKey
        );
    }

    private static class Tuple3<A, B, C> {
        private A a;
        private B b;
        private C c;

        private Tuple3(final A a, final B b, final C c) {
            this.a = a;
            this.b = b;
            this.c = c;
        }
    }

    private void pollAll() {
        final Timer.Context time = metricRegistrar.timer("emo_lambda_fanout.pollAll.time", ImmutableMap.of()).time();
        LOG.info("polling all subs: " + Thread.currentThread().getName());
        try {
            subscriptions.values().parallelStream()
                .map(EmoJson::getJsonNode)
                .map(jsonNode -> {
                    registerSubscriptionSizeGauge(jsonNode, ImmutableMap.of("lambda_arn", getLambdaArn(jsonNode).replaceAll("[:]", "_")));
                    return jsonNode;
                })
                .filter(LambdaSubscriptionDAO::getActive)
                .map(LambdaSubscriptionDAO::asLambdaSubscription)
                .flatMap(lambdaSubscription -> {
                    try {
                        LOG.debug("enqueuing lambda {} {}", lambdaSubscription.getSubscriptionName(), lambdaSubscription.getLambdaArn());

                        final String delegateApiKey = apiKeyCrypto.decrypt(lambdaSubscription.getCypherTextDelegateApiKey(), lambdaSubscription.getSubscriptionName(), lambdaSubscription.getLambdaArn());

                        LOG.debug("Renewing sub[{}]", lambdaSubscription.getSubscriptionName());
                        dataBusClient.subscribe(lambdaSubscription.getSubscriptionName(), lambdaSubscription.getCondition(), SUBSCRIPTION_TTL, EVENT_TTL, delegateApiKey);

                        LOG.debug("Polling sub[{}]", lambdaSubscription.getSubscriptionName());

                        List<JsonNode> poll = dataBusClient.poll(lambdaSubscription.getSubscriptionName(), lambdaSubscription.getClaimTtl(), lambdaConfiguration.getPollSize(), delegateApiKey);
                        metricRegistrar
                            .histogram("emo_lambda_fanout.poll.events", ImmutableMap.of("lambda_arn", lambdaSubscription.getLambdaArn().replaceAll("[:]", "_")))
                            .update(poll.size());
                        LOG.info("Polled [{}] events for arn[{}]", poll.size(), lambdaSubscription.getLambdaArn());

                        return Lists
                            .partition(poll, lambdaSubscription.getBatchSize())
                            .parallelStream()
                            .map(batch -> new Tuple3<>(lambdaSubscription, batch, delegateApiKey));
                    } catch (Exception e) {
                        LOG.error(String.format("error handling sub [%s] [%s]", lambdaSubscription.getSubscriptionName(), lambdaSubscription.getLambdaArn()), e);
                        throw e;
                    }
                })
                .forEach((tuple3 -> processExecutor.submit(() -> process(tuple3.a, tuple3.b, tuple3.c))));
        } catch (Exception e) {
            LOG.error("error in pollAll", e);
        }
        time.stop();
    }

    private Spliterator<JsonNode> scanSubscriptions() {
        final Timer.Context time = metricRegistrar.timer("emo_lambda_fanout.datastore.get_pscan_iterator.time", ImmutableMap.of()).time();
        final Spliterator<JsonNode> spliterator = DataStoreUtils.pscan(dataStoreClient, subscriptionTable, managerApiKey).spliterator();
        time.stop();
        return spliterator;
    }

    private void registerSubscriptionSizeGauge(final JsonNode jsonNode, final ImmutableMap<String, String> tags) {
        final String metricName = "emo_lambda_fanout.subscription.size";
        if (getActive(jsonNode)) {
            LOG.debug("Registering gauge for [{}] [{}]", metricName, tags);

            metricRegistrar.register(
                metricName, tags,
                () -> {
                    final String subscriptionName = getSubscriptionName(jsonNode);
                    return dataBusClient.size(subscriptionName, 1000, managerApiKey);
                }
            );
        } else {
            LOG.debug("Deregistering gauge for [{}] [{}]", metricName, tags);
            metricRegistrar.removeGauge(metricName, tags);
        }
    }

    private ConcurrentHashMap<String, AtomicInteger> errorCount = new ConcurrentHashMap<>();


    @Timed(name = "emo_lambda_fanout.poll.time")
    private void process(final LambdaSubscriptionDAO.LambdaSubscription lambdaSubscription, final List<JsonNode> batch, final String delegateApiKey) {
        // we may have been queued up for quite a while, so check again if this subscription is active
        if (!getActive(subscriptions.get(lambdaSubscription.getId()).getJsonNode())) {
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
                final ArrayNode contents = JsonUtil.arr(batch.stream().map(n -> n.get("content")));
                final AWSLambdaInvocationImpl.InvocationResult result = lambdaInvocation.invoke(lambdaSubscription.getLambdaArn(), contents);
                final long durationMillis = System.currentTimeMillis() - start;
                metricRegistrar
                    .timer("emo_lambda_fanout.execution.time", ImmutableMap.of("lambda_arn", lambdaSubscription.getLambdaArn().replaceAll("[:]", "_")))
                    .update(durationMillis, TimeUnit.MILLISECONDS);
                optionallyReportFunctionTime(lambdaSubscription.getLambdaArn(), result, durationMillis);


                // ACKNOWLEDGE
                final Timer.Context ackTimer = metricRegistrar.timer("emo_lambda_fanout.execution.ackTime", ImmutableMap.of()).time();

                final List<String> eventKeys = batch.stream().map(n -> n.get("eventKey").asText()).collect(Collectors.toList());
                dataBusClient.acknowledge(lambdaSubscription.getSubscriptionName(), eventKeys, delegateApiKey);

                ackTimer.stop();

                errorCount.get(lambdaSubscription.getId()).set(0);

                LOG.debug(String.format("Successfully ran [%s] for sub[%s] in [%dms]", lambdaSubscription.getLambdaArn(), lambdaSubscription.getSubscriptionName(), durationMillis));
            } catch (NoSuchFunctionException e) {
                LOG.info(String.format("Deactivating [%s] due to missing function [%s]", lambdaSubscription.getSubscriptionName(), lambdaSubscription.getLambdaArn()), e);
                deactivate(lambdaSubscription.getId());
            } catch (FunctionErrorException e) {
                final int errors = errorCount.get(lambdaSubscription.getId()).incrementAndGet();
                LOG.warn(String.format("Error #[%d] running [%s] for sub[%s]", errors, lambdaSubscription.getLambdaArn(), lambdaSubscription.getSubscriptionName()), e);
                if (errors > 100000) {
                    LOG.warn(String.format("Deactivating due to > 100,000 of: Error running [%s] for sub[%s]", lambdaSubscription.getLambdaArn(), lambdaSubscription.getSubscriptionName()), e);
                    deactivate(lambdaSubscription.getId());
                }
                metricRegistrar.counter("emo_lambda_fanout.execution.totalFunctionErrors", ImmutableMap.of("lambda_arn", lambdaSubscription.getLambdaArn().replaceAll("[:]", "_"))).inc();
            } catch (InsufficientPermissionsException e) {
                LOG.info(String.format("Deactivating: Error running [%s] for sub[%s]", lambdaSubscription.getLambdaArn(), lambdaSubscription.getSubscriptionName()), e);
                deactivate(lambdaSubscription.getId());
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
