package com.bazaarvoice.emopoller.busplus.lambda;

import com.bazaarvoice.emodb.sor.api.Intrinsic;
import com.bazaarvoice.emodb.sor.condition.Conditions;
import com.bazaarvoice.emodb.sor.delta.Delta;
import com.bazaarvoice.emodb.sor.delta.Deltas;
import com.bazaarvoice.emopoller.EmoPollerConfiguration;
import com.bazaarvoice.emopoller.busplus.kms.ApiKeyCrypto;
import com.bazaarvoice.emopoller.busplus.lambda.model.LambdaSubscription;
import com.bazaarvoice.emopoller.emo.DataBusClient;
import com.bazaarvoice.emopoller.emo.DataStoreClient;
import com.bazaarvoice.emopoller.emo.DataStoreUtils;
import com.bazaarvoice.emopoller.util.HashUtil;
import com.bazaarvoice.emopoller.util.JsonUtil;
import com.codahale.metrics.health.HealthCheck;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.common.util.concurrent.Service;
import com.google.inject.Inject;
import io.dropwizard.lifecycle.Managed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vvcephei.occ_map.OCCHashMap;
import org.vvcephei.occ_map.VersionConflictException;

import java.time.Duration;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.google.common.collect.ImmutableMap.of;

public class LambdaSubscriptionDAOImpl implements LambdaSubscriptionDAO, Managed {
    private static final Logger LOG = LoggerFactory.getLogger(LambdaSubscriptionDAOImpl.class);

    private final DataStoreClient dataStoreClient;
    private final DataBusClient dataBusClient;
    private final String subscriptionTable;
    private final String subscriptionPrefix;
    private final String managerApiKey;
    private final Service metadataPoller;

    private final OCCHashMap<String, LambdaSubscription> subscriptions = new OCCHashMap<>();
    private final ConcurrentHashMap<String, Watcher> watchers = new ConcurrentHashMap<>();

    private class LoadedLambdaSubscription implements LambdaSubscription {
        private final JsonNode jsonNode;

        LoadedLambdaSubscription(final JsonNode jsonNode) {
            this.jsonNode = jsonNode.deepCopy();
        }

        public String getTenant() { return jsonNode.path("tenant").asText(""); }

        public String getSubscriptionName() { return jsonNode.path("subscriptionName").asText(""); }

        public String getLambdaArn() { return jsonNode.path("lambdaArn").asText(""); }

        public String getCondition() { return jsonNode.path("condition").textValue(); }

        public Duration getClaimTtl() { return Duration.ofSeconds(jsonNode.path("claimTtl").intValue()); }

        public String getDelegateApiKeyHash() { return jsonNode.path("delegateApiKeyHash").asText(""); }

        public String getCypherTextDelegateApiKey() { return jsonNode.path("delegateApiKeyCypherText").textValue(); }

        public String getId() { return jsonNode.path("~id").asText(); }

        public Integer getBatchSize() { return jsonNode.path("batchSize").asInt(1); }

        public boolean isActive() { return jsonNode.path("active").asBoolean(); }

        @Override public long getVersion() { return jsonNode.path("~version").asLong(); }

        @Override public JsonNode asJson() {
            return jsonNode.deepCopy();
        }
    }



    @Inject
    public LambdaSubscriptionDAOImpl(final DataStoreClient dataStoreClient,
                                     final DataBusClient dataBusClient,
                                     final ApiKeyCrypto apiKeyCrypto,
                                     final EmoPollerConfiguration.EmoConfiguration emoConfiguration,
                                     final HealthCheckRegistry healthCheckRegistry) {
        this.dataStoreClient = dataStoreClient;
        this.dataBusClient = dataBusClient;
        this.subscriptionTable = emoConfiguration.getSubscriptionTable();
        this.subscriptionPrefix = emoConfiguration.getSubscriptionPrefix();
        this.managerApiKey = apiKeyCrypto.decryptCustom(emoConfiguration.getApiKey(), of("app", "emo-lambda-fanout"));

        {
            final AtomicReference<Date> lastMetadataPoll = new AtomicReference<>(null);


            metadataPoller = new AbstractScheduledService() {
                @Override protected String serviceName() { return "metadataPoller"; }

                private void innerRunOneIteration() {
                    dataBusClient.subscribe(
                        subscriptionPrefix + "-subscriptions",
                        Conditions.intrinsic(Intrinsic.TABLE, subscriptionTable),
                        Duration.ofDays(7), // we re-subscribe every time we poll (delay of 1 second), so this should be fine ;)
                        Duration.ofDays(1000), // ideally, events would never expire
                        managerApiKey);

                    final List<JsonNode> poll = dataBusClient.poll(
                        subscriptionPrefix + "-subscriptions",
                        Duration.ofMinutes(10),
                        999,
                        true,
                        managerApiKey);

                    poll.parallelStream()
                        .forEach(jsonNode -> {
                            final LambdaSubscription subscription = new LoadedLambdaSubscription(jsonNode.get("content"));
                            try {
                                subscriptions.put(subscription.getId(), subscription);
                            } catch (VersionConflictException e) {
                                // ignore
                            }
                            watchers.values().forEach((watcher) -> watcher.onUpdate(subscriptions.get(subscription.getId())));
                            dataBusClient.acknowledge(subscriptionPrefix + "-subscriptions", jsonNode.get("eventKey").asText(), managerApiKey);
                        });

                    LOG.info("Polling subscription metadata returned [{}] events", poll.size());
                    lastMetadataPoll.set(new Date());
                }

                @Override protected void runOneIteration() throws Exception {
                    try {
                        innerRunOneIteration();
                    } catch (Exception e) {
                        LOG.error("Uncaught exception polling metadata", e);
                    }
                }

                @Override protected Scheduler scheduler() {
                    return Scheduler.newFixedDelaySchedule(0, 1, TimeUnit.SECONDS);
                }
            };

            healthCheckRegistry.register("LambdaSubscriptionManager.metadataPoller", new HealthCheck() {
                @Override protected Result check() throws Exception {
                    final String lastPoll = String.format("Last poll: [%s]", String.valueOf(lastMetadataPoll.get()));
                    return metadataPoller.isRunning() ? Result.healthy(lastPoll) : Result.unhealthy("metadataPoller is not running. " + lastPoll);
                }
            });
        }
    }

    @Override public void registerWatcher(final String name, final LambdaSubscriptionDAO.Watcher watcher) {
        watchers.putIfAbsent(name, watcher);
        for (LambdaSubscription sub : subscriptions.values()) {
            watcher.onUpdate(sub);
        }
    }

    @Override
    public String saveAndNotifyWatchers(final LambdaSubscription lambdaSubscription) {
        final String id = getSubscriptionId(lambdaSubscription.getTenant(), lambdaSubscription.getLambdaArn());
        Preconditions.checkArgument(lambdaSubscription.getId() == null || lambdaSubscription.getId().equals(id));
        dataStoreClient.update(
            subscriptionTable,
            id,
            createDelta(lambdaSubscription),
            "comment:registering+lambda+for+subscription",
            ImmutableList.of(),
            managerApiKey
        );

        try {
            subscriptions.put(id, new LoadedLambdaSubscription(dataStoreClient.get(subscriptionTable, id, managerApiKey)));
        } catch (VersionConflictException e) {
            // ignore
        }

        watchers.values().forEach((watcher) -> watcher.onUpdate(subscriptions.get(id)));

        return id;
    }

    @Override public Set<LambdaSubscription> getAll(final String tenant) {
        return subscriptions.values().parallelStream().filter(n -> tenant.equals(n.getTenant())).collect(Collectors.toSet());
    }

    @Override public LambdaSubscription get(final String tenant, final String lambdaArn) {
        return subscriptions.get(getSubscriptionId(tenant, lambdaArn));
    }

    @Override public void start() throws Exception {
        dataStoreClient.updateTable(
            subscriptionTable,
            "placement:'app_global:default'",
            JsonUtil.obj(),
            "comment:Ensuring+table+exists",
            managerApiKey);

        dataBusClient.subscribe(subscriptionPrefix + "-subscriptions",
            Conditions.intrinsic(Intrinsic.TABLE, subscriptionTable),
            Duration.ofDays(7),
            Duration.ofDays(1000),
            managerApiKey);

        metadataPoller.startAsync().awaitRunning();

        StreamSupport.stream(DataStoreUtils.pscan(dataStoreClient, subscriptionTable, managerApiKey).spliterator(), true)
            .map(LoadedLambdaSubscription::new)
            .forEach(emoJson -> {
                try {
                    subscriptions.put(emoJson.getId(), emoJson);
                } catch (VersionConflictException e) {
                    // ignore
                }
                watchers.values().forEach((watcher) -> watcher.onUpdate(subscriptions.get(emoJson.getId())));
            });

        LOG.info("started");
    }

    @Override public void stop() throws Exception { }

    public void deactivate(final String id) {
        dataStoreClient.update(
            subscriptionTable,
            id,
            deactivateDelta(),
            "comment:deactivating+subscription",
            ImmutableList.of(),
            managerApiKey
        );

        try {
            subscriptions.put(id, new LoadedLambdaSubscription(dataStoreClient.get(subscriptionTable, id, managerApiKey)));
        } catch (VersionConflictException e) {
            // ignore
        }

        watchers.values().forEach((watcher) -> watcher.onUpdate(subscriptions.get(id)));
    }

    public void activate(final String id) {
        dataStoreClient.update(
            subscriptionTable,
            id,
            activateDelta(),
            "comment:activating+subscription",
            ImmutableList.of(),
            managerApiKey
        );

        try {
            subscriptions.put(id, new LoadedLambdaSubscription(dataStoreClient.get(subscriptionTable, id, managerApiKey)));
        } catch (VersionConflictException e) {
            // ignore
        }

        watchers.values().forEach((watcher) -> watcher.onUpdate(subscriptions.get(id)));
    }

    private String getSubscriptionId(final String tenant, final String lambdaArn) {
        return tenant + ":" + HashUtil.sha512hash_base16(lambdaArn);
    }

    private static Delta createDelta(final LambdaSubscription lambdaSubscription) {
        return Deltas.mapBuilder()
            .put("tenant", lambdaSubscription.getTenant())
            .put("subscriptionName", lambdaSubscription.getSubscriptionName())
            .put("lambdaArn", lambdaSubscription.getLambdaArn())
            .put("condition", lambdaSubscription.getCondition())
            .put("claimTtl", lambdaSubscription.getClaimTtl().getSeconds())
            .put("delegateApiKeyCypherText", lambdaSubscription.getCypherTextDelegateApiKey())
            .put("delegateApiKeyHash", HashUtil.argon2hash(lambdaSubscription.getDelegateApiKeyHash()))
            .put("active", lambdaSubscription.isActive())
            .put("batchSize", lambdaSubscription.getBatchSize())
            .build();
    }

    private static Delta deactivateDelta() {
        return Deltas.mapBuilder()
            .put("active", false)
            .build();
    }

    private static Delta activateDelta() {
        return Deltas.mapBuilder()
            .put("active", true)
            .build();
    }
}
