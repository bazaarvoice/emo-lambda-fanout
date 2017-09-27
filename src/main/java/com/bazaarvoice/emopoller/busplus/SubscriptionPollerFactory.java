package com.bazaarvoice.emopoller.busplus;

import com.bazaarvoice.emodb.sor.condition.Condition;
import com.bazaarvoice.emodb.sor.condition.eval.ConditionEvaluator;
import com.bazaarvoice.emodb.sor.delta.deser.DeltaParser;
import com.bazaarvoice.emopoller.EmoPollerConfiguration;
import com.bazaarvoice.emopoller.busplus.kms.ApiKeyCrypto;
import com.bazaarvoice.emopoller.busplus.lambda.LambdaSubscriptionDAO;
import com.bazaarvoice.emopoller.busplus.lambda.model.LambdaSubscription;
import com.bazaarvoice.emopoller.emo.DataBusClient;
import com.bazaarvoice.emopoller.metrics.MetricRegistrar;
import com.bazaarvoice.emopoller.util.JsonUtil;
import com.codahale.metrics.Timer;
import com.codahale.metrics.health.HealthCheck;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.inject.Inject;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.Client;
import java.time.Duration;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class SubscriptionPollerFactory {
    private static final Duration SUBSCRIPTION_TTL = Duration.ofDays(7);
    private static final Duration EVENT_TTL = Duration.ofDays(1000);


    private final LambdaSubscriptionDAO lambdaSubscriptionDAO;
    private final EmoPollerConfiguration.LambdaConfiguration lambdaConfiguration;
    private final ApiKeyCrypto apiKeyCrypto;
    private final ProcessPool processPool;
    private final MetricRegistrar metricRegistrar;
    private final HealthCheckRegistry healthCheckRegistry;
    private final Map<String, EmoPollerConfiguration.EnvironmentConfiguration> environmentConfigurations;
    private final Client client;

    @Inject
    public SubscriptionPollerFactory(final LambdaSubscriptionDAO lambdaSubscriptionDAO,
                                     final EmoPollerConfiguration pollerConfiguration,
                                     final Client client,
                                     final ApiKeyCrypto apiKeyCrypto,
                                     final ProcessPool processPool,
                                     final MetricRegistrar metricRegistrar,
                                     final HealthCheckRegistry healthCheckRegistry) {
        this.lambdaSubscriptionDAO = lambdaSubscriptionDAO;
        this.lambdaConfiguration = pollerConfiguration.getLambdaConfiguration();
        this.environmentConfigurations = pollerConfiguration.getEnvironmentConfigurations();
        this.apiKeyCrypto = apiKeyCrypto;
        this.processPool = processPool;
        this.metricRegistrar = metricRegistrar;
        this.healthCheckRegistry = healthCheckRegistry;
        this.client = client;
    }

    SubscriptionPoller produce(final String environment, final String lambdaArn) {
        Preconditions.checkArgument(environmentConfigurations.containsKey(environment), "Unknown environment: [" + environment + "] for [" + lambdaArn + "]");
        return new SubscriptionPoller(environment, lambdaArn);
    }

    class SubscriptionPoller extends AbstractScheduledService {
        private final Logger LOG;
        private final AtomicReference<Date> lastPoll;
        private final AtomicReference<DateTime> lastSubscribe;


        private final String environment;
        private final String lambdaArn;
        private final DataBusClient dataBusClient;

        private SubscriptionPoller(final String environment, final String lambdaArn) {
            this.environment = environment;
            this.lambdaArn = lambdaArn;
            this.dataBusClient = new DataBusClient(client, environmentConfigurations.get(environment).getEnvironmentEmoConfiguration().getBaseURL(), metricRegistrar);

            LOG = LoggerFactory.getLogger("SubscriptionPoller-" + environment + "-" + lambdaArn);
            lastPoll = new AtomicReference<>(null);
            lastSubscribe = new AtomicReference<>(null);
            healthCheckRegistry.register("LambdaSubscriptionManager.poller." + environment + "-" + lambdaArn, new HealthCheck() {
                @Override protected Result check() throws Exception {
                    final String lastPoll = String.format("Last poll: [%s]", String.valueOf(SubscriptionPoller.this.lastPoll.get()));
                    final String lastSubscribe = String.format("Last subscribe: [%s]", String.valueOf(SubscriptionPoller.this.lastSubscribe.get()));
                    return isRunning() ? Result.healthy(lastPoll) : Result.unhealthy("poller is not running. " + lastPoll + " " + lastSubscribe);
                }
            });
        }

        void ensureSubscribed() {
            final LambdaSubscription lambdaSubscription = lambdaSubscriptionDAO.get(environment, lambdaArn);
            final String delegateApiKey = apiKeyCrypto.decrypt(lambdaSubscription.getCypherTextDelegateApiKey(), lambdaSubscription.getSubscriptionName(), lambdaSubscription.getLambdaArn());
            dataBusClient.subscribe(
                lambdaSubscription.getSubscriptionName(),
                lambdaSubscription.getCondition(),
                SUBSCRIPTION_TTL,
                EVENT_TTL,
                delegateApiKey);
            lastSubscribe.set(new DateTime());
        }

        Integer size() {
            final LambdaSubscription lambdaSubscription = lambdaSubscriptionDAO.get(environment, lambdaArn);
            final String delegateApiKey = apiKeyCrypto.decrypt(lambdaSubscription.getCypherTextDelegateApiKey(), lambdaSubscription.getSubscriptionName(), lambdaSubscription.getLambdaArn());
            return dataBusClient.size(lambdaSubscription.getSubscriptionName(), 1000, delegateApiKey);
        }

        @Override protected String serviceName() { return "poller-" + environment + lambdaArn.replace(':', '-'); }

        @Override protected Scheduler scheduler() { return Scheduler.newFixedDelaySchedule(0, 100, TimeUnit.MILLISECONDS); }

        private void innerRunOneIteration() throws Exception {
            final LambdaSubscription lambdaSubscription = lambdaSubscriptionDAO.get(environment, lambdaArn);

            // first deal with active/inactive state business

            final String gaugeName = "emo_lambda_fanout.subscription.size";
            final ImmutableMap<String, String> tags = ImmutableMap.of(
                "lambda_arn", lambdaSubscription.getLambdaArn().replaceAll("[:]", "_"),
                "environment", lambdaSubscription.getEnvironment()
            );

            if (!lambdaSubscription.isActive()) {
                metricRegistrar.removeGauge(gaugeName, tags);

                Thread.sleep(30_000L); // if not active, wait 30s before polling again.
                return;
            }

            // subscription is active. Proceed to poll...

            final String delegateApiKey = apiKeyCrypto.decrypt(lambdaSubscription.getCypherTextDelegateApiKey(), lambdaSubscription.getSubscriptionName(), lambdaSubscription.getLambdaArn());

            metricRegistrar.register(
                gaugeName, tags,
                () -> {
                    try {
                        return dataBusClient.size(lambdaSubscription.getSubscriptionName(), 1000, delegateApiKey);
                    } catch (Exception e) {
                        LOG.error(String.format("Error in gauge for [%s] [%s]", lambdaSubscription.getSubscriptionName(), lambdaSubscription.getLambdaArn()), e);
                        return null;
                    }
                }
            );

            // we'll keep the subscription around for at least a day and renew at most hourly
            // if the subscription fails, we bomb out and try again in 100ms.
            final DateTime lastSubscription = lastSubscribe.get();
            if (SUBSCRIPTION_TTL.getSeconds() < 60 * 60 * 24) {
                throw new IllegalArgumentException("subscription ttl must be greater than one day. Was " + SUBSCRIPTION_TTL);
            }
            if (lastSubscription == null || lastSubscription.isBefore(new DateTime().minusHours(1))) {
                // keep subscription alive
                dataBusClient.subscribe(
                    lambdaSubscription.getSubscriptionName(),
                    lambdaSubscription.getCondition(),
                    SUBSCRIPTION_TTL,
                    EVENT_TTL,
                    delegateApiKey);
                lastSubscribe.set(new DateTime());
            }

            final List<JsonNode> superSetPoll = dataBusClient.poll(
                lambdaSubscription.getSubscriptionName(),
                lambdaSubscription.getClaimTtl(),
                lambdaConfiguration.getPollSize(),
                true,
                delegateApiKey);

            final List<JsonNode> poll;
            if (lambdaSubscription.getDocCondition() == null) {
                poll = superSetPoll;
            } else {
                final Condition docCondition = DeltaParser.parseCondition(lambdaSubscription.getDocCondition());
                final ImmutableList.Builder<JsonNode> toPoll = ImmutableList.builder();
                final ImmutableList.Builder<String> toAck = ImmutableList.builder();
                for (JsonNode node : superSetPoll) {
                    if (ConditionEvaluator.eval(docCondition, JsonUtil.mapper().convertValue(node.get("content"), Object.class), null)) {
                        toPoll.add(node);
                    } else {
                        toAck.add(node.get("eventKey").asText());
                    }
                }
                final ImmutableList<String> acks = toAck.build();
                if (!acks.isEmpty()) {
                    final Timer.Context time = metricRegistrar.timer("emo_lambda_fanout.poll.filteredAckTime", tags).time();
                    dataBusClient.acknowledge(lambdaSubscription.getSubscriptionName(), acks, delegateApiKey);
                    time.stop();
                }
                metricRegistrar.counter("emo_lambda_fanout.poll.filtered", tags).inc(acks.size());
                poll = toPoll.build();
            }

            metricRegistrar.counter("emo_lambda_fanout.poll.polled", tags).inc(poll.size());

            metricRegistrar
                .histogram("emo_lambda_fanout.poll.events", tags)
                .update(poll.size());

            LOG.info("Polled [{}] events for arn[{}]", poll.size(), lambdaSubscription.getLambdaArn());

            for (List<JsonNode> batch : Lists.partition(poll, lambdaSubscription.getBatchSize())) {
                final List<JsonNode> content = batch.stream().map(n -> n.get("content")).collect(Collectors.toList());
                final List<String> eventKeys = batch.stream().map(n -> n.get("eventKey").asText()).collect(Collectors.toList());

                processPool.submit(
                    lambdaSubscription,
                    content,
                    () -> {
                        final Timer.Context ackTimer = metricRegistrar.timer("emo_lambda_fanout.execution.ackTime", ImmutableMap.of()).time();
                        dataBusClient.acknowledge(lambdaSubscription.getSubscriptionName(), eventKeys, delegateApiKey);
                        ackTimer.stop();
                    }
                );
            }

            lastPoll.set(new Date());
        }

        @Override protected void runOneIteration() throws Exception {
            try {
                innerRunOneIteration();
            } catch (Exception e) {
                LOG.error("Uncaught exception in runOneIteration.", e);
            }
        }

        void start() { startAsync().awaitRunning(); }

        void stop() { stopAsync().awaitTerminated(); }
    }
}


