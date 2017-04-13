package com.bazaarvoice.emopoller.busplus;

import com.bazaarvoice.emopoller.EmoPollerConfiguration;
import com.bazaarvoice.emopoller.busplus.kms.ApiKeyCrypto;
import com.bazaarvoice.emopoller.busplus.lambda.LambdaSubscriptionDAO;
import com.bazaarvoice.emopoller.busplus.lambda.model.LambdaSubscription;
import com.bazaarvoice.emopoller.emo.DataBusClient;
import com.bazaarvoice.emopoller.metrics.MetricRegistrar;
import com.codahale.metrics.Timer;
import com.codahale.metrics.health.HealthCheck;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.inject.Inject;
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
    private final Map<String, EmoPollerConfiguration.TenantConfiguration> tenantConfigurations;
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
        this.tenantConfigurations = pollerConfiguration.getTenantConfigurations();
        this.apiKeyCrypto = apiKeyCrypto;
        this.processPool = processPool;
        this.metricRegistrar = metricRegistrar;
        this.healthCheckRegistry = healthCheckRegistry;
        this.client = client;
    }

    SubscriptionPoller produce(final String tenant, final String lambdaArn) {
        Preconditions.checkArgument(tenantConfigurations.containsKey(tenant), "Unknown tenant: [" + tenant + "] for [" + lambdaArn + "]");
        return new SubscriptionPoller(tenant, lambdaArn);
    }

    class SubscriptionPoller extends AbstractScheduledService {
        private final Logger LOG;
        private final AtomicReference<Date> lastPoll;

        private final String tenant;
        private final String lambdaArn;
        private final DataBusClient dataBusClient;

        private SubscriptionPoller(final String tenant, final String lambdaArn) {
            this.tenant = tenant;
            this.lambdaArn = lambdaArn;
            this.dataBusClient = new DataBusClient(client, tenantConfigurations.get(tenant).getTenantEmoConfiguration().getBaseURL(), metricRegistrar);

            LOG = LoggerFactory.getLogger("SubscriptionPoller-" + tenant + "-" + lambdaArn);
            lastPoll = new AtomicReference<>(null);
            healthCheckRegistry.register("LambdaSubscriptionManager.poller." + tenant + lambdaArn, new HealthCheck() {
                @Override protected Result check() throws Exception {
                    final String lastPoll = String.format("Last poll: [%s]", String.valueOf(SubscriptionPoller.this.lastPoll.get()));
                    return isRunning() ? Result.healthy(lastPoll) : Result.unhealthy("poller is not running. " + lastPoll);
                }
            });
        }

        void ensureSubscribed() {
            final LambdaSubscription lambdaSubscription = lambdaSubscriptionDAO.get(tenant, lambdaArn);
            final String delegateApiKey = apiKeyCrypto.decrypt(lambdaSubscription.getCypherTextDelegateApiKey(), lambdaSubscription.getSubscriptionName(), lambdaSubscription.getLambdaArn());
            dataBusClient.subscribe(
                lambdaSubscription.getSubscriptionName(),
                lambdaSubscription.getCondition(),
                SUBSCRIPTION_TTL,
                EVENT_TTL,
                delegateApiKey);
        }

        Integer size() {
            final LambdaSubscription lambdaSubscription = lambdaSubscriptionDAO.get(tenant, lambdaArn);
            final String delegateApiKey = apiKeyCrypto.decrypt(lambdaSubscription.getCypherTextDelegateApiKey(), lambdaSubscription.getSubscriptionName(), lambdaSubscription.getLambdaArn());
            return dataBusClient.size(lambdaSubscription.getSubscriptionName(), 1000, delegateApiKey);
        }

        @Override protected String serviceName() { return "poller-" + tenant + lambdaArn.replace(':', '-'); }

        @Override protected Scheduler scheduler() { return Scheduler.newFixedDelaySchedule(0, 100, TimeUnit.MILLISECONDS); }

        @Override protected void runOneIteration() throws Exception {
            final LambdaSubscription lambdaSubscription = lambdaSubscriptionDAO.get(tenant, lambdaArn);

            // first deal with active/inactive state business

            final String gaugeName = "emo_lambda_fanout.subscription.size";
            final ImmutableMap<String, String> gaugeTags = ImmutableMap.of(
                "lambda_arn", lambdaSubscription.getLambdaArn().replaceAll("[:]", "_"),
                "tenant", lambdaSubscription.getTenant()
            );

            if (!lambdaSubscription.isActive()) {
                metricRegistrar.removeGauge(gaugeName, gaugeTags);

                Thread.sleep(30_000L); // if not active, wait 30s before polling again.
                return;
            }

            // subscription is active. Proceed to poll...

            final String delegateApiKey = apiKeyCrypto.decrypt(lambdaSubscription.getCypherTextDelegateApiKey(), lambdaSubscription.getSubscriptionName(), lambdaSubscription.getLambdaArn());

            metricRegistrar.register(
                gaugeName, gaugeTags,
                () -> dataBusClient.size(lambdaSubscription.getSubscriptionName(), 1000, delegateApiKey)
            );

            // keep subscription alive
            dataBusClient.subscribe(
                lambdaSubscription.getSubscriptionName(),
                lambdaSubscription.getCondition(),
                SUBSCRIPTION_TTL,
                EVENT_TTL,
                delegateApiKey);

            List<JsonNode> poll = dataBusClient.poll(
                lambdaSubscription.getSubscriptionName(),
                lambdaSubscription.getClaimTtl(),
                lambdaConfiguration.getPollSize(),
                true,
                delegateApiKey);

            metricRegistrar
                .histogram("emo_lambda_fanout.poll.events", ImmutableMap.of("lambda_arn", lambdaSubscription.getLambdaArn().replaceAll("[:]", "_")))
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

        void start() { startAsync().awaitRunning(); }

        void stop() { stopAsync().awaitTerminated(); }
    }
}

