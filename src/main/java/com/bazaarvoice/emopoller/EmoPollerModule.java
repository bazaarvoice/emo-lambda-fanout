package com.bazaarvoice.emopoller;

import com.amazonaws.services.kms.AWSKMS;
import com.amazonaws.services.kms.AWSKMSClient;
import com.amazonaws.services.lambda.AWSLambda;
import com.amazonaws.services.lambda.AWSLambdaClient;
import com.bazaarvoice.emopoller.busplus.LambdaSubscriptionManager;
import com.bazaarvoice.emopoller.busplus.kms.ApiKeyCrypto;
import com.bazaarvoice.emopoller.busplus.lambda.AWSLambdaInvocationImpl;
import com.bazaarvoice.emopoller.busplus.lambda.LambdaInvocation;
import com.bazaarvoice.emopoller.busplus.lambda.LocalLambdaInvocationImpl;
import com.bazaarvoice.emopoller.emo.DataBusClient;
import com.bazaarvoice.emopoller.emo.DataStoreClient;
import com.bazaarvoice.emopoller.metrics.MetricRegistrar;
import com.bazaarvoice.emopoller.metrics.MetricsTelemetry;
import com.bazaarvoice.emopoller.resource.PollerResource;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.name.Names;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

class EmoPollerModule extends AbstractModule {
    private final EmoPollerConfiguration configuration;
    private final MetricRegistry metrics;
    private final String name;

    EmoPollerModule(final EmoPollerConfiguration configuration, final MetricRegistry metrics, final String name) {

        this.configuration = configuration;
        this.metrics = metrics;
        this.name = name;
    }

    @Override protected void configure() {
        binder().requireExplicitBindings(); // no magic!!

        bind(String.class).annotatedWith(Names.named("appName")).toInstance(name);

        bind(MetricRegistrar.class).toInstance(new MetricRegistrar(metrics));
        bind(EmoPollerConfiguration.class).toInstance(configuration);
        bind(EmoPollerConfiguration.EmoConfiguration.class).toInstance(configuration.getEmoConfiguration());
        bind(EmoPollerConfiguration.LambdaConfiguration.class).toInstance(configuration.getLambdaConfiguration());
        bind(JsonNode.class).annotatedWith(Names.named("apiKeyDigestWhitelist")).toInstance(configuration.getProxyConfiguration().getApiKeyDigestWhitelist());
        bind(String.class).annotatedWith(Names.named("cmk")).toInstance(configuration.getProxyConfiguration().getCMK());

        bind(PollerResource.class).asEagerSingleton();
        bind(DataBusClient.class).asEagerSingleton();

        bind(Client.class).toInstance(ClientBuilder.newClient());

        bind(LambdaSubscriptionManager.class).asEagerSingleton();
        bind(ApiKeyCrypto.class).asEagerSingleton();

        bind(DataBusClient.class).asEagerSingleton();
        bind(DataStoreClient.class).asEagerSingleton();

        if (configuration.getLambdaConfiguration().isLocal()) {
            bind(LambdaInvocation.class).to(LocalLambdaInvocationImpl.class);
        } else {
            bind(LambdaInvocation.class).to(AWSLambdaInvocationImpl.class);
        }


        final Map<String, String> tags = new HashMap<>();
        tags.put("application", "emo_lambda_fanout");

        bind(MetricsTelemetry.class).toInstance(new MetricsTelemetry(metrics, ImmutableMap.copyOf(tags), AbstractScheduledService.Scheduler.newFixedDelaySchedule(1, 1, TimeUnit.MINUTES)));
    }

    @Provides public AWSLambda awsLambda() {
        // the default provider chain will use your credentials locally and
        // the instance's role deployed
        return new AWSLambdaClient();
    }

    @Provides public AWSKMS awskms() {
        // the default provider chain will use your credentials locally and
        // the instance's role deployed
        return new AWSKMSClient();
    }
}
