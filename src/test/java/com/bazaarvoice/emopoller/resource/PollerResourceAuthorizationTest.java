package com.bazaarvoice.emopoller.resource;

import com.bazaarvoice.emopoller.EmoPollerConfiguration;
import com.bazaarvoice.emopoller.busplus.LambdaSubscriptionManager;
import com.bazaarvoice.emopoller.busplus.lambda.LambdaSubscriptionDAO;
import com.bazaarvoice.emopoller.busplus.lambda.model.LambdaSubscription;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import javax.ws.rs.core.Application;
import javax.ws.rs.core.Response;
import java.util.Set;

public class PollerResourceAuthorizationTest extends JerseyTest {

    @BeforeTest
    public void before() throws Exception {
        super.setUp();
    }

    @AfterTest
    public void after() throws Exception {
        super.tearDown();
    }

    @Override protected Application configure() {
        final ResourceConfig resourceConfig = new ResourceConfig();
        final AuthorizationRequestFilter authorizationRequestFilter = new AuthorizationRequestFilter(ImmutableMap.of(
            "testenv", new EmoPollerConfiguration.EnvironmentConfiguration(ImmutableMap.of(
                "testkey", "$argon2i$v=19$m=4096,t=3,p=1$bVNLL1Q5Qy9paHBMVklIazZQN0V5dGh2eUJ3ckR2cUd3YVREVDhGMVJ5VWtISlNZRmZ6aHFaSnpTTHdyRVFkV3Y0MC9SZFJxQTc3WWtRQld5VVlRRHhtbjQ3eGtEZmVNQ0U5NnBCUW9nYjVFNHNiWGlFcEgrRDA2QTJkbHBYSGsyWmhHWjBnN0ZNR1ZJMmZzeXh1ZlpjNE45SzJoNWNrVnlrMEpRMEVSeGY0PQ$3xuzz1Kq+U0ZdvQjAdzD/Q2qRc3MaYLXbpcbLlW6KzQ"
            ), null)
        ));
        resourceConfig.register(authorizationRequestFilter);

        final EmoPollerConfiguration.EmoConfiguration emoConfiguration = new EmoPollerConfiguration.EmoConfiguration("", "", "", "");
        final LambdaSubscriptionDAO lambdaSubscriptionDAO = new LambdaSubscriptionDAO() {

            @Override public void start() throws Exception { }

            @Override public void stop() throws Exception { }

            @Override public void registerWatcher(final String name, final Watcher watcher) {

            }

            @Override public String saveAndNotifyWatchers(final LambdaSubscription lambdaSubscription) {
                return null;
            }

            @Override public Set<LambdaSubscription> getAll(final String environment) {
                return ImmutableSet.of();
            }

            @Override public LambdaSubscription get(final String environment, final String lambdaArn) {
                return null;
            }

            @Override public void deactivate(final String id) {

            }

            @Override public void activate(final String id) {

            }
        };
        final LambdaSubscriptionManager lambdaSubscriptionManager = new LambdaSubscriptionManager(emoConfiguration, null, lambdaSubscriptionDAO, null, null, null);
        final PollerResource pollerResource = new PollerResource(lambdaSubscriptionManager);
        resourceConfig.register(pollerResource);
        return resourceConfig;
    }

    @Test public void testRequiredApiKey() {
        final Response response = target("/pants/poller").request().get();
        Assert.assertEquals(response.getStatus(), 401);
        Assert.assertEquals(response.readEntity(String.class), "You must specify exactly one api key as a header [X-BV-API-Key].");
    }

    @Test public void testRequiredEnvironment() {
        final Response response = target("/pants/poller").request()
            .header("X-BV-API-Key", "missing key")
            .get();
        Assert.assertEquals(response.getStatus(), 401);
        Assert.assertEquals(response.readEntity(String.class), "Unknown environment. Contact the poller administrators for help.");
    }

    @Test public void testInvalidKey() {
        final Response response = target("/testenv/poller").request()
            .header("X-BV-API-Key", "invalid key")
            .get();
        Assert.assertEquals(response.getStatus(), 401);
        Assert.assertEquals(response.readEntity(String.class), "Unknown api key. Contact the poller administrators for help.");
    }

    @Test public void testValidKey() {
        final Response response = target("/testenv/poller").request()
            .header("X-BV-API-Key", "mytestkey")
            .get();
        Assert.assertEquals(response.getStatus(), 200);
        Assert.assertEquals(response.readEntity(Set.class), ImmutableSet.of());
    }
}
