package com.bazaarvoice.emopoller.resource;

import com.bazaarvoice.emodb.sor.api.Audit;
import com.bazaarvoice.emodb.sor.api.TableOptions;
import com.bazaarvoice.emodb.sor.condition.Condition;
import com.bazaarvoice.emodb.sor.delta.Delta;
import com.bazaarvoice.emopoller.EmoPollerConfiguration;
import com.bazaarvoice.emopoller.busplus.LambdaSubscriptionManager;
import com.bazaarvoice.emopoller.busplus.kms.ApiKeyCrypto;
import com.bazaarvoice.emopoller.busplus.lambda.NoSuchFunctionException;
import com.bazaarvoice.emopoller.emo.DataBusClient;
import com.bazaarvoice.emopoller.emo.DataStoreClient;
import com.bazaarvoice.emopoller.util.JsonUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import io.dropwizard.jersey.params.IntParam;
import org.apache.commons.lang3.NotImplementedException;
import org.eclipse.jetty.util.ConcurrentHashSet;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import javax.annotation.Nullable;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Cookie;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class PollerResourceAuthTest {
    private static final Object HEADER_PLACE = new Object();
    private final String key = "super_secret_key";
    private final JsonNode whitelist = JsonUtil.obj(
        "a key", "$argon2i$v=19$m=65536,t=2,p=1$NeVZcgmZFI7O184yVOwzYw$+L/Six0dljczOPOC6Rd119QB1kDvKyivQB18pWvXAPA"
    );
    private final ApiKeyCrypto apiKeyCrypto = new ApiKeyCrypto(null, null, null) {
        @Override public String decryptCustom(final String cypherTextKey, final ImmutableMap<String, String> encryptionContext) {
            return "dummy";
        }
    };
    private final PollerResource pollerResource = new PollerResource(new TestDatastoreClient(), new TestDataBusClient(), new TestLambdaSubscriptionManager(apiKeyCrypto), whitelist);

    private final Set<Method> testedMethods = new ConcurrentHashSet<>();

    @AfterClass
    public void testCoverage() {
        final Set<Method> allMethods = Arrays.stream(pollerResource.getClass().getDeclaredMethods()).filter((method) -> Modifier.isPublic(method.getModifiers())).collect(Collectors.toSet());
        final Set<Method> missingTests = Sets.difference(allMethods, testedMethods);
        Assert.assertEquals(missingTests, Collections.emptySet());
    }

    @Test
    public void testAuthKeyRequiredSubscribe() throws NoSuchMethodException {
        makeAssertions(
            key,
            PollerResource.class.getDeclaredMethod("subscribeLambda", String.class, IntParam.class, IntParam.class, HttpHeaders.class, String.class),
            new Object[] { "", new IntParam("1"), new IntParam("1"), HEADER_PLACE, "alwaysTrue()"}
        );
    }

    @Test
    public void testActivateLambdaSubscription() throws NoSuchMethodException {
        makeAssertions(
            key,
            PollerResource.class.getDeclaredMethod("activateLambdaSubscription", String.class, HttpHeaders.class),
            new Object[] { "asub", HEADER_PLACE }
        );
    }

    @Test
    public void testDeactivateLambdaSubscription() throws NoSuchMethodException {
        makeAssertions(
            key,
            PollerResource.class.getDeclaredMethod("deactivateLambdaSubscription", String.class, HttpHeaders.class),
            new Object[] { "asub", HEADER_PLACE }
        );
    }

    @Test
    public void testGetLambdaSubscription() throws NoSuchMethodException {
        makeAssertions(
            key,
            PollerResource.class.getDeclaredMethod("getLambdaSubscription", String.class, HttpHeaders.class),
            new Object[] { "asub", HEADER_PLACE }
        );
    }

    @Test
    public void testGetLambdaSubscriptionSize() throws NoSuchMethodException {
        makeAssertions(
            key,
            PollerResource.class.getDeclaredMethod("getLambdaSubscriptionSize", String.class, HttpHeaders.class),
            new Object[] { "asub", HEADER_PLACE }
        );
    }


    private void makeAssertions(final String key, final Method toTest, final Object[] arguments) {
        final int headerIndex = getHeaderIndex(arguments);

        makeAssertions(key, (headers) -> {
            arguments[headerIndex] = headers;
            try {
                toTest.invoke(pollerResource, arguments);
            } catch (IllegalAccessException e) {
                fail("structural problem", e);
            } catch (InvocationTargetException e) {
                final Throwable cause = e.getCause();
                if (cause instanceof RuntimeException) {
                    throw (RuntimeException) cause;
                } else {
                    fail("method threw exception", cause);
                }
            }
            return null;
        });
        testedMethods.add(toTest);
    }

    private int getHeaderIndex(final Object[] arguments) {
        for (int i = 0; i < arguments.length; i++) {
            if (arguments[i] == HEADER_PLACE) {
                return i;
            }
        }
        throw new NoSuchElementException();
    }

    private void makeAssertions(final String key, final Function<HttpHeaders, Void> call) {// we can pass a key
        try {
            call.apply(new TestHttpAuthHeaders("X-BV-API-Key", key));
            fail("The test framework should always throw exceptions, even to pass success messages back.");
        } catch (GotKey g) {
            assertEquals(g.key, key);
        }

        // we must pass a key
        try {
            call.apply(new TestHttpAuthHeaders());
            fail("The test framework should always throw exceptions, even to pass success messages back.");
        } catch (WebApplicationException e) {
            assertTrue(e.getMessage().contains("You must specify exactly one api key as a header"));
        }

        // we must pass a whitelisted key
        final String randoKey = UUID.randomUUID().toString();
        try {
            call.apply(new TestHttpAuthHeaders("X-BV-API-Key", randoKey));
            fail("The test framework should always throw exceptions, even to pass success messages back.");
        } catch (GotKey g) {
            fail(String.format("[%s] was not whitelisted, so the poller should not have permitted it.", randoKey));
        } catch (WebApplicationException e) {
            assertTrue(e.getMessage().contains("Unknown api key"));
        }
    }

    private class TestHttpAuthHeaders implements HttpHeaders {
        private final ImmutableMap<String, ImmutableList<String>> headers;

        TestHttpAuthHeaders(final String header, final String value) {
            this.headers = ImmutableMap.of(header, ImmutableList.of(value));
        }

        TestHttpAuthHeaders() {
            this.headers = ImmutableMap.of();
        }

        @Override public List<String> getRequestHeader(final String name) {
            return headers.get(name);
        }

        @Override public String getHeaderString(final String name) {
            throw new NotImplementedException("not implemented");
        }

        @Override public MultivaluedMap<String, String> getRequestHeaders() {
            throw new NotImplementedException("not implemented");
        }

        @Override public List<MediaType> getAcceptableMediaTypes() {
            throw new NotImplementedException("not implemented");
        }

        @Override public List<Locale> getAcceptableLanguages() {
            throw new NotImplementedException("not implemented");
        }

        @Override public MediaType getMediaType() {
            throw new NotImplementedException("not implemented");
        }

        @Override public Locale getLanguage() {
            throw new NotImplementedException("not implemented");
        }

        @Override public Map<String, Cookie> getCookies() {
            throw new NotImplementedException("not implemented");
        }

        @Override public Date getDate() {
            throw new NotImplementedException("not implemented");
        }

        @Override public int getLength() {
            throw new NotImplementedException("not implemented");
        }
    }

    private class GotKey extends RuntimeException {
        private final String key;

        GotKey(final String key) {
            this.key = key;
        }
    }

    private class TestDatastoreClient extends DataStoreClient {
        TestDatastoreClient() {
            super(null, new EmoPollerConfiguration.EmoConfiguration(null, ""));
        }

        @Override public void updateTable(final String table, final TableOptions options, final JsonNode template, final Audit audit, final String apiKey) {
            throw new GotKey(apiKey);
        }

        @Override public JsonNode updateTable(final String table, final String options, final JsonNode template, final String audit, final String apiKey) {
            throw new GotKey(apiKey);
        }

        @Override public JsonNode get(final String table, final String key, final String apiKey) {
            throw new GotKey(apiKey);
        }

        @Override public JsonNode delete(final String table, final String key, final String audit, final List<String> tags, final String apiKey) {
            throw new GotKey(apiKey);
        }

        @Override public JsonNode update(final String table, final String key, final Delta delta, final Audit audit, final List<String> tags, final String apiKey) {
            throw new GotKey(apiKey);
        }

        @Override public JsonNode update(final String table, final String key, final Delta delta, final String audit, final List<String> tags, final String apiKey) {
            throw new GotKey(apiKey);
        }

        @Override public Collection<String> getSplits(final String table, final int desiredRecordsPerSplit, final String apiKey) {
            throw new GotKey(apiKey);
        }

        @Override public Collection<JsonNode> getSplit(final String table, final String split, @Nullable final String fromKeyExclusive, final long limit, final String apiKey) {
            throw new GotKey(apiKey);
        }

        @Override public Collection<JsonNode> scan(final String table, final String from, final Long limit, final String apiKey) {
            throw new GotKey(apiKey);
        }

        @Override public Collection<JsonNode> listTables(final String from, final Long limit, final String apiKey) {
            throw new GotKey(apiKey);
        }
    }

    private class TestDataBusClient extends DataBusClient {

        public TestDataBusClient() {
            super(null, new EmoPollerConfiguration.EmoConfiguration(null, ""), null);
        }

        @Override public JsonNode subscribe(final String subscriptionName, final Condition condition, final Duration subscriptionTTL, final Duration eventTTL, final String apiKey) {
            throw new GotKey(apiKey);
        }

        @Override public JsonNode subscribe(final String subscriptionName, final String condition, final Duration subscriptionTTL, final Duration eventTTL, final String apiKey) {
            throw new GotKey(apiKey);
        }

        @Override public JsonNode getSubscription(final String subscriptionName, final String apiKey) {
            throw new GotKey(apiKey);
        }

        @Override public void unsubscribe(final String subscriptionName, final String apiKey) {
            throw new GotKey(apiKey);
        }

        @Override public Integer size(final String subscriptionName, final int limit, final String apiKey) {
            throw new GotKey(apiKey);
        }

        @Override public List<JsonNode> poll(final String subscriptionName, final Duration claimTtl, final int limit, final String apiKey) {
            throw new GotKey(apiKey);
        }

        @Override public List<JsonNode> peek(final String subscriptionName, final int limit, final String apiKey) {
            throw new GotKey(apiKey);
        }

        @Override public void acknowledge(final String subscriptionName, final String eventKey, final String apiKey) {
            throw new GotKey(apiKey);
        }

        @Override public void acknowledge(final String subscriptionName, final List<String> eventKeys, final String apiKey) {
            throw new GotKey(apiKey);
        }
    }

    private class TestLambdaSubscriptionManager extends LambdaSubscriptionManager {
        TestLambdaSubscriptionManager(final ApiKeyCrypto apiKeyCrypto) {
            super(null, null, new EmoPollerConfiguration.EmoConfiguration("", ""), null, null, apiKeyCrypto, null);
        }

        @Override public void register(final String lambdaArn, final Condition condition, final Duration claimTtl, final Integer batchSize, final String delegateApiKey)
            throws NoSuchFunctionException {
            throw new GotKey(delegateApiKey);
        }

        @Override public JsonNode get(final String lambdaArn, final String delegateApiKey) {
            throw new GotKey(delegateApiKey);
        }
    }
}
