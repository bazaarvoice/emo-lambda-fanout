package com.bazaarvoice.emopoller.emo;

import com.bazaarvoice.emodb.sor.condition.Condition;
import com.bazaarvoice.emopoller.EmoPollerConfiguration;
import com.bazaarvoice.emopoller.metrics.MetricRegistrar;
import com.bazaarvoice.emopoller.util.JsonUtil;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.time.Duration;
import java.util.List;

import static com.bazaarvoice.emopoller.emo.EmoUtils.consume;
import static com.bazaarvoice.emopoller.emo.EmoUtils.responseOrThrow;

public class DataBusClient {
    private final Client client;
    private final String baseURL;
    private final MetricRegistrar metricRegistrar;

    private static final String APIKEY_AUTH_HEADER = "X-BV-API-Key";
    private static final MediaType APPLICATION_X_JSON_CONDITION_TYPE = new MediaType("application", "x.json-condition");
    private static final Logger LOG = LoggerFactory.getLogger(DataBusClient.class);

    @Inject
    public DataBusClient(final Client client,
                         final EmoPollerConfiguration.EmoConfiguration emoConfiguration,
                         final MetricRegistrar metricRegistrar) {
        this.client = client;
        this.baseURL = emoConfiguration.getBaseURL();
        this.metricRegistrar = metricRegistrar;
    }

    public DataBusClient(final Client client,
                         final String baseURL,
                         final MetricRegistrar metricRegistrar) {
        this.client = client;
        this.baseURL = baseURL;
        this.metricRegistrar = metricRegistrar;
    }

    @SuppressWarnings("UnusedReturnValue") public JsonNode subscribe(final String subscriptionName,
                                                                     final Condition condition,
                                                                     final Duration subscriptionTTL,
                                                                     final Duration eventTTL,
                                                                     final String apiKey) {
        return subscribe(subscriptionName, condition.toString(), subscriptionTTL, eventTTL, apiKey);
    }

    public JsonNode subscribe(final String subscriptionName,
                              final String condition,
                              final Duration subscriptionTTL,
                              final Duration eventTTL,
                              final String apiKey) {
        final Timer.Context time = metricRegistrar.timer("emo_lambda_fanout.databus.subscribe.time", ImmutableMap.of()).time();
        final URI uri = UriBuilder.fromUri(baseURL)
            .segment("bus", "1", subscriptionName)
            .queryParam("ttl", subscriptionTTL.getSeconds())
            .queryParam("eventTtl", eventTTL.getSeconds())
            .queryParam("partitioned", false)
            .build();

        final Response response = responseOrThrow(client
                .target(uri)
                .request()
                .accept(MediaType.APPLICATION_JSON_TYPE)
                .header(APIKEY_AUTH_HEADER, apiKey)
                .buildPut(Entity.entity(condition, APPLICATION_X_JSON_CONDITION_TYPE))
                .invoke(),
            (r, e) -> {
                final String context = String.format("dataBus(%s).subscribe(%s, %s, %s, %s)", uri, subscriptionName, condition, subscriptionTTL, eventTTL);
                LOG.warn("[{}] Unexpected response status [{}] from [{}]: {}. May retry...", context, r.getStatus(), r.getLocation(), e);
                time.stop();
            }
        );

        try (InputStream is = response.readEntity(InputStream.class)) {
            return JsonUtil.mapper().readTree(is);
        } catch (IOException e) {
            throw new RuntimeException(String.format("Unexpected error getting size [%s]", subscriptionName), e);
        } finally {
            time.stop();
        }
    }

    public Integer size(final String subscriptionName,
                        final int limit,
                        final String apiKey) {
        final URI uri = UriBuilder.fromUri(baseURL)
            .segment("bus", "1", subscriptionName, "size")
            .queryParam("limit", limit)
            .queryParam("includeTags", true)
            .queryParam("partitioned", false)
            .build();

        final Response response = responseOrThrow(client
                .target(uri)
                .request()
                .accept(MediaType.APPLICATION_JSON_TYPE)
                .header(APIKEY_AUTH_HEADER, apiKey)
                .buildGet()
                .invoke(),
            (r, e) -> {
                final String context = String.format("dataBus(%s).size(%s, %s)", uri, subscriptionName, limit);
                LOG.warn("[{}] Unexpected response status [{}] from [{}]: {}. May retry...", context, r.getStatus(), r.getLocation(), e);
            }
        );

        try (InputStream is = response.readEntity(InputStream.class)) {
            final JsonNode jsonNode = JsonUtil.mapper().readTree(is);
            return jsonNode.asInt();
        } catch (IOException e) {
            throw new RuntimeException(String.format("Unexpected error getting size [%s]", subscriptionName), e);
        }
    }

    public List<JsonNode> poll(final String subscriptionName,
                               final Duration claimTtl,
                               final int limit,
                               final boolean longpoll,
                               final String apiKey) {
        final Timer.Context time = metricRegistrar.timer("emo_lambda_fanout.databus.poll.time", ImmutableMap.of()).time();
        final URI uri = UriBuilder.fromUri(baseURL)
            .segment("bus", "1", subscriptionName, "poll")
            .queryParam("ttl", claimTtl.getSeconds())
            .queryParam("limit", limit)
            .queryParam("includeTags", true)
            .queryParam("partitioned", false)
            .queryParam("ignoreLongPoll", !longpoll)
            .build();

        final Response response = responseOrThrow(client
                .target(uri)
                .request()
                .accept(MediaType.APPLICATION_JSON_TYPE)
                .header(APIKEY_AUTH_HEADER, apiKey)
                .buildGet()
                .invoke(),
            (r, e) -> {
                final String context = String.format("dataBus(%s).poll(%s, %s, %s)", uri, subscriptionName, claimTtl, limit);
                LOG.warn("[{}] Unexpected response status [{}]: {}. May retry...", context, r.getStatus(), e);
                time.stop();
            }
        );

        try (InputStream is = response.readEntity(InputStream.class)) {
            final JsonNode jsonNode = JsonUtil.mapper().readTree(is);
            return ImmutableList.copyOf(jsonNode.elements());
        } catch (IOException e) {
            throw new RuntimeException(String.format("Unexpected error polling [%s]", subscriptionName), e);
        } finally {
            time.stop();
        }
    }

    public void acknowledge(final String subscriptionName,
                            final String eventKey,
                            final String apiKey) {
        final URI uri = UriBuilder.fromUri(baseURL)
            .segment("bus", "1", subscriptionName, "ack")
            .queryParam("partitioned", false)
            .build();

        final Response response = responseOrThrow(client
                .target(uri)
                .request()
                .accept(MediaType.APPLICATION_JSON_TYPE)
                .header(APIKEY_AUTH_HEADER, apiKey)
                .buildPost(Entity.json(ImmutableList.of(eventKey)))
                .invoke(),
            (r, e) -> {
                final String context = String.format("dataBus(%s).ack(%s, %s)", uri, subscriptionName, eventKey);
                LOG.warn("[{}] Unexpected response status [{}] from [{}]: {}. May retry...", context, r.getStatus(), r.getLocation(), e);
            }
        );

        consume(response.readEntity(InputStream.class));
    }

    public void acknowledge(final String subscriptionName,
                            final List<String> eventKeys,
                            final String apiKey) {
        final URI uri = UriBuilder.fromUri(baseURL)
            .segment("bus", "1", subscriptionName, "ack")
            .queryParam("partitioned", false)
            .build();

        final Response response = responseOrThrow(client
                .target(uri)
                .request()
                .accept(MediaType.APPLICATION_JSON_TYPE)
                .header(APIKEY_AUTH_HEADER, apiKey)
                .buildPost(Entity.json(eventKeys))
                .invoke(),
            (r, e) -> {
                final String context = String.format("dataBus(%s).ack(%s, %s)", uri, subscriptionName, eventKeys);
                LOG.warn("[{}] Unexpected response status [{}] from [{}]: {}. May retry...", context, r.getStatus(), r.getLocation(), e);
            }
        );

        consume(response.readEntity(InputStream.class));
    }
}
