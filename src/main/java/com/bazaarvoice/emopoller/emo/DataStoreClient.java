package com.bazaarvoice.emopoller.emo;

import com.bazaarvoice.emodb.sor.delta.Delta;
import com.bazaarvoice.emopoller.EmoPollerConfiguration;
import com.bazaarvoice.emopoller.util.JsonUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static com.bazaarvoice.emopoller.emo.EmoUtils.responseOrThrow;

public class DataStoreClient {
    private final Client client;
    private final String baseURL;

    private static final String APIKEY_AUTH_HEADER = "X-BV-API-Key";
    private static final MediaType APPLICATION_X_JSON_DELTA_TYPE = new MediaType("application", "x.json-delta");
    private static final Logger LOG = LoggerFactory.getLogger(DataStoreClient.class);

    @Inject
    public DataStoreClient(final Client client,
                           final EmoPollerConfiguration.EmoConfiguration emoConfiguration) {
        this.client = client;
        this.baseURL = emoConfiguration.getBaseURL();
    }

    public DataStoreClient(final Client client,
                           final String baseURL) {

        this.client = client;
        this.baseURL = baseURL;
    }

    public JsonNode updateTable(final String table,
                                final String options,
                                final JsonNode template,
                                final String audit,
                                final String apiKey) {
        final URI uri = UriBuilder.fromUri(baseURL)
            .segment("sor", "1", "_table", table)
            .queryParam("options", options)
            .queryParam("audit", audit)
            .build();

        final Response response = responseOrThrow(client
                .target(uri)
                .request()
                .accept(MediaType.APPLICATION_JSON_TYPE)
                .header(APIKEY_AUTH_HEADER, apiKey)
                .buildPut(Entity.json(template))
                .invoke(),
            (r, e) -> {
                final String context = String.format("dataStore(%s).updateTable(%s, %s, %s, %s)", uri, table, options, template, audit);
                LOG.warn("[{}] Unexpected response status [{}] from [{}]: {}. May retry...", context, r.getStatus(), r.getLocation(), e);
            }
        );

        try (InputStream is = response.readEntity(InputStream.class)) {
            return JsonUtil.mapper().readTree(is);
        } catch (IOException e) {
            throw new RuntimeException(String.format("Unexpected error updating [%s]", table), e);
        }
    }

    public Collection<JsonNode> listTables(final String from, final Long limit, final String apiKey) {
        final UriBuilder uri = UriBuilder.fromUri(baseURL)
            .segment("sor", "1", "_table");
        if (from != null) {
            uri.queryParam("from", from);
        }
        if (limit != null) {
            uri.queryParam("limit", limit);
        }

        final Response response = responseOrThrow(client
                .target(uri.build())
                .request()
                .accept(MediaType.APPLICATION_JSON_TYPE)
                .header(APIKEY_AUTH_HEADER, apiKey)
                .buildGet()
                .invoke(),
            (r, e) -> {
                final String context = String.format("dataStore(%s).listTables(%s, %s)", uri, from, limit);
                LOG.warn("[{}] Unexpected response status [{}] from [{}]: {}. May retry...", context, r.getStatus(), r.getLocation(), e);
            }
        );

        try (InputStream is = response.readEntity(InputStream.class)) {
            return ImmutableList.copyOf(JsonUtil.mapper().readTree(is).elements());
        } catch (IOException e) {
            throw new RuntimeException("Unexpected error getting listing tables", e);
        }
    }

    public JsonNode get(final String table,
                        final String key,
                        final String apiKey) {
        final URI uri = UriBuilder.fromUri(baseURL)
            .segment("sor", "1", table, key)
            .build();

        final Response response = responseOrThrow(client
                .target(uri)
                .request()
                .accept(MediaType.APPLICATION_JSON_TYPE)
                .header(APIKEY_AUTH_HEADER, apiKey)
                .buildGet()
                .invoke(),
            (r, e) -> {
                final String context = String.format("dataStore(%s).get(%s, %s)", uri, table, key);
                LOG.warn("[{}] Unexpected response status [{}] from [{}]: {}. May retry...", context, r.getStatus(), r.getLocation(), e);
            }
        );

        try (InputStream is = response.readEntity(InputStream.class)) {
            return JsonUtil.mapper().readTree(is);
        } catch (IOException e) {
            throw new RuntimeException(String.format("Unexpected error reading [%s][%s]", table, key), e);
        }
    }

    public JsonNode delete(final String table,
                           final String key,
                           final String audit,
                           final List<String> tags,
                           final String apiKey) {
        final UriBuilder uri = UriBuilder.fromUri(baseURL)
            .segment("sor", "1", table, key)
            .queryParam("audit", audit);


        for (String tag : tags) {
            uri.queryParam("tag", tag);
        }

        final Response response = responseOrThrow(client
                .target(uri.build())
                .request()
                .accept(MediaType.APPLICATION_JSON_TYPE)
                .header(APIKEY_AUTH_HEADER, apiKey)
                .buildDelete()
                .invoke(),
            (r, e) -> {
                final String context = String.format("dataStore(%s).get(%s, %s)", uri, table, key);
                LOG.warn("[{}] Unexpected response status [{}] from [{}]: {}. May retry...", context, r.getStatus(), r.getLocation(), e);
            }
        );

        try (InputStream is = response.readEntity(InputStream.class)) {
            return JsonUtil.mapper().readTree(is);
        } catch (IOException e) {
            throw new RuntimeException(String.format("Unexpected error reading [%s][%s]", table, key), e);
        }
    }

    public JsonNode update(final String table,
                           final String key,
                           final Delta delta,
                           final String audit,
                           final List<String> tags,
                           final String apiKey) {
        final UriBuilder uri = UriBuilder.fromUri(baseURL)
            .segment("sor", "1", table, key)
            .queryParam("audit", audit);

        for (String tag : tags) {
            uri.queryParam("tag", tag);
        }

        final Response response = responseOrThrow(client
                .target(uri.build())
                .request()
                .accept(MediaType.APPLICATION_JSON_TYPE)
                .header(APIKEY_AUTH_HEADER, apiKey)
                .buildPost(Entity.entity(delta.toString(), APPLICATION_X_JSON_DELTA_TYPE))
                .invoke(),
            (r, e) -> {
                final String context = String.format("dataStore(%s).update(%s, %s, %s, %s)", uri, table, key, delta, audit);
                LOG.warn("[{}] Unexpected response status [{}] from [{}]: {}. May retry...", context, r.getStatus(), r.getLocation(), e);
            }
        );

        try (InputStream is = response.readEntity(InputStream.class)) {
            return JsonUtil.mapper().readTree(is);
        } catch (IOException e) {
            throw new RuntimeException(String.format("Unexpected error updating [%s][%s]", table, key), e);
        }
    }

    public Collection<JsonNode> scan(final String table, final String from, final Long limit, final String apiKey) {
        final UriBuilder uri = UriBuilder.fromUri(baseURL)
            .segment("sor", "1", table);
        if (from != null) {
            uri.queryParam("from", from);
        }
        if (limit != null) {
            uri.queryParam("limit", limit);
        }

        final Response response = responseOrThrow(client
                .target(uri.build())
                .request()
                .accept(MediaType.APPLICATION_JSON_TYPE)
                .header(APIKEY_AUTH_HEADER, apiKey)
                .buildGet()
                .invoke(),
            (r, e) -> {
                final String context = String.format("dataStore(%s).scan(%s, %s, %s)", uri, table, from, limit);
                LOG.warn("[{}] Unexpected response status [{}] from [{}]: {}. May retry...", context, r.getStatus(), r.getLocation(), e);
            }
        );

        try (InputStream is = response.readEntity(InputStream.class)) {
            return ImmutableList.copyOf(JsonUtil.mapper().readTree(is).elements());
        } catch (IOException e) {
            throw new RuntimeException(String.format("Unexpected error scanning [%s]", table), e);
        }
    }

    public Collection<String> getSplits(final String table, final int desiredRecordsPerSplit, final String apiKey) {
        final URI uri = UriBuilder.fromUri(baseURL)
            .segment("sor", "1", "_split", table)
            .queryParam("size", desiredRecordsPerSplit)
            .build();

        final Response response = responseOrThrow(client
                .target(uri)
                .request()
                .accept(MediaType.APPLICATION_JSON_TYPE)
                .header(APIKEY_AUTH_HEADER, apiKey)
                .buildGet()
                .invoke(),
            (r, e) -> {
                final String context = String.format("dataStore(%s).getSplits(%s, %s)", uri, table, desiredRecordsPerSplit);
                LOG.warn("[{}] Unexpected response status [{}] from [{}]: {}. May retry...", context, r.getStatus(), r.getLocation(), e);
            }
        );

        try (InputStream is = response.readEntity(InputStream.class)) {
            return ImmutableList.copyOf(JsonUtil.mapper().readTree(is).elements()).stream().map(JsonNode::textValue).collect(Collectors.toList());
        } catch (IOException e) {
            throw new RuntimeException(String.format("Unexpected error getting splits for [%s]", table), e);
        }
    }

    public Collection<JsonNode> getSplit(final String table, final String split, final @Nullable String fromKeyExclusive, final long limit, final String apiKey) {
        final UriBuilder uri = UriBuilder.fromUri(baseURL)
            .segment("sor", "1", "_split", table, split)
            .queryParam("limit", limit);

        if (fromKeyExclusive != null) {
            uri.queryParam("from", fromKeyExclusive);
        }

        final Response response = responseOrThrow(client
                .target(uri.build())
                .request()
                .accept(MediaType.APPLICATION_JSON_TYPE)
                .header(APIKEY_AUTH_HEADER, apiKey)
                .buildGet()
                .invoke(),
            (r, e) -> {
                final String context = String.format("dataStore(%s).getSplit(%s, %s, %s, %s)", uri, table, split, fromKeyExclusive, limit);
                LOG.warn("[{}] Unexpected response status [{}] from [{}]: {}. May retry...", context, r.getStatus(), r.getLocation(), e);
            }
        );

        try (InputStream is = response.readEntity(InputStream.class)) {
            return ImmutableList.copyOf(JsonUtil.mapper().readTree(is).elements());
        } catch (IOException e) {
            throw new RuntimeException(String.format("Unexpected error getting splits for [%s]", table), e);
        }
    }
}
