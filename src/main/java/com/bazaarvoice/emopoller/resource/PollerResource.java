package com.bazaarvoice.emopoller.resource;

import com.bazaarvoice.emodb.sor.condition.Conditions;
import com.bazaarvoice.emodb.sor.delta.deser.ParseException;
import com.bazaarvoice.emopoller.busplus.LambdaSubscriptionManager;
import com.bazaarvoice.emopoller.busplus.lambda.InsufficientPermissionsException;
import com.bazaarvoice.emopoller.busplus.lambda.NoSuchFunctionException;
import com.bazaarvoice.emopoller.emo.DataBusClient;
import com.bazaarvoice.emopoller.emo.DataStoreClient;
import com.bazaarvoice.emopoller.util.HashUtil;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.dropwizard.jersey.params.IntParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * An intentionally limited poller to EmoDB.
 * This poller will have greater exposure than Emo itself,
 * so if someone achieves unauthorized access to the poller's api,
 * they will only be able to do what the poller can do.
 */

@Path("/poller")
public class PollerResource {
    private static final String APIKEY_AUTH_HEADER = "X-BV-API-Key";
    private static final String APPLICATION_X_JSON_DELTA = "application/x.json-delta";
    private static final String APPLICATION_X_JSON_CONDITION = "application/x.json-condition";
    private static final Logger LOG = LoggerFactory.getLogger(PollerResource.class);

    private final DataStoreClient dataStoreClient;
    private final DataBusClient dataBusClient;
    private final LambdaSubscriptionManager lambdaSubscriptionManager;
    private final ImmutableSet<String> apiKeyDigestWhitelist;

    @Inject
    public PollerResource(final DataStoreClient dataStoreClient,
                          final DataBusClient dataBusClient,
                          final LambdaSubscriptionManager lambdaSubscriptionManager,
                          final @Named("apiKeyDigestWhitelist") JsonNode apiKeyDigestWhitelist) {
        this.dataStoreClient = dataStoreClient;
        this.dataBusClient = dataBusClient;
        this.lambdaSubscriptionManager = lambdaSubscriptionManager;

        final ImmutableSet.Builder<String> builder = ImmutableSet.builder();
        final Iterator<Map.Entry<String, JsonNode>> fields = apiKeyDigestWhitelist.fields();
        while (fields.hasNext()) {
            final Map.Entry<String, JsonNode> next = fields.next();
            builder.add(next.getValue().textValue());
        }
        this.apiKeyDigestWhitelist = builder.build();
    }

    @Timed
    @POST
    @Consumes(APPLICATION_X_JSON_DELTA)
    public void subscribeLambda(@QueryParam("lambdaArn") final String lambdaArn,
                                @QueryParam("batchSize") final IntParam batchSize,
                                @QueryParam("claimTtl") final IntParam claimTtl, // this one is a function of how long the lambda to take to run.
                                @Context final HttpHeaders headers,
                                final String condition) {
        if (lambdaArn == null) throw new WebApplicationException("lambdaArn is required", 400);
        if (claimTtl == null) throw new WebApplicationException("claimTtl is required", 400);
        try {
            lambdaSubscriptionManager.register(lambdaArn, Conditions.fromString(condition), Duration.ofSeconds(claimTtl.get()), batchSize == null ? null : batchSize.get(), getKey(headers));
        } catch (NoSuchFunctionException | ParseException e) {
            throw new WebApplicationException(e.getMessage(), 404);
        } catch (InsufficientPermissionsException e) {
            throw new WebApplicationException(
                String.format(
                    "Insufficient permissions detected. " +
                        "You need to grant [%s] permission [%s] on your function [%s] " +
                        "(required permissions: lambda:GetFunctionConfiguration and lambda:InvokeFunction)",
                    e.getPrincipal(),
                    Joiner.on(',').join(e.getPermissions()),
                    lambdaArn
                ),
                400);
        }
    }

    @Timed
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public JsonNode getLambdaSubscription(@Nullable @QueryParam("lambdaArn") final String lambdaArn, @Context final HttpHeaders headers) {
        try {
            if (lambdaArn == null) {
                return lambdaSubscriptionManager.getAll(getKey(headers));
            } else {
                return lambdaSubscriptionManager.get(lambdaArn, getKey(headers));
            }
        } catch (LambdaSubscriptionManager.UnauthorizedException e) {
            throw new WebApplicationException("not authorized", 403);
        }
    }

    @Timed
    @DELETE
    @Produces(MediaType.APPLICATION_JSON)
    public JsonNode deactivateLambdaSubscription(@Nullable @QueryParam("lambdaArn") final String lambdaArn, @Context final HttpHeaders headers) {
        try {
            if (lambdaArn == null) {
                throw new WebApplicationException("lambdaArn is required", 400);
            } else {
                lambdaSubscriptionManager.deactivate(lambdaArn, getKey(headers));
                return lambdaSubscriptionManager.get(lambdaArn, getKey(headers));
            }
        } catch (LambdaSubscriptionManager.UnauthorizedException e) {
            throw new WebApplicationException("not authorized", 403);
        }
    }

    @Timed
    @POST @Path("activate")
    @Produces(MediaType.APPLICATION_JSON)
    public JsonNode activateLambdaSubscription(@Nullable @QueryParam("lambdaArn") final String lambdaArn, @Context final HttpHeaders headers) {
        try {
            if (lambdaArn == null) {
                throw new WebApplicationException("lambdaArn is required", 400);
            } else {
                lambdaSubscriptionManager.activate(lambdaArn, getKey(headers));
                return lambdaSubscriptionManager.get(lambdaArn, getKey(headers));
            }
        } catch (LambdaSubscriptionManager.UnauthorizedException e) {
            throw new WebApplicationException("not authorized", 403);
        }
    }

    @Timed
    @GET @Path("size")
    @Produces(MediaType.APPLICATION_JSON)
    public Integer getLambdaSubscriptionSize(@Nullable @QueryParam("lambdaArn") final String lambdaArn, @Context final HttpHeaders headers) {
        try {
            if (lambdaArn == null) {
                throw new WebApplicationException("lambdaArn is required", 400);
            } else {
                return lambdaSubscriptionManager.size(lambdaArn, getKey(headers));
            }
        } catch (LambdaSubscriptionManager.UnauthorizedException e) {
            throw new WebApplicationException("not authorized", 403);
        }
    }

    private String getKey(final @Context HttpHeaders headers) {
        final List<String> requestHeader = headers.getRequestHeader(APIKEY_AUTH_HEADER);
        if (requestHeader == null || requestHeader.size() != 1) {
            throw new WebApplicationException("You must specify exactly one api key as a header [" + APIKEY_AUTH_HEADER + "].", 401);
        }
        final String plainTextKey = requestHeader.get(0);

        for (String digest : apiKeyDigestWhitelist) {
            if (HashUtil.verifyArgon2(digest, plainTextKey)) {
                return plainTextKey;
            }
        }

        throw new WebApplicationException("Unknown api key. Contact the poller administrators for help.", 401);
    }
}
