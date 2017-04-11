package com.bazaarvoice.emopoller.resource;

import com.bazaarvoice.emodb.sor.condition.Conditions;
import com.bazaarvoice.emodb.sor.delta.deser.ParseException;
import com.bazaarvoice.emopoller.busplus.LambdaSubscriptionManager;
import com.bazaarvoice.emopoller.busplus.lambda.InsufficientPermissionsException;
import com.bazaarvoice.emopoller.busplus.lambda.NoSuchFunctionException;
import com.bazaarvoice.emopoller.busplus.lambda.model.LambdaSubscription;
import com.bazaarvoice.emopoller.util.JsonUtil;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Joiner;
import com.google.inject.Inject;
import io.dropwizard.jersey.params.IntParam;

import javax.annotation.Nullable;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import java.time.Duration;

import static com.bazaarvoice.emopoller.resource.AuthorizationRequestFilter.APIKEY_AUTH_HEADER;

@Path("/{tenant}/poller")
public class PollerResource {
    private static final String APPLICATION_X_JSON_DELTA = "application/x.json-delta";

    private final LambdaSubscriptionManager lambdaSubscriptionManager;

    @Inject
    public PollerResource(final LambdaSubscriptionManager lambdaSubscriptionManager) {
        this.lambdaSubscriptionManager = lambdaSubscriptionManager;

    }

    @Timed
    @POST
    @Consumes(APPLICATION_X_JSON_DELTA)
    public void subscribeLambda(@PathParam("tenant") String tenant,
                                @QueryParam("lambdaArn") final String lambdaArn,
                                @QueryParam("batchSize") final IntParam batchSize,
                                @QueryParam("claimTtl") final IntParam claimTtl, // this one is a function of how long the lambda to take to run.
                                @Context final HttpHeaders headers,
                                final String condition) {
        if (lambdaArn == null) throw new WebApplicationException("lambdaArn is required", 400);
        if (claimTtl == null) throw new WebApplicationException("claimTtl is required", 400);
        try {
            lambdaSubscriptionManager.register(tenant, lambdaArn, Conditions.fromString(condition), Duration.ofSeconds(claimTtl.get()), batchSize == null ? null : batchSize.get(), getKey(headers));
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
        } catch (IllegalArgumentException e) {
            throw new WebApplicationException("Illegal parameter:" + e.getMessage());
        }
    }

    @Timed
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public JsonNode getLambdaSubscription(final @PathParam("tenant") String tenant, @Nullable @QueryParam("lambdaArn") final String lambdaArn, @Context final HttpHeaders headers) {
        try {
            if (lambdaArn == null) {
                return JsonUtil.arr(lambdaSubscriptionManager.getAll(tenant, getKey(headers)).parallelStream().map(LambdaSubscription::asJson));
            } else {
                return lambdaSubscriptionManager.get(tenant, lambdaArn, getKey(headers)).asJson();
            }
        } catch (LambdaSubscriptionManager.UnauthorizedException e) {
            throw new WebApplicationException("not authorized", 403);
        }
    }

    @Timed
    @DELETE
    @Produces(MediaType.APPLICATION_JSON)
    public JsonNode deactivateLambdaSubscription(final @PathParam("tenant") String tenant, @Nullable @QueryParam("lambdaArn") final String lambdaArn, @Context final HttpHeaders headers) {
        try {
            if (lambdaArn == null) {
                throw new WebApplicationException("lambdaArn is required", 400);
            } else {
                lambdaSubscriptionManager.deactivate(tenant, lambdaArn, getKey(headers));
                return lambdaSubscriptionManager.get(tenant, lambdaArn, getKey(headers)).asJson();
            }
        } catch (LambdaSubscriptionManager.UnauthorizedException e) {
            throw new WebApplicationException("not authorized", 403);
        }
    }

    @Timed
    @POST @Path("activate")
    @Produces(MediaType.APPLICATION_JSON)
    public JsonNode activateLambdaSubscription(final @PathParam("tenant") String tenant, @Nullable @QueryParam("lambdaArn") final String lambdaArn, @Context final HttpHeaders headers) {
        try {
            if (lambdaArn == null) {
                throw new WebApplicationException("lambdaArn is required", 400);
            } else {
                lambdaSubscriptionManager.activate(tenant, lambdaArn, getKey(headers));
                return lambdaSubscriptionManager.get(tenant, lambdaArn, getKey(headers)).asJson();
            }
        } catch (LambdaSubscriptionManager.UnauthorizedException e) {
            throw new WebApplicationException("not authorized", 403);
        }
    }

    @Timed
    @GET @Path("size")
    @Produces(MediaType.APPLICATION_JSON)
    public Integer getLambdaSubscriptionSize(final @PathParam("tenant") String tenant, @Nullable @QueryParam("lambdaArn") final String lambdaArn, @Context final HttpHeaders headers) {
        try {
            if (lambdaArn == null) {
                throw new WebApplicationException("lambdaArn is required", 400);
            } else {
                return lambdaSubscriptionManager.size(tenant, lambdaArn, getKey(headers));
            }
        } catch (LambdaSubscriptionManager.UnauthorizedException e) {
            throw new WebApplicationException("not authorized", 403);
        }
    }

    // AuthorizedRequestFilter takes care of making sure it's a valid key.
    private String getKey(final @Context HttpHeaders headers) {
        return headers.getRequestHeader(APIKEY_AUTH_HEADER).get(0);
    }
}
