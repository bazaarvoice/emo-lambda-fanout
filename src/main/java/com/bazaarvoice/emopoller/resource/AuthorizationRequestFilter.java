package com.bazaarvoice.emopoller.resource;

import com.bazaarvoice.emopoller.EmoPollerConfiguration;
import com.bazaarvoice.emopoller.util.HashUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.name.Named;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.Provider;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AuthorizationRequestFilter implements ContainerRequestFilter {
    static final String APIKEY_AUTH_HEADER = "X-BV-API-Key";
    private static final String APIKEY_AUTH_ID_HEADER = "X-BV-API-Key-Id";

    private final ImmutableMap<String, ImmutableMap<String, String>> apiKeyDigestWhitelist;
    private final ImmutableMap<String, HashMap<String, String>> validKeyCache;

    @Inject public AuthorizationRequestFilter(final @Named("tenantConfigurations") Map tenantConfigurations) {
        final ImmutableMap.Builder<String, ImmutableMap<String, String>> whitelistBuilder = ImmutableMap.builder();
        final ImmutableMap.Builder<String, HashMap<String, String>> cacheBuilder = ImmutableMap.builder();
        //noinspection unchecked
        for (Map.Entry<String, EmoPollerConfiguration.TenantConfiguration> tenant : ((Map<String, EmoPollerConfiguration.TenantConfiguration>) tenantConfigurations).entrySet()) {
            final String tenantName = tenant.getKey();
            whitelistBuilder.put(tenantName, ImmutableMap.copyOf(tenant.getValue().getKeyDigestWhitelist()));
            cacheBuilder.put(tenantName, new HashMap<>());
        }
        this.apiKeyDigestWhitelist = whitelistBuilder.build();
        this.validKeyCache = cacheBuilder.build();
    }

    @Override public void filter(final ContainerRequestContext requestContext) throws IOException {
        final List<String> requestHeader = requestContext.getHeaders().get(APIKEY_AUTH_HEADER);
        if (requestHeader == null || requestHeader.size() != 1) {
            requestContext.abortWith(Response.status(401).entity("You must specify exactly one api key as a header [" + APIKEY_AUTH_HEADER + "].").build());
            return;
        }

        final List<String> tenantParam = requestContext.getUriInfo().getPathParameters().get("tenant");
        Preconditions.checkNotNull(tenantParam, "expect all requests to have a tenant path parameter");
        Preconditions.checkState(tenantParam.size() == 1, "expect to have only one value for path parameter 'tenant'");
        final String tenant = tenantParam.get(0);
        final String plainTextKey = requestHeader.get(0);

        if (!apiKeyDigestWhitelist.containsKey(tenant)) {
            requestContext.abortWith(Response.status(401).entity("Unknown tenant. Contact the poller administrators for help.").build());
            return;
        }

        // depending on memory dynamics, might be a thread-local view, but that's ok.
        if (validKeyCache.get(tenant).containsKey(plainTextKey)) {
            requestContext.getHeaders().putSingle(APIKEY_AUTH_ID_HEADER, validKeyCache.get(tenant).get(plainTextKey));
            //noinspection UnnecessaryReturnStatement
            return;
        } else {
            for (Map.Entry<String, String> nameAndDigest : apiKeyDigestWhitelist.get(tenant).entrySet()) {
                if (HashUtil.verifyArgon2(nameAndDigest.getValue(), plainTextKey)) {
                    validKeyCache.get(tenant).put(nameAndDigest.getValue(), nameAndDigest.getKey());
                    requestContext.getHeaders().putSingle(APIKEY_AUTH_ID_HEADER, nameAndDigest.getKey());
                    return;
                }
            }
            requestContext.abortWith(Response.status(401).entity("Unknown api key. Contact the poller administrators for help.").build());
        }
    }
}
