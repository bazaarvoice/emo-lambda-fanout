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
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AuthorizationRequestFilter implements ContainerRequestFilter {
    static final String APIKEY_AUTH_HEADER = "X-BV-API-Key";
    private static final String APIKEY_AUTH_ID_HEADER = "X-BV-API-Key-Id";

    private final ImmutableMap<String, ImmutableMap<String, String>> apiKeyDigestWhitelist;
    private final ImmutableMap<String, HashMap<String, String>> validKeyCache;

    @Inject public AuthorizationRequestFilter(final @Named("environmentConfigurations") Map environmentConfigurations) {
        final ImmutableMap.Builder<String, ImmutableMap<String, String>> whitelistBuilder = ImmutableMap.builder();
        final ImmutableMap.Builder<String, HashMap<String, String>> cacheBuilder = ImmutableMap.builder();
        //noinspection unchecked
        for (Map.Entry<String, EmoPollerConfiguration.EnvironmentConfiguration> environment : ((Map<String, EmoPollerConfiguration.EnvironmentConfiguration>) environmentConfigurations).entrySet()) {
            final String environmentName = environment.getKey();
            whitelistBuilder.put(environmentName, ImmutableMap.copyOf(environment.getValue().getKeyDigestWhitelist()));
            cacheBuilder.put(environmentName, new HashMap<>());
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

        final List<String> environmentParam = requestContext.getUriInfo().getPathParameters().get("environment");
        Preconditions.checkNotNull(environmentParam, "expect all requests to have a environment path parameter");
        Preconditions.checkState(environmentParam.size() == 1, "expect to have only one value for path parameter 'environment'");
        final String environment = environmentParam.get(0);
        final String plainTextKey = requestHeader.get(0);

        if (!apiKeyDigestWhitelist.containsKey(environment)) {
            requestContext.abortWith(Response.status(401).entity("Unknown environment. Contact the poller administrators for help.").build());
            return;
        }

        // depending on memory dynamics, might be a thread-local view, but that's ok.
        if (validKeyCache.get(environment).containsKey(plainTextKey)) {
            requestContext.getHeaders().putSingle(APIKEY_AUTH_ID_HEADER, validKeyCache.get(environment).get(plainTextKey));
            //noinspection UnnecessaryReturnStatement
            return;
        } else {
            for (Map.Entry<String, String> nameAndDigest : apiKeyDigestWhitelist.get(environment).entrySet()) {
                if (HashUtil.verifyArgon2(nameAndDigest.getValue(), plainTextKey)) {
                    validKeyCache.get(environment).put(nameAndDigest.getValue(), nameAndDigest.getKey());
                    requestContext.getHeaders().putSingle(APIKEY_AUTH_ID_HEADER, nameAndDigest.getKey());
                    return;
                }
            }
            requestContext.abortWith(Response.status(401).entity("Unknown api key. Contact the poller administrators for help.").build());
        }
    }
}
