package com.bazaarvoice.emopoller;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import io.dropwizard.Configuration;

import static com.google.common.base.Preconditions.checkNotNull;

public class EmoPollerConfiguration extends Configuration {
    private final EmoConfiguration emoConfiguration;
    private LambdaConfiguration lambdaConfiguration;
    private final ProxyConfiguration pollerConfiguration;


    public EmoPollerConfiguration(@JsonProperty("emodb") final EmoConfiguration emoConfiguration,
                                  @JsonProperty("lambda") final LambdaConfiguration lambdaConfiguration,
                                  @JsonProperty("poller") final ProxyConfiguration pollerConfiguration) {
        this.emoConfiguration = emoConfiguration;
        this.lambdaConfiguration = lambdaConfiguration;
        this.pollerConfiguration = pollerConfiguration;
    }

    EmoConfiguration getEmoConfiguration() {
        return checkNotNull(emoConfiguration);
    }

    LambdaConfiguration getLambdaConfiguration() {
        return checkNotNull(lambdaConfiguration);
    }

    ProxyConfiguration getProxyConfiguration() {
        return checkNotNull(pollerConfiguration);
    }

    public static class EmoConfiguration {
        private final String apiKey;
        private final String baseURL;

        public EmoConfiguration(@JsonProperty("apiKey") final String apiKey, @JsonProperty("baseURL") final String baseURL) {
            this.apiKey = apiKey;
            this.baseURL = baseURL;
        }

        public String getApiKey() {
            return checkNotNull(apiKey);
        }

        public String getBaseURL() {
            return checkNotNull(baseURL);
        }
    }

    public static class LambdaConfiguration {
        private final boolean local;
        private final String nodejsHome;
        private final int pollSize;
        private int processPoolSize;
        private int processQueueSize;
        private final int pollAllConcurrency;

        private LambdaConfiguration(
            @JsonProperty("local") final boolean local,
            @JsonProperty("nodejsHome") final String nodejsHome,
            @JsonProperty("pollSize") final int pollSize,
            @JsonProperty("processPoolSize") final int processPoolSize,
            @JsonProperty("processQueueSize") final int processQueueSize,
            @JsonProperty("pollAllConcurrency") final int pollAllConcurrency) {
            this.local = local;
            this.nodejsHome = nodejsHome;
            this.pollSize = pollSize;
            this.processPoolSize = processPoolSize;
            this.processQueueSize = processQueueSize;
            this.pollAllConcurrency = pollAllConcurrency;
        }


        boolean isLocal() {
            return local;
        }

        public String getNodejsHome() {return checkNotNull(nodejsHome);}

        public int getPollSize() { return pollSize; }

        public int getProcessPoolSize() {
            return processPoolSize;
        }

        public int getProcessQueueSize() {
            return processQueueSize;
        }

        public int getPollAllConcurrency() { return pollAllConcurrency; }
    }

    static class ProxyConfiguration {
        private final JsonNode apiKeyDigestWhitelist;
        private String cmk;

        public ProxyConfiguration(
            @JsonProperty("apiKeyDigestWhitelist") final JsonNode apiKeyDigestWhitelist,
            @JsonProperty("cmk") final String cmk) {
            this.apiKeyDigestWhitelist = apiKeyDigestWhitelist;
            this.cmk = cmk;
        }

        JsonNode getApiKeyDigestWhitelist() {
            return checkNotNull(apiKeyDigestWhitelist);
        }

        String getCMK() {
            return checkNotNull(cmk);
        }
    }
}
