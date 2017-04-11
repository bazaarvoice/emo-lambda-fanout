package com.bazaarvoice.emopoller;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class EmoPollerConfiguration extends Configuration {
    public EmoPollerConfiguration(@JsonProperty("tenants") final Map<String, TenantConfiguration> tenantConfigurations,
                                  @JsonProperty("emodb") final EmoConfiguration emoConfiguration,
                                  @JsonProperty("lambda") final LambdaConfiguration lambdaConfiguration,
                                  @JsonProperty("poller") final PollerConfiguration pollerConfiguration) {
        this.tenantConfigurations = tenantConfigurations;
        this.emoConfiguration = emoConfiguration;
        this.lambdaConfiguration = lambdaConfiguration;
        this.pollerConfiguration = pollerConfiguration;
    }

    private final Map<String, TenantConfiguration> tenantConfigurations;

    public Map<String, TenantConfiguration> getTenantConfigurations() { return checkNotNull(tenantConfigurations); }

    public static class TenantConfiguration {
        public TenantConfiguration(@JsonProperty("keyDigestWhitelist") final Map<String, String> keyDigestWhitelist,
                                   @JsonProperty("emodb") final TenantEmoConfiguration tenantEmoConfiguration) {
            this.keyDigestWhitelist = keyDigestWhitelist;
            this.tenantEmoConfiguration = tenantEmoConfiguration;
        }

        private final Map<String, String> keyDigestWhitelist;

        public Map<String, String> getKeyDigestWhitelist() { return checkNotNull(keyDigestWhitelist); }

        private final TenantEmoConfiguration tenantEmoConfiguration;

        public TenantEmoConfiguration getTenantEmoConfiguration() { return checkNotNull(tenantEmoConfiguration); }

        public static class TenantEmoConfiguration {
            public TenantEmoConfiguration(@JsonProperty("baseURL") final String baseURL) {this.baseURL = baseURL;}

            private final String baseURL;

            public String getBaseURL() { return checkNotNull(baseURL); }
        }
    }

    private final EmoConfiguration emoConfiguration;

    EmoConfiguration getEmoConfiguration() {
        return checkNotNull(emoConfiguration);
    }

    public static class EmoConfiguration {
        public EmoConfiguration(@JsonProperty("apiKey") final String apiKey,
                                @JsonProperty("baseURL") final String baseURL,
                                @JsonProperty("subscriptionTable") final String subscriptionTable,
                                @JsonProperty("subscriptionPrefix") final String subscriptionPrefix) {
            this.apiKey = apiKey;
            this.baseURL = baseURL;
            this.subscriptionTable = subscriptionTable;
            this.subscriptionPrefix = subscriptionPrefix;
        }

        private final String apiKey;

        public String getApiKey() { return checkNotNull(apiKey); }

        private final String baseURL;

        public String getBaseURL() { return checkNotNull(baseURL); }

        private final String subscriptionTable;

        public String getSubscriptionTable() {return checkNotNull(subscriptionTable);}

        private final String subscriptionPrefix;

        public String getSubscriptionPrefix() {return checkNotNull(subscriptionPrefix);}
    }

    private LambdaConfiguration lambdaConfiguration;

    public LambdaConfiguration getLambdaConfiguration() {
        return checkNotNull(lambdaConfiguration);
    }

    public static class LambdaConfiguration {
        private LambdaConfiguration(
            @JsonProperty("local") final boolean local,
            @JsonProperty("nodejsHome") final String nodejsHome,
            @JsonProperty("pollSize") final int pollSize,
            @JsonProperty("processPoolSize") final int processPoolSize,
            @JsonProperty("processQueueSize") final int processQueueSize) {
            this.local = local;
            this.nodejsHome = nodejsHome;
            this.pollSize = pollSize;
            this.processPoolSize = processPoolSize;
            this.processQueueSize = processQueueSize;
        }

        private final boolean local;

        boolean isLocal() {
            return local;
        }

        private final String nodejsHome;

        public String getNodejsHome() {return checkNotNull(nodejsHome);}

        private final int pollSize;

        public int getPollSize() { return pollSize; }

        private int processPoolSize;

        public int getProcessPoolSize() {
            return processPoolSize;
        }

        private int processQueueSize;

        public int getProcessQueueSize() {
            return processQueueSize;
        }
    }

    private final PollerConfiguration pollerConfiguration;

    PollerConfiguration getProxyConfiguration() {
        return checkNotNull(pollerConfiguration);
    }

    static class PollerConfiguration {
        public PollerConfiguration(@JsonProperty("cmk") final String cmk) { this.cmk = cmk; }

        private String cmk;

        String getCMK() {
            return checkNotNull(cmk);
        }
    }
}
