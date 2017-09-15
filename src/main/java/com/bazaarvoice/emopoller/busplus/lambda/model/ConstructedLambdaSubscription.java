package com.bazaarvoice.emopoller.busplus.lambda.model;

import com.bazaarvoice.emodb.sor.condition.Condition;
import com.bazaarvoice.emopoller.util.JsonUtil;
import com.fasterxml.jackson.databind.JsonNode;

import java.time.Duration;
import java.util.Optional;

public class ConstructedLambdaSubscription implements LambdaSubscription {
    private final String id;
    private final Long version;
    private final String environment;
    private final String subscriptionName;
    private final String lambdaArn;
    private final String condition;
    private final Duration claimTtl;
    private final Integer batchSize;
    private final String delegateApiKeyHash;
    private final String cypherTextDelegateApiKey;
    private final boolean active;
    private final String docCondition;

    public ConstructedLambdaSubscription(final String environment, final String subscriptionName, final String lambdaArn, final String tableCondition, final Optional<Condition> docCondition, final Duration claimTtl, final Integer batchSize, final String delegateApiKeyHash, final String cypherTextDelegateApiKey, final boolean active) {
        id = null;
        version = null;
        this.environment = environment;
        this.subscriptionName = subscriptionName;
        this.lambdaArn = lambdaArn;
        this.condition = tableCondition;
        this.docCondition = docCondition.map(Condition::toString).orElse(null);
        this.claimTtl = claimTtl;
        this.batchSize = batchSize;
        this.delegateApiKeyHash = delegateApiKeyHash;
        this.cypherTextDelegateApiKey = cypherTextDelegateApiKey;
        this.active = active;
    }

    public String getEnvironment() { return environment; }

    public String getSubscriptionName() { return subscriptionName; }

    public String getLambdaArn() { return lambdaArn; }

    public String getCondition() { return condition; }

    public String getDocCondition() { return docCondition; }

    public Duration getClaimTtl() { return claimTtl; }

    public String getDelegateApiKeyHash() { return delegateApiKeyHash; }

    public String getCypherTextDelegateApiKey() { return cypherTextDelegateApiKey; }

    public String getId() { return id; }

    public Integer getBatchSize() { return batchSize; }

    public boolean isActive() { return active; }

    @Override public long getVersion() { return version; }

    @Override public JsonNode asJson() {
        return JsonUtil.mapper().convertValue(this, JsonNode.class);
    }
}
