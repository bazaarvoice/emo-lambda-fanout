package com.bazaarvoice.emopoller.busplus.lambda;

import com.bazaarvoice.emodb.sor.condition.Condition;
import com.bazaarvoice.emodb.sor.delta.Delta;
import com.bazaarvoice.emodb.sor.delta.Deltas;
import com.bazaarvoice.emopoller.util.HashUtil;
import com.fasterxml.jackson.databind.JsonNode;

import java.time.Duration;

public class LambdaSubscriptionDAO {
    private LambdaSubscriptionDAO() {}

    public static class LambdaSubscription {
        private final String id;
        private final String subscriptionName;
        private final String lambdaArn;
        private final String condition;
        private final Duration claimTtl;
        private final Integer batchSize;
        private final String delegateApiKey;
        private final String cypherTextDelegateApiKey;

        public LambdaSubscription(final String id, final String subscriptionName, final String lambdaArn, final Condition condition, final Duration claimTtl, final Integer batchSize, final String delegateApiKey, final String cypherTextDelegateApiKey) {
            this.id = id;
            this.subscriptionName = subscriptionName;
            this.lambdaArn = lambdaArn;
            this.condition = condition.toString();
            this.claimTtl = claimTtl;
            this.batchSize = batchSize;
            this.delegateApiKey = delegateApiKey;
            this.cypherTextDelegateApiKey = cypherTextDelegateApiKey;
        }

        public LambdaSubscription(final String id, final String subscriptionName, final String lambdaArn, final String condition, final Duration claimTtl, final Integer batchSize, final String delegateApiKey, final String cypherTextDelegateApiKey) {
            this.id = id;
            this.subscriptionName = subscriptionName;
            this.lambdaArn = lambdaArn;
            this.condition = condition;
            this.claimTtl = claimTtl;
            this.batchSize = batchSize;
            this.delegateApiKey = delegateApiKey;
            this.cypherTextDelegateApiKey = cypherTextDelegateApiKey;
        }

        public String getSubscriptionName() {
            return subscriptionName;
        }

        public String getLambdaArn() {
            return lambdaArn;
        }

        public String getCondition() {
            return condition;
        }

        public Duration getClaimTtl() {
            return claimTtl;
        }

        public String getDelegateApiKey() {
            return delegateApiKey;
        }

        public String getCypherTextDelegateApiKey() {
            return cypherTextDelegateApiKey;
        }

        public String getId() {
            return id;
        }

        public Integer getBatchSize() {
            return batchSize;
        }
    }

    public static Delta createDelta(final LambdaSubscription lambdaSubscription) {
        return Deltas.mapBuilder()
            .put("subscriptionName", lambdaSubscription.getSubscriptionName())
            .put("lambdaArn", lambdaSubscription.getLambdaArn())
            .put("condition", lambdaSubscription.getCondition())
            .put("claimTtl", lambdaSubscription.getClaimTtl().getSeconds())
            .put("delegateApiKeyCypherText", lambdaSubscription.getCypherTextDelegateApiKey())
            .put("delegateApiKeyArgon2Hash", HashUtil.argon2hash(lambdaSubscription.getDelegateApiKey()))
            .put("active", true)
            .put("batchSize", lambdaSubscription.getBatchSize())
            .build();
    }

    public static Delta deactivateDelta() {
        return Deltas.mapBuilder()
            .put("active", false)
            .build();
    }

    public static Delta activateDelta() {
        return Deltas.mapBuilder()
            .put("active", true)
            .build();
    }

    public static Delta errorDelta(final String error, final String errorTime) {
        return Deltas.mapBuilder()
            .update("errors", Deltas.mapBuilder().put(errorTime, error).build())
            .build();
    }

    public static LambdaSubscription asLambdaSubscription(final JsonNode jsonNode) {
        final String subscriptionId = getId(jsonNode);
        final String subscriptionName = getSubscriptionName(jsonNode);
        final String condition = jsonNode.path("condition").textValue();
        final String lambdaArn = getLambdaArn(jsonNode);
        final Duration claimTtl = Duration.ofSeconds(jsonNode.path("claimTtl").intValue());
        final String delegateApiKeyCypherText = jsonNode.path("delegateApiKeyCypherText").textValue();
        final String delegateApiKeyHash = getDelegateApiKeyHash(jsonNode);
        final Integer batchSize = jsonNode.path("batchSize").asInt(1);
        return new LambdaSubscription(subscriptionId, subscriptionName, lambdaArn, condition, claimTtl, batchSize, delegateApiKeyHash, delegateApiKeyCypherText);
    }

    public static String getId(final JsonNode jsonNode) {return jsonNode.path("~id").asText("");}

    public static String getDelegateApiKeyHash(final JsonNode jsonNode) {return jsonNode.path("delegateApiKeyHash").asText("");}

    public static String getDelegateApiKeyArgon2Hash(final JsonNode jsonNode) {return jsonNode.path("delegateApiKeyArgon2Hash").asText("");}

    public static String getSubscriptionName(final JsonNode jsonNode) {return jsonNode.path("subscriptionName").asText("");}

    public static String getSubscriptionName(final JsonNode jsonNode, final String defaultName) {return jsonNode.path("subscriptionName").asText(defaultName);}

    public static String getLambdaArn(final JsonNode jsonNode) {return jsonNode.path("lambdaArn").asText("");}

    public static boolean getActive(final JsonNode jsonNode) {return jsonNode.path("active").asBoolean(false);}
}
