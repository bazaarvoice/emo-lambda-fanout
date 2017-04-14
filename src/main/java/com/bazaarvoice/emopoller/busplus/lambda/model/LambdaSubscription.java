package com.bazaarvoice.emopoller.busplus.lambda.model;

import com.fasterxml.jackson.databind.JsonNode;
import org.vvcephei.occ_map.Versioned;

import java.time.Duration;

public interface LambdaSubscription extends Versioned {
    String getEnvironment();

    String getSubscriptionName();

    String getLambdaArn();

    String getCondition();

    Duration getClaimTtl();

    String getDelegateApiKeyHash();

    String getCypherTextDelegateApiKey();

    String getId();

    Integer getBatchSize();

    boolean isActive();

    @Override long getVersion();

    JsonNode asJson();
}
