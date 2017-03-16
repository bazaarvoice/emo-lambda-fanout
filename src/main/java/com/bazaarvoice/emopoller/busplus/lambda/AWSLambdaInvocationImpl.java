package com.bazaarvoice.emopoller.busplus.lambda;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.ResponseMetadata;
import com.amazonaws.services.lambda.AWSLambda;
import com.amazonaws.services.lambda.model.GetFunctionConfigurationRequest;
import com.amazonaws.services.lambda.model.InvocationType;
import com.amazonaws.services.lambda.model.InvokeRequest;
import com.amazonaws.services.lambda.model.InvokeResult;
import com.amazonaws.services.lambda.model.ResourceNotFoundException;
import com.bazaarvoice.emopoller.util.JsonUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AWSLambdaInvocationImpl implements LambdaInvocation {
    private final AWSLambda awsLambda;
    private final Pattern NotAuthorizedRegex = Pattern.compile(".*User: (\\S+) is not authorized to perform: (lambda:GetFunctionConfiguration|lambda:InvokeFunction).*");

    private static final Logger LOG = LoggerFactory.getLogger(AWSLambdaInvocationImpl.class);

    @Inject
    public AWSLambdaInvocationImpl(final AWSLambda awsLambda) {this.awsLambda = awsLambda;}

    @Override public void check(final String lambdaArn) throws NoSuchFunctionException, InsufficientPermissionsException {
        try {
            awsLambda.getFunctionConfiguration(new GetFunctionConfigurationRequest().withFunctionName(lambdaArn));
        } catch (ResourceNotFoundException e) {
            throw new NoSuchFunctionException(lambdaArn, e);
        } catch (AmazonServiceException e) {
            throw convertAmazonServiceException(e);
        }
    }

    public static class InvocationResult {
        private final String requestId;
        private final String resultPayload;

        InvocationResult(final String requestId, final String resultPayload) {
            this.requestId = requestId;
            this.resultPayload = resultPayload;
        }

        public String getRequestId() {
            return requestId;
        }

        public String getResultPayload() {
            return resultPayload;
        }
    }

    @Override public InvocationResult invoke(final String lambdaArn, final JsonNode event) throws NoSuchFunctionException, FunctionErrorException, InsufficientPermissionsException {
        final String payload;
        try {
            payload = JsonUtil.mapper().writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Highly unlikely...", e);
        }

        try {
            final InvokeRequest request = new InvokeRequest()
                .withFunctionName(lambdaArn)
                .withPayload(payload)
                .withInvocationType(InvocationType.RequestResponse);

            final InvokeResult response = awsLambda.invoke(request);

            final ResponseMetadata cachedResponseMetadata = awsLambda.getCachedResponseMetadata(request);

            final String resultPayload = new String(response.getPayload().array());
            final String requestId = cachedResponseMetadata == null ? "unknown" : cachedResponseMetadata.getRequestId();
            final String functionError = response.getFunctionError();

            LOG.info("Result of arn[{}] request[{}]: err[{}] payload[{}]",
                lambdaArn, requestId, Objects.toString(functionError), resultPayload
            );

            if (functionError == null) {
                return new InvocationResult(requestId, resultPayload);
            } else {
                switch (functionError) {
                    case "Handled": { // function error
                        throw new FunctionErrorException(resultPayload, null);
                    }
                    case "Unhandled": {
                        final String message = String.format("Unexpected error running function [%s]: [%s]", lambdaArn, resultPayload);
                        throw new FunctionErrorException(message, null);
                    }
                    default:
                        throw new RuntimeException(String.format("Unexpected result from function call: [%s]", functionError));
                }
            }

        } catch (ResourceNotFoundException e) {
            throw new NoSuchFunctionException(lambdaArn, e);
        } catch (AmazonServiceException e) {
            throw convertAmazonServiceException(e);
        }
    }

    private RuntimeException convertAmazonServiceException(final AmazonServiceException e) throws InsufficientPermissionsException {
        final Matcher matcher = NotAuthorizedRegex.matcher(e.getMessage());
        if (matcher.matches()) {
            final String myRole = matcher.group(1);
            final String missingPerm = matcher.group(2);
            throw new InsufficientPermissionsException(myRole, ImmutableSet.of(missingPerm));
        } else {
            return e;
        }
    }

    // for playing with Lambda
    public static void main(String[] args) throws IOException, NoSuchFunctionException, InsufficientPermissionsException, FunctionErrorException {

    }
}
