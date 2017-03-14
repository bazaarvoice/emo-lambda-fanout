package com.bazaarvoice.emopoller.busplus.lambda;

import com.fasterxml.jackson.databind.JsonNode;

public interface LambdaInvocation {
    void check(String lambdaArn) throws NoSuchFunctionException, InsufficientPermissionsException;
    AWSLambdaInvocationImpl.InvocationResult invoke(String lambdaArn, JsonNode event) throws NoSuchFunctionException, FunctionErrorException, InsufficientPermissionsException;
}
