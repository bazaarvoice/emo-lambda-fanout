package com.bazaarvoice.emopoller.busplus.lambda;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.lambda.AWSLambda;
import com.amazonaws.services.lambda.model.GetFunctionConfigurationRequest;
import com.google.common.collect.ImmutableSet;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class AWSLambdaInvocationImplTest {
    @Test public void testPermissionsErrorRegex() {
        final AWSLambda mock = Mockito.mock(AWSLambda.class);
        when(mock.getFunctionConfiguration(any(GetFunctionConfigurationRequest.class)))
            .thenThrow(new AmazonServiceException(
                "User: arn:aws:sts::999999999999:assumed-role/SOME_ROLE/i-07898c52e00279786 is not authorized to perform: " +
                    "lambda:GetFunctionConfiguration on resource: arn:aws:lambda:us-east-1:999999999999:function:SOME_FUNCTION (Service: AWSLambda; " +
                    "Status Code: 403; Error Code: null; Request ID: da389f7b-87f5-11e6-874b-f9e5c53546e2)"
            ));

        final AWSLambdaInvocationImpl awsLambdaInvocation = new AWSLambdaInvocationImpl(mock);

        try {
            awsLambdaInvocation.check("arn:aws:lambda:us-east-1:999999999999:function:SOME_FUNCTION");
            fail("Should have thrown");
        } catch (NoSuchFunctionException e) {
            fail("Not expecting this error");
        } catch (InsufficientPermissionsException e) {
            assertEquals(e.getPrincipal(), "arn:aws:sts::999999999999:assumed-role/SOME_ROLE/i-07898c52e00279786");
            assertEquals(e.getPermissions(), ImmutableSet.of("lambda:GetFunctionConfiguration"));
        }
    }

    @Test public void testPermissionsErrorRegex2() {
        final AWSLambda mock = Mockito.mock(AWSLambda.class);
        when(mock.getFunctionConfiguration(any(GetFunctionConfigurationRequest.class)))
            .thenThrow(new AmazonServiceException(
                "User: arn:aws:sts::999999999999:assumed-role/SOME_ROLE/i-0f32d1ab17d6b5989 is not authorized to perform: lambda:InvokeFunction on resource: " +
                    "arn:aws:lambda:us-east-1:999999999999:function:SOME_FUNCTION (Service: AWSLambda; Status Code: 403; Error Code: null; " +
                    "Request ID: 539f2203-ab75-11e6-971c-a123a22e3c06)"
            ));

        final AWSLambdaInvocationImpl awsLambdaInvocation = new AWSLambdaInvocationImpl(mock);

        try {
            awsLambdaInvocation.check("arn:aws:lambda:us-east-1:999999999999:function:SOME_FUNCTION");
            fail("Should have thrown");
        } catch (NoSuchFunctionException e) {
            fail("Not expecting this error");
        } catch (InsufficientPermissionsException e) {
            assertEquals(e.getPrincipal(), "arn:aws:sts::999999999999:assumed-role/SOME_ROLE/i-0f32d1ab17d6b5989");
            assertEquals(e.getPermissions(), ImmutableSet.of("lambda:InvokeFunction"));
        }
    }
}
