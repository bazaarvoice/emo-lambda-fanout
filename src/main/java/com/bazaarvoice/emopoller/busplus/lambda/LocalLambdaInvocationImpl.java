package com.bazaarvoice.emopoller.busplus.lambda;

import com.bazaarvoice.emopoller.EmoPollerConfiguration;
import com.bazaarvoice.emopoller.util.JsonUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class LocalLambdaInvocationImpl implements LambdaInvocation {
    private final String nodejsHome;
    private static final Logger LOG = LoggerFactory.getLogger(LocalLambdaInvocationImpl.class);

    @Inject
    public LocalLambdaInvocationImpl(final EmoPollerConfiguration.LambdaConfiguration lambdaConfiguration) {
        this.nodejsHome = lambdaConfiguration.getNodejsHome();
    }

    private static String toPath(final String localLambdaArn) {
        return localLambdaArn.replaceAll(":", "/");
    }

    @Override public void check(final String lambdaArn) throws NoSuchFunctionException {
        final String path = toPath(lambdaArn);
        if (!new File(path).exists()) {
            throw new NoSuchFunctionException(String.format("arn[%s] path[%s]", lambdaArn, path), null);
        }
    }

    @Override public AWSLambdaInvocationImpl.InvocationResult invoke(final String lambdaArn, final JsonNode event) throws NoSuchFunctionException, FunctionErrorException {
        final Runtime runtime = Runtime.getRuntime();
        final String path = toPath(lambdaArn);
        final File file = new File(path);
        if (!file.exists()) {
            throw new NoSuchFunctionException(String.format("arn[%s] path[%s]", lambdaArn, path), null);
        }
        try {
            // Precondition: run "npm install -g node-lambda" and "node-lambda setup"
            final File eventFile = new File(path + "/temp.json");
            JsonUtil.mapper().writeValue(eventFile, event);
            final Process exec = runtime.exec(nodejsHome + "/node " + nodejsHome + "/node-lambda run --eventFile temp.json", new String[] {}, file);
            if (!exec.waitFor(10, TimeUnit.SECONDS) || exec.exitValue() != 0) {
                final String err = exec.exitValue() + ": " + read(exec.getInputStream()) + ": " + read(exec.getErrorStream());
                eventFile.delete();
                throw new FunctionErrorException(err, null);
            }
            eventFile.delete();
            LOG.info(String.format("Finished invocation:%n%s%n%s", read(exec.getInputStream()), read(exec.getErrorStream())));
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    private static String read(InputStream input) {
        try (BufferedReader buffer = new BufferedReader(new InputStreamReader(input))) {
            return buffer.lines().collect(Collectors.joining("\n"));
        } catch (IOException e) {
            throw new RuntimeException("unexpected", e);
        }
    }
}
