package com.bazaarvoice.emopoller.busplus.lambda;

public class NoSuchFunctionException extends Exception {
    NoSuchFunctionException(final String s, final Throwable throwable) {
        super(s, throwable);
    }
}
