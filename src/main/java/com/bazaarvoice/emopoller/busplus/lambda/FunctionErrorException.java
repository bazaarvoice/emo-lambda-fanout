package com.bazaarvoice.emopoller.busplus.lambda;

public class FunctionErrorException extends Exception {
    FunctionErrorException(final String s, final Throwable throwable) {
        super(s, throwable);
    }
}
