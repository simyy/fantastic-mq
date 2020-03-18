package com.github.fantasticlab.mq.core.common;

public class HttpException extends RuntimeException {

    public HttpException(String message) {
        super(message);
    }

    public HttpException(Throwable cause) {
        super(cause);
    }

}
