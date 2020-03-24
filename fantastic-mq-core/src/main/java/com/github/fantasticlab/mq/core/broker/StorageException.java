package com.github.fantasticlab.mq.core.broker;

public class StorageException extends BrokerException {

    public StorageException(String message) {
        super(message);
    }

    public StorageException(Throwable cause) {
        super(cause);
    }
}
