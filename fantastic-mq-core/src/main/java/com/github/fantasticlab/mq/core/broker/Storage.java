package com.github.fantasticlab.mq.core.broker;

import com.github.fantasticlab.mq.core.common.Message;

public interface Storage {

    boolean add(Message msg) throws StorageException;

    Message get(int offset);

    int getMaxOffset();

}
