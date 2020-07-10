package com.github.fantasticlab.mq.core.broker;

import com.github.fantasticlab.mq.core.common.Message;

public interface Storage {

    boolean write(Message msg) throws StorageException;

    Message load(int offset);

    int getMaxOffset();

}
