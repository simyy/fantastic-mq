package com.github.fantasticlab.mq.core.producer;

import com.github.fantasticlab.mq.core.common.Message;

public interface Producer {

    boolean send(Message msg);

}
