package com.github.fantasticlab.mq.core.broker;

import com.github.fantasticlab.mq.core.common.Message;

public interface Persistence {

    boolean writeMsg(Message msg);

    boolean writeMsg2Disk(Message msg);

    boolean flushDisk(int millseconds);

}
