package com.github.fantasticlab.mq.core.broker;

import com.github.fantasticlab.mq.core.common.Message;

public interface Router {

    int getRouteKey(Message msg);

    int getRouteKey(String key, String topic);

}
