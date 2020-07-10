package com.github.fantasticlab.mq.core.producer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.github.fantasticlab.mq.core.common.*;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class ProducerClient implements Producer {

    public ProducerClient(Http http, String url) {
        this.http = http;
        this.url = url;
    }

    private Http http;

    private String url;

    @Override
    public boolean send(Message msg) {
        try {
            Map map = new HashMap();
            map.put("topic", msg.getTopic());
            map.put("body", msg.getBody());
            ApiResult<Void> rs = http.doPost(url, map, new TypeReference<ApiResult<Void>>(){});
            return rs.isOK();
        } catch (HttpException e) {
            log.error("ProducerClient Push error", e);
            return false;
        }

    }

}
