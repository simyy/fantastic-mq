package com.github.fantasticlab.mq.core.consumer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fantasticlab.mq.core.common.ApiResult;
import com.github.fantasticlab.mq.core.common.Http;
import com.github.fantasticlab.mq.core.common.HttpClient;
import com.github.fantasticlab.mq.core.common.Message;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class ConsumerClient implements Consumer {

    public ConsumerClient(Http http, String url) {
        this.http = http;
        this.url = url;
    }

    private Http http;

    private String url;

    @Override
    public Message pull(String topic, int offset) throws ConsumerException {
        try {
            Map<String, Object> map = new HashMap();
            map.put("topic", topic);
            map.put("offset", offset);
            ApiResult<Message> rs = http.doGet(url + "/consumer", map, new TypeReference<ApiResult<Message>>(){});
            if (!rs.isOK()) {
                log.error("ConsumerClient PULL ERROR Result={}", rs);
                throw new ConsumerException("PULL ERROR");
            }
            log.info("ConsumerClient PULL MSG={}", rs.getMsg());
            return rs.getMsg();
        } catch (Exception e) {
            // pass
        }
        return null;
    }

    @Override
    public int offset(String topic, String group) throws ConsumerException {
        Map<String, Object> map = new HashMap();
        map.put("topic", topic);
        map.put("group", group);
        ApiResult<Integer> rs = http.doGet(url + "/offset", map, new TypeReference<ApiResult<Integer>>(){});
        if (!rs.isOK()) {
            log.error("ConsumerClient OFFSET ERROR RS={}", rs);
            throw new ConsumerException("OFFSET ERROR");
        }
        log.info("ConsumerClient PULL MSG={}", rs.getMsg());
        return rs.getMsg();
    }

    @Override
    public boolean confirm(String topic, String group, int offset){
        Map<String, Object> map = new HashMap();
        map.put("topic", topic);
        map.put("group", group);
        map.put("offset", offset);
        ApiResult<Void> rs = http.doPost(url + "/confirm", map, new TypeReference<ApiResult<Void>>(){});
        log.info("ConsumerClient PULL MSG={}", rs.getMsg());
        return rs.isOK();
    }

}
