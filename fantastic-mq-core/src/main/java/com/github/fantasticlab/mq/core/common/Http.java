package com.github.fantasticlab.mq.core.common;

import com.fasterxml.jackson.core.type.TypeReference;

import java.util.Map;

public interface Http {

    <T> ApiResult<T> doGet(String url, Map<String, Object> params, TypeReference reference);

    <T> ApiResult<T> doPost(String url, Map<String, Object> params, TypeReference reference);

}
