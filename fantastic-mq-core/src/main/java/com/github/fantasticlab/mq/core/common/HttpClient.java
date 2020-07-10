package com.github.fantasticlab.mq.core.common;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.Consts;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.*;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.pool.PoolStats;
import org.apache.http.util.EntityUtils;

import javax.annotation.PreDestroy;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class HttpClient implements Http {

    public HttpClient() {

        this.manager = poolingHttpClientConnectionManager();
        this.requestConfig = requestConfig();
        this.httpClientBuilder = httpClientBuilder(manager);
        this.client = closeableHttpClient(httpClientBuilder);

        HttpClientCloseThread thread = new HttpClientCloseThread(manager);
        thread.setDaemon(true);
        thread.start();


    }

    private PoolingHttpClientConnectionManager manager;
    private RequestConfig requestConfig;
    private CloseableHttpClient client;
    private HttpClientBuilder httpClientBuilder;

    @Override
    public <T> ApiResult<T> doGet(String url, Map<String, Object> params, TypeReference reference) throws HttpException {
        try {

            URIBuilder uriBuilder = new URIBuilder(url);
            uriBuilder.setCharset(Consts.UTF_8).build();
            if (params != null) {
                params.forEach((k, v) -> uriBuilder.addParameter(k, String.valueOf(v)));
            }
            HttpGet httpGet = new HttpGet(uriBuilder.build());
            httpGet.setConfig(requestConfig);

            CloseableHttpResponse response = client.execute(httpGet);
            log.info("HTTPClient doGet Url={} Params={} Resp.code={}",
                    url, params, response.getStatusLine().getStatusCode());
            if (response.getStatusLine().getStatusCode() != 200) {
                throw new HttpException("url=" + url +
                        "\nparams="+ params +
                        "\nstatus=" + response.getStatusLine().getStatusCode());
            }
            String responseEntity= EntityUtils.toString(response.getEntity());
            ObjectMapper mapper = new ObjectMapper();
            return mapper.readValue(responseEntity, reference);

        } catch (Exception e) {
            throw new HttpException(e);
        }
    }

    @Override
    public <T> ApiResult<T> doPost(String url, Map<String, Object> params, TypeReference reference) throws HttpException {
        try {
            HttpPost httpPost = new HttpPost(url);
            httpPost.setConfig(requestConfig);
            httpPost.addHeader("content-type", "application/json;charset=UTF-8");

            ObjectMapper mapper = new ObjectMapper();
            String jsonEntity = mapper.writeValueAsString(params);
            httpPost.setEntity(new StringEntity(jsonEntity, "UTF-8"));

            CloseableHttpResponse response = client.execute(httpPost);
            log.info("HTTPClient doPost Url={} Params={} Resp.code={}",
                    url, params, response.getStatusLine().getStatusCode());
            if (response.getStatusLine().getStatusCode() != 200) {
                throw new HttpException("url=" + url +
                        "\nparams="+ params +
                        "\nstatus=" + response.getStatusLine().getStatusCode());
            }
            String responseEntity= EntityUtils.toString(response.getEntity());
            return mapper.readValue(responseEntity, reference);

        } catch (Exception e) {
            throw new HttpException(e);
        }
    }

    private PoolingHttpClientConnectionManager poolingHttpClientConnectionManager() {
        PoolingHttpClientConnectionManager httpClientConnectionManager = new PoolingHttpClientConnectionManager();
        httpClientConnectionManager.setDefaultMaxPerRoute(20);
        httpClientConnectionManager.setMaxTotal(200);
        httpClientConnectionManager.setValidateAfterInactivity(2000);
        return httpClientConnectionManager;
    }

    private RequestConfig requestConfig(){
        return RequestConfig.custom().setConnectionRequestTimeout(2000)
                .setConnectTimeout(2000)
                .setSocketTimeout(2000)
                .build();
    }

    private HttpClientBuilder httpClientBuilder(PoolingHttpClientConnectionManager poolingHttpClientConnectionManager) {
        HttpClientBuilder httpClientBuilder = HttpClientBuilder.create();
        httpClientBuilder.setConnectionManager(poolingHttpClientConnectionManager);
        return httpClientBuilder;
    }


    private CloseableHttpClient closeableHttpClient(HttpClientBuilder httpClientBuilder){
        return httpClientBuilder.build();
    }

    private class HttpClientCloseThread extends Thread {

        public HttpClientCloseThread(PoolingHttpClientConnectionManager manager) {
            super();
            this.manager = manager;
        }

        public void setShutdown(boolean shutdown) {
            this.shutdown = shutdown;
        }

        private PoolingHttpClientConnectionManager manager;
        private volatile boolean shutdown = false;

        @Override
        public void run() {
            try {
                while(!shutdown) {
                    synchronized (this) {
                        wait(5000); // 5s
                        PoolStats stats = manager.getTotalStats();
                        log.info("PoolingHttpClientConnectionManager.state " +
                                        "Max={} Available={} Pending()={} Leased={}",
                                stats.getMax(),
                                stats.getAvailable(),
                                stats.getPending(),
                                stats.getLeased());
                        manager.closeExpiredConnections();
                    }
                }
            } catch (Exception e) {
                // ignore
            }

            super.run();
        }

        @PreDestroy
        public void shutdown() {
            shutdown = true;
            synchronized (this) {
                notifyAll();
            }
        }
    }

    public static void main(String[] args) throws HttpException {
        Http http = new HttpClient();
        Map<String, Object> map = new HashMap<>();
        map.put("topic", "test");
        map.put("body", "hello");
        ApiResult rs1 = http.doPost("http://127.0.0.1:8080/producer", map, new TypeReference<ApiResult<Void>>(){});
        assert rs1.isOK();

        map.put("body", "hello1");
        ApiResult rs2 = http.doPost("http://127.0.0.1:8080/producer", map, new TypeReference<ApiResult<Void>>(){});
        assert rs2.isOK();
    }
}
