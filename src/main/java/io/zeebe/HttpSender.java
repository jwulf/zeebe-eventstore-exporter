package io.zeebe;

import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.json.JSONArray;

import java.io.IOException;

class HttpSender {
    private static final String MIME_TYPE = "application/vnd.eventstore.events+json";
    final private String url;
    int backOffFactor = 1;
    boolean failed = false;

    HttpSender(EventStoreExporterConfiguration configuration) {
        url = configuration.url + "/streams/" + configuration.streamName;
    }

    void send(JSONArray eventBatch) throws IOException {
        try (final CloseableHttpClient client = HttpClients.createDefault()) {
            final HttpPost httpPost = new HttpPost(url);
            final StringEntity json = new StringEntity(eventBatch.toString());
            httpPost.setEntity(json);
            httpPost.setHeader("Content-Type", MIME_TYPE);
            backOffFactor ++;
            failed = true;
            client.execute(httpPost);
            failed = false;
            backOffFactor = 1;
        }
    }




}
