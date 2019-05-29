package io.zeebe;

import io.zeebe.exporter.api.context.Context;
import io.zeebe.exporter.api.context.Controller;
import io.zeebe.exporter.api.record.Record;
import io.zeebe.exporter.api.spi.Exporter;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.io.IOException;
import java.time.Duration;
import java.util.*;

public class EventStoreExporter implements Exporter
{
    private static final String ENV_PREFIX = "EVENT_STORE_EXPORTER_";
    private static final String ENV_URL = ENV_PREFIX + "URL";
    private static final String ENV_STREAM_NAME = ENV_PREFIX + "STREAM_NAME";
    private static final String MIME_TYPE = "application/vnd.eventstore.events+json";

    private Logger log;
    private Controller controller;

    private int batchSize;
    private Duration batchExecutionTimer;
    private String url;
    private int batchTimerMilli;
    private LinkedList<ImmutablePair<Long, JSONObject>> queue = new LinkedList<>();

    public void configure(final Context context) {
        log = context.getLogger();
        EventStoreExporterConfiguration configuration = context
                .getConfiguration()
                .instantiate(EventStoreExporterConfiguration.class);

        applyEnvironmentVariables(configuration);

        batchSize = configuration.batchSize;
        batchTimerMilli = configuration.batchTimeMilli;
        url = configuration.url + "/streams/" + configuration.streamName;

        log.debug("Exporter configured with {}", configuration);

        try {
            sendBatchToEventStore(new JSONArray());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void open(Controller controller) {
        this.controller = controller;
        if (batchTimerMilli > 0) {
            batchExecutionTimer = Duration.ofMillis(batchTimerMilli);
            scheduleNextBatch();
        }
    }

    private void scheduleNextBatch() {
        controller.scheduleTask(batchExecutionTimer, this::batchRecordsFromQueue);
    }

    private void sendBatchToEventStore(JSONArray jsonArray) throws IOException {
        final CloseableHttpClient client = HttpClients.createDefault();
        final HttpPost httpPost = new HttpPost(url);
        final StringEntity json = new StringEntity(jsonArray.toString());
        httpPost.setEntity(json);
        httpPost.setHeader("Content-Type", MIME_TYPE);
        client.execute(httpPost);
        client.close();
    }

    private void batchRecordsFromQueue() {
        if (queue.isEmpty()) {
            scheduleNextBatch();
            return;
        }

        OptionalLong lastRecordSentPosition = OptionalLong.empty();
        final int countRecordsToSend = Math.max(batchSize, queue.size());

        final JSONArray jsonArray = new JSONArray();

        for (int i = 0; i < countRecordsToSend; i ++) {
            final ImmutablePair<Long, JSONObject> current = queue.pollFirst();
            if (current != null) {
                jsonArray.put(current.getValue());
                lastRecordSentPosition = OptionalLong.of(current.getKey());
            }
        }
        
        try {
            sendBatchToEventStore(jsonArray);
            lastRecordSentPosition.ifPresent(p -> controller.updateLastExportedRecordPosition(p));
        } catch (IOException e) {
            log.debug(e.getMessage());
        } finally {
            scheduleNextBatch();
        }
    }

    public void close() {
        log.debug("Closing Event Store Exporter");
    }

    public void export(Record record) {
        final JSONObject json = new JSONObject();
        json.put("eventId", UUID.randomUUID());
        json.put("data", new JSONObject(record.toJson()));
        json.put("eventType", "ZeebeEvent");
        queue.add(new ImmutablePair<>(record.getPosition(), json));
    }

    private void applyEnvironmentVariables(final EventStoreExporterConfiguration configuration) {
        final Map<String, String> environment = System.getenv();

        Optional.ofNullable(environment.get(ENV_STREAM_NAME))
                .ifPresent(streamName -> configuration.streamName = streamName);
        Optional.ofNullable(environment.get(ENV_URL))
                .ifPresent(url -> configuration.url = url);
    }
}
