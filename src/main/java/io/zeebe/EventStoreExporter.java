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
    private static final String ENV_BATCH_SIZE = ENV_PREFIX + "BATCH_SIZE";
    private static final String ENV_BATCH_TIME_MILLI = ENV_PREFIX + "BATCH_TIME_MILLI";
    private static final String ENV_RETRY_TIME_MILLI = ENV_PREFIX + "RETRY_TIME_MILLI";
    private static final String MIME_TYPE = "application/vnd.eventstore.events+json";

    private Logger log;
    private Controller controller;

    private int batchSize;
    private int retryTimeMillis;
    private String url;
    private int batchTimerMilli;
    private int backoffFactor;
    private LinkedList<ImmutablePair<Long, JSONObject>> queue = new LinkedList<>();
    private LinkedList<JSONArray> retryQueue = new LinkedList<>();

    public void configure(final Context context) {
        log = context.getLogger();
        EventStoreExporterConfiguration configuration = context
                .getConfiguration()
                .instantiate(EventStoreExporterConfiguration.class);

        applyEnvironmentVariables(configuration);

        batchSize = configuration.batchSize;
        batchTimerMilli = configuration.batchTimeMilli;
        url = configuration.url + "/streams/" + configuration.streamName;

        retryTimeMillis = configuration.retryTimeMilli;
        backoffFactor = 1;
        log.debug("Exporter configured with {}", configuration);

        // Test the connection to Event Store, and halt the broker if unavailable
        try {
            sendBatchToEventStore(new JSONArray());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void open(Controller controller) {
        this.controller = controller;
        if (batchTimerMilli > 0) {
            scheduleNextBatch();
        }
        log.debug("Event Store exporter started.");
    }

    private void scheduleNextBatch() {
        int period = batchTimerMilli * backoffFactor;
        controller.scheduleTask(Duration.ofMillis(period), this::batchRecordsFromQueue);
    }

    private void scheduleRetry() {
        int period = retryTimeMillis * backoffFactor;
        controller.scheduleTask(Duration.ofMillis(period), this::retryFailedBatch);
    }

    private void sendBatchToEventStore(JSONArray jsonArray) throws IOException {
        try (final CloseableHttpClient client = HttpClients.createDefault()) {
            final HttpPost httpPost = new HttpPost(url);
            final StringEntity json = new StringEntity(jsonArray.toString());
            httpPost.setEntity(json);
            httpPost.setHeader("Content-Type", MIME_TYPE);
            client.execute(httpPost);
        }
    }

    private void retryFailedBatch() {
        if (retryQueue.isEmpty()) {
            return;
        }
        JSONArray retryBatch = retryQueue.getFirst();
        try {
            sendBatchToEventStore(retryBatch);
            retryQueue.poll();
            backoffFactor = 1;
        } catch (IOException e) {
            log.debug("Post to Event Store failed: " + e.getMessage());
            backoffFactor++;
            log.debug("Retrying in " + backoffFactor * retryTimeMillis + "ms");
            scheduleRetry();
        } finally {
            if (!retryQueue.isEmpty()) {
                scheduleRetry();
            }
        }
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
            backoffFactor = 1;
        } catch (IOException e) {
            log.debug(e.getMessage());
            retryQueue.add(jsonArray);
            scheduleRetry();
        } finally {
            scheduleNextBatch();
        }
    }

    public void close() {
        log.debug("Closing Event Store Exporter");
    }

    /**
     *     Events passed to the exporter are at-least-once -- the same event may be seen twice.
     *     Event Store guarantees idempotent event creation based on the eventId - which must be a UUID.
     *     We use a seed UUID, and replace the last part with the position and partition to get
     *     a UUID that is idempotent for an event on the broker.
     */
    private String getIdempotentEventId(Record record) {
        String seed = "393d7039721342d6b619de6bff4ffd2e";
        String id = String.valueOf(record.getPosition()) + record.getMetadata().getPartitionId();
        StringBuilder sb = new StringBuilder(seed);
        sb.delete(31 - id.length(), 31);
        sb.append(id);
        sb.insert(8, "-");
        sb.insert(13, "-");
        sb.insert(18, "-");
        sb.insert(23, "-");
        return sb.toString();
    }

    public void export(Record record) {
        final JSONObject json = new JSONObject();
        json.put("eventId", getIdempotentEventId(record));
        json.put("data", new JSONObject(record.toJson()));
        json.put("eventType", "ZeebeEvent");
        queue.add(new ImmutablePair<>(record.getPosition(), json));
    }

    private void applyEnvironmentVariables(final EventStoreExporterConfiguration configuration) {
        final Map<String, String> environment = System.getenv();
        log.debug(environment.toString());

        Optional.ofNullable(environment.get(ENV_STREAM_NAME))
                .ifPresent(streamName -> configuration.streamName = streamName);
        Optional.ofNullable(environment.get(ENV_URL))
                .ifPresent(url -> configuration.url = url);
        Optional.ofNullable(environment.get(ENV_BATCH_SIZE))
                .ifPresent(batchSize -> configuration.batchSize = Integer.parseInt(batchSize));
        Optional.ofNullable(environment.get(ENV_BATCH_TIME_MILLI))
                .ifPresent(batchTimeMilli -> configuration.batchTimeMilli = Integer.parseInt(batchTimeMilli));
        Optional.ofNullable(environment.get(ENV_RETRY_TIME_MILLI))
                .ifPresent(retryTimeMilli -> configuration.retryTimeMilli = Integer.parseInt(retryTimeMilli));
    }
}
