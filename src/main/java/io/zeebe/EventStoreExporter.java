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
    private static final String MIME_TYPE = "application/vnd.eventstore.events+json";
    private static final int BACK_PRESSURE_WARNING_SECONDS = 30;

    private Logger log;
    private Controller controller;

    private int batchSize;
    private String url;
    private int batchTimerMilli;
    private int currentBackoffFactor;

    private int backPressureWarningPeriodIterations;
    private int batchingBackPressureIterationCounter;
    private int sendingBackPressureIterationCounter;

    private LinkedList<ImmutablePair<Long, JSONObject>> eventsToBatch = new LinkedList<>();
    private LinkedList<ImmutablePair<Long, JSONArray>> batchesToSend = new LinkedList<>();

    public void configure(final Context context) {
        log = context.getLogger();
        EventStoreExporterConfiguration configuration = context
                .getConfiguration()
                .instantiate(EventStoreExporterConfiguration.class);

        applyEnvironmentVariables(configuration);

        batchSize = configuration.batchSize;
        batchTimerMilli = configuration.batchTimeMilli;
        url = configuration.url + "/streams/" + configuration.streamName;

        currentBackoffFactor = 1;
        log.debug("Exporter configured with {}", configuration);

        // Test the connection to Event Store, and halt the broker if unavailable
        try {
            postBatchToEventStore(new JSONArray());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void open(Controller controller) {
        this.controller = controller;
        if (batchTimerMilli > 0) {
            backPressureWarningPeriodIterations = BACK_PRESSURE_WARNING_SECONDS * 1000 / batchTimerMilli;
            batchingBackPressureIterationCounter = 0;
            sendingBackPressureIterationCounter = 0;
            scheduleNextBatchEvents();
            scheduleNextSendBatch();
        }
        log.debug("Event Store exporter started.");
    }

    public void close() {
        log.debug("Closing Event Store Exporter");
    }

    /**
     * At the moment all events are exported. To filter only certain event types, you could examine
     * record.getMetadata().getValueType() and exit immediately for events that are not of interest.
     */
    public void export(Record record) {
        final JSONObject json = new JSONObject();
        json.put("eventId", createIdempotentEventId(record));
        json.put("data", new JSONObject(record.toJson()));
        json.put("eventType", "ZeebeEvent");
        eventsToBatch.add(new ImmutablePair<>(record.getPosition(), json));
    }

    /**
     * This loop runs constantly at the rate of batchTimerMilli, and batches all the available events
     * up to the maximum batch size.
     */
    private void scheduleNextBatchEvents() {
        controller.scheduleTask(Duration.ofMillis(batchTimerMilli), this::batchEvents);
    }

    private void scheduleNextSendBatch() {
        int period = batchTimerMilli * currentBackoffFactor;
        controller.scheduleTask(Duration.ofMillis(period), this::sendBatch);
    }

    private void batchEvents() {
        if (eventsToBatch.isEmpty()) {
            scheduleNextBatchEvents();
            return;
        }
        final int eventQueueSize = eventsToBatch.size();
        if (eventQueueSize > 200) {
            batchingBackPressureIterationCounter++;
        } else {
            batchingBackPressureIterationCounter = 0;
        }
        final boolean batchingBackPressure = batchingBackPressureIterationCounter > backPressureWarningPeriodIterations;
        if (batchingBackPressure) {
            log.debug("The Event Store exporter has been experiencing back pressure for " + BACK_PRESSURE_WARNING_SECONDS + " seconds");
            log.debug("The event queue is currently: " + eventQueueSize);
            log.debug("If this continues to grow, decrease batchTimerMilli or increase batchSize.");
            batchingBackPressureIterationCounter = 0;
        }

        Long lastRecordInBatchPosition = -1L;
        final int countRecordsToSend = Math.max(batchSize, eventsToBatch.size());

        final JSONArray jsonArray = new JSONArray();

        for (int i = 0; i < countRecordsToSend; i ++) {
            final ImmutablePair<Long, JSONObject> current = eventsToBatch.pollFirst();
            if (current != null) {
                jsonArray.put(current.getValue());
                lastRecordInBatchPosition = current.getKey();
            }
        }
        batchesToSend.add(new ImmutablePair<>(lastRecordInBatchPosition, jsonArray));
        scheduleNextBatchEvents();
    }

    private void sendBatch() {
        if (batchesToSend.isEmpty()) {
            scheduleNextSendBatch();
            return;
        }
        final int batchQueueSize = batchesToSend.size();

        if (batchQueueSize > 5) {
            sendingBackPressureIterationCounter++;
        } else {
            sendingBackPressureIterationCounter = 0;
        }
        final boolean sendingBackPressure = sendingBackPressureIterationCounter > backPressureWarningPeriodIterations;
        if (sendingBackPressure && currentBackoffFactor == 1) {
            log.debug("The Event Store exporter has been experiencing back pressure for " + BACK_PRESSURE_WARNING_SECONDS + " seconds");
            log.debug("The current sending queue has " + batchQueueSize + " batches to send");
            log.debug("If this continues to grow, increase the batchSize or decrease the batchTimerMilli value");
            sendingBackPressureIterationCounter = 0;
        }

        ImmutablePair<Long, JSONArray> retryBatch = batchesToSend.getFirst();
        try {
            postBatchToEventStore(retryBatch.getValue());
            batchesToSend.poll();
            controller.updateLastExportedRecordPosition(retryBatch.getKey());
            currentBackoffFactor = 1;
        } catch (IOException e) {
            currentBackoffFactor++;
            log.debug("Post to Event Store failed. Retrying in " + currentBackoffFactor * batchTimerMilli + "ms");
        } finally {
            scheduleNextSendBatch();
        }
    }

    private void postBatchToEventStore(JSONArray jsonArray) throws IOException {
        try (final CloseableHttpClient client = HttpClients.createDefault()) {
            final HttpPost httpPost = new HttpPost(url);
            final StringEntity json = new StringEntity(jsonArray.toString());
            httpPost.setEntity(json);
            httpPost.setHeader("Content-Type", MIME_TYPE);
            client.execute(httpPost);
        }
    }

    /**
     *     Events passed to the exporter are at-least-once -- the same event may be seen twice.
     *     Event Store guarantees idempotent event creation based on the eventId - which must be a UUID.
     *     We use a seed UUID, and replace the last part with the position and partition to get
     *     a UUID that is idempotent for an event on the broker.
     */
    private String createIdempotentEventId(Record record) {
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

    private void applyEnvironmentVariables(final EventStoreExporterConfiguration configuration) {
        final Map<String, String> environment = System.getenv();

        Optional.ofNullable(environment.get(ENV_STREAM_NAME))
                .ifPresent(streamName -> configuration.streamName = streamName);
        Optional.ofNullable(environment.get(ENV_URL))
                .ifPresent(url -> configuration.url = url);
        Optional.ofNullable(environment.get(ENV_BATCH_SIZE))
                .ifPresent(batchSize -> configuration.batchSize = Integer.parseInt(batchSize));
        Optional.ofNullable(environment.get(ENV_BATCH_TIME_MILLI))
                .ifPresent(batchTimeMilli -> configuration.batchTimeMilli = Integer.parseInt(batchTimeMilli));
    }
}
