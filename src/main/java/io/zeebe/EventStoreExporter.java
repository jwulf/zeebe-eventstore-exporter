package io.zeebe;

import io.zeebe.exporter.api.context.Context;
import io.zeebe.exporter.api.context.Controller;
import io.zeebe.exporter.api.record.Record;
import io.zeebe.exporter.api.spi.Exporter;
import org.apache.http.client.methods.CloseableHttpResponse;
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

    private Logger log;
    private EventStoreExporterConfiguration configuration;
    private Controller controller;

    private int batchSize;
    private int batchTimerMilli;
    private Duration batchExecutionTimer;
    private String url;
    private String streamName;
    private LinkedList<ImmutablePair<Long, String>> batch = new LinkedList<>();

    public void configure(final Context context) {
        log = context.getLogger();
        configuration = context
                .getConfiguration()
                .instantiate(EventStoreExporterConfiguration.class);

        applyEnvironmentVariables(configuration);

        batchSize = configuration.batchSize;
        batchTimerMilli = configuration.batchTimeMilli;
        url = configuration.url;
        streamName = configuration.streamName;

        log.debug("Exporter configured with {}", configuration);
    }

    public void open(Controller controller) {
        this.controller = controller;
        if (batchTimerMilli > 0) {
            batchExecutionTimer = Duration.ofMillis(batchTimerMilli);
            this.controller.scheduleTask(batchExecutionTimer, this::batchExportExecution);
        }
    }

    private void sendRecord(JSONArray jsonString) throws IOException {
        try {
            final String url = this.url + "/streams/" + this.streamName;
            final CloseableHttpClient client = HttpClients.createDefault();
            HttpPost httpPost = new HttpPost(url);
            StringEntity json = new StringEntity(jsonString.toString());
            httpPost.setEntity(json);
            httpPost.setHeader("Content-Type", "application/vnd.eventstore.events+json");
            CloseableHttpResponse response = client.execute(httpPost);
            client.close();
        } catch (IOException e) {
            throw new IOException(e.getMessage());
        }
    }

    private void batchExportExecution() {
        // Return if there are no queued events to send
        if (batch.isEmpty()) {
            controller.scheduleTask(batchExecutionTimer, this::batchExportExecution);
            return;
        }
        // In this iteration, send all the queued events up to the batchSize
        int countRecordsToSend = Math.max(batchSize, batch.size());

        // Grab the queued events to send
        ArrayList<String> recordsToSend = new ArrayList<>();
        long lastRecordSentPosition = -1;
        for (int i = 0; i < countRecordsToSend; i++) {
            ImmutablePair<Long, String> current = batch.pollFirst();
            if (current != null) {
                recordsToSend.add(current.getValue());
                lastRecordSentPosition = current.getKey();
            }
        }

        JSONArray jsonArray = new JSONArray();
        for (String s : recordsToSend) {
            jsonArray.put(new JSONObject(s));
        }

        // Send the JSON Array to Event Store
        try {
            this.sendRecord(jsonArray);
        } catch (IOException e) {
            // YOLO - lossy
            log.debug(e.getMessage());
        }

        if (lastRecordSentPosition != -1) {
            controller.updateLastExportedRecordPosition(lastRecordSentPosition);
        }
        controller.scheduleTask(batchExecutionTimer, this::batchExportExecution);
    }

    public void close() {

    }

    public void export(Record record) {
        String jsonRecord = record.toJson();
        JSONObject jo = new JSONObject();
        jo.put("eventId", UUID.randomUUID());
        jo.put("data", new JSONObject(jsonRecord));
        jo.put("eventType", "ZeebeEvent");
        batch.add(new ImmutablePair<>(record.getPosition(), jo.toString()));
    }

    private void applyEnvironmentVariables(final EventStoreExporterConfiguration configuration) {
        final Map<String, String> environment = System.getenv();

        Optional.ofNullable(environment.get(ENV_STREAM_NAME))
                .ifPresent(streamName -> configuration.streamName = streamName);
        Optional.ofNullable(environment.get(ENV_URL))
                .ifPresent(url -> configuration.url = url);
    }
}
