package io.zeebe;

import io.zeebe.exporter.api.context.Controller;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.util.LinkedList;
import org.slf4j.Logger;

class Batcher {
    private final int[] FIBONACCI = new int[]{ 1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144};
    private final LinkedList<ImmutablePair<Long, JSONArray>> queue = new LinkedList<>();
    private final int batchSize;
    private final HttpSender sender;
    private final Logger log;
    private final Controller controller;
    private final int sendTimeMilli;
    int sendPeriod;
    final int batchPeriod;

    Batcher(EventStoreExporterContext context) {
        EventStoreExporterConfiguration configuration = context.configuration;
        batchSize = configuration.batchSize;
        log = context.log;
        controller = context.controller;
        sender = new HttpSender(configuration);
        sendTimeMilli = configuration.batchTimeMilli;
        sendPeriod = sendTimeMilli;
        batchPeriod = configuration.batchTimeMilli;

    }

    void batchFrom(LinkedList<ImmutablePair<Long, JSONObject>> eventQueue) {
        if (eventQueue.isEmpty()) {
            return;
        }
        Long positionOfLastRecordInBatch = -1L;
        final int countEventsToBatchNow = Math.max(batchSize, eventQueue.size());
        final JSONArray jsonArrayOfEvents = new JSONArray();

        for (int i = 0; i < countEventsToBatchNow; i ++) {
            final ImmutablePair<Long, JSONObject> event = eventQueue.pollFirst();
            if (event != null) {
                jsonArrayOfEvents.put(event.getValue());
                positionOfLastRecordInBatch = event.getKey();
            }
        }
        queue.add(new ImmutablePair<>(positionOfLastRecordInBatch, jsonArrayOfEvents));
    }

    void sendBatch() {
        if (queue.isEmpty()) {
            return;
        }
        ImmutablePair<Long, JSONArray> batch = queue.getFirst();
        final JSONArray eventBatch = batch.getValue();
        final Long positionOfLastEventInBatch = batch.getKey();
        try {
            sender.send(eventBatch);
            queue.pollFirst();
            controller.updateLastExportedRecordPosition(positionOfLastEventInBatch);
        } catch (IOException e) {
            log.debug("Post to Event Store failed.");
            log.debug(e.toString());
        } finally {
            sendPeriod = FIBONACCI[Math.min(sender.backOffFactor, FIBONACCI.length -  1)] * sendTimeMilli;
            if (sender.failed) {
                log.debug("Retrying in "+ sendPeriod + "ms...");
            }
        }
    }
}
