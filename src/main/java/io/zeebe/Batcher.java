package io.zeebe;

import io.zeebe.exporter.api.context.Controller;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.util.LinkedList;
import org.slf4j.Logger;

class Batcher {
    private LinkedList<ImmutablePair<Long, JSONArray>> queue = new LinkedList<>();
    private int batchSize;
    private HttpSender sender;
    private Logger log;
    private int sendingBackPressureIterationCounter = 0;
    private int backPressureWarningPeriodIterations;
    private int backPressureWarningSeconds;
    private Controller controller;
    private int sendTimeMilli;
    int sendPeriod;
    int batchPeriod;


    Batcher(EventStoreExporterContext context) {
        EventStoreExporterConfiguration configuration = context.configuration;
        batchSize = configuration.batchSize;
        log = context.log;
        controller = context.controller;
        sender = new HttpSender(configuration);
        sendTimeMilli = configuration.batchTimeMilli;
        sendPeriod = sendTimeMilli;
        batchPeriod = configuration.batchTimeMilli;
        backPressureWarningSeconds = configuration.backPressureWarningSeconds;
        backPressureWarningPeriodIterations = backPressureWarningSeconds * 1000 / sendTimeMilli;
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
        ImmutablePair<Long, JSONArray> batch = queue.getFirst();
        final JSONArray eventBatch = batch.getValue();
        final Long positionOfLastEventInBatch = batch.getKey();
        try {
            sender.send(eventBatch);
            queue.pollFirst();
            controller.updateLastExportedRecordPosition(positionOfLastEventInBatch);
        } catch (IOException e) {
            sendPeriod = sender.backOffFactor * sendTimeMilli;
            log.debug("Post to Event Store failed. Retrying in " + sendPeriod + "ms");
        } finally {
            sendPeriod = sender.backOffFactor * sendTimeMilli;
        }
    }

    void warnIfBackPressure() {
        int batchQueueSize = queue.size();
        if (batchQueueSize > 3) {
            sendingBackPressureIterationCounter++;
        } else {
            sendingBackPressureIterationCounter = 0;
        }
        final boolean sendingBackPressure = sendingBackPressureIterationCounter > backPressureWarningPeriodIterations;
        if (sendingBackPressure && sender.backOffFactor == 1) {
            log.debug("The Event Store exporter has been experiencing back pressure for " + backPressureWarningSeconds + " seconds");
            log.debug("The current sending queue has " + batchQueueSize + " batches to send");
            log.debug("If this continues to grow, increase the batchSize or decrease the sendTimeMilli value");
            sendingBackPressureIterationCounter = 0;
        }
    }
}
