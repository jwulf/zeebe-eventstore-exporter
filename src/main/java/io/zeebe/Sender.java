package io.zeebe;

import io.zeebe.exporter.api.context.Controller;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.json.JSONArray;
import org.slf4j.Logger;

import java.io.IOException;

class Sender {
    private final int[] FIBONACCI = new int[]{ 1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144};

    private Controller controller;
    private Logger log;
    private HttpStatelessSender http;
    private int sendTimeMilli;
    private int backOffFactor = 1;
    int sendPeriod;

    Sender(Controller controller, Logger log, EventStoreExporterConfiguration configuration) {
        sendTimeMilli = configuration.batchTimeMilli;
        this.controller = controller;
        this.log = log;
        http = new HttpStatelessSender(configuration);
    }

    void sendFrom(Batcher batcher) {
        if (batcher.queue.isEmpty()) {
            return;
        }
        final ImmutablePair<Long, JSONArray> batch = batcher.queue.getFirst();
        final JSONArray eventBatch = batch.getValue();
        final Long positionOfLastEventInBatch = batch.getKey();
        try {
            http.send(eventBatch);
            batcher.queue.pollFirst();
            controller.updateLastExportedRecordPosition(positionOfLastEventInBatch);
            backOffFactor = 1;
            sendPeriod = sendTimeMilli;
        } catch (IOException e) {
            backOff();
        }
    }

    private void backOff() {
        backOffFactor ++;
        sendPeriod = FIBONACCI[Math.min(backOffFactor, FIBONACCI.length -  1)] * sendTimeMilli;
        log.debug("Post to Event Store failed.");
        log.debug("Retrying in " + sendPeriod + "ms...");
    }
}
