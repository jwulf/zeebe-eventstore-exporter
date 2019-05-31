package io.zeebe;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.json.JSONArray;
import org.json.JSONObject;
import java.util.LinkedList;

class Batcher {
    private final int batchSize;
    final LinkedList<ImmutablePair<Long, JSONArray>> queue = new LinkedList<>();
    int batchPeriod;

    Batcher(EventStoreExporterContext context) {
        EventStoreExporterConfiguration configuration = context.configuration;
        batchSize = configuration.batchSize;
        batchPeriod = configuration.batchTimeMilli;
    }

    void batchFrom(EventQueue eventQueue) {
        if (eventQueue.queue.isEmpty()) {
            return;
        }
        Long positionOfLastRecordInBatch = -1L;
        final int countEventsToBatchNow = Math.max(batchSize, eventQueue.queue.size());
        final JSONArray jsonArrayOfEvents = new JSONArray();

        for (int i = 0; i < countEventsToBatchNow; i ++) {
            final ImmutablePair<Long, JSONObject> event = eventQueue.queue.pollFirst();
            if (event != null) {
                jsonArrayOfEvents.put(event.getValue());
                positionOfLastRecordInBatch = event.getKey();
            }
        }
        queue.add(new ImmutablePair<>(positionOfLastRecordInBatch, jsonArrayOfEvents));
    }
}
