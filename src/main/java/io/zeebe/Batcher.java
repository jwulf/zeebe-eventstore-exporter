package io.zeebe;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.json.JSONArray;
import org.json.JSONObject;
import java.util.LinkedList;

class Batcher {
    final LinkedList<ImmutablePair<Long, JSONArray>> queue = new LinkedList<>();
    private final int batchSize;
    final int batchPeriod;

    Batcher(EventStoreExporterConfiguration configuration) {
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
