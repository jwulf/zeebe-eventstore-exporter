package io.zeebe;

import io.zeebe.exporter.api.record.Record;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.json.JSONObject;
import org.slf4j.Logger;
import java.util.LinkedList;

class EventQueue {
    private LinkedList<ImmutablePair<Long, JSONObject>> queue = new LinkedList<>();
    private Logger log;
    private int batchSize;
    private int batchingBackPressureIterationCounter;
    private int backPressureWarningPeriodIterations;
    private int backPressureWarningSeconds;

    EventQueue(EventStoreExporterContext context) {
        log = context.log;
        EventStoreExporterConfiguration configuration = context.configuration;
        batchSize = configuration.batchSize;
        final int batchTimeMilli = configuration.batchTimeMilli;
        batchingBackPressureIterationCounter = 0;
        backPressureWarningSeconds = configuration.backPressureWarningSeconds;
        backPressureWarningPeriodIterations = backPressureWarningSeconds * 1000 / batchTimeMilli;
    }

    LinkedList<ImmutablePair<Long, JSONObject>> getEvents() {
        return queue;
    }

    /**
     * At the moment all events are exported. To filter only certain event types, you could examine
     * record.getMetadata().getValueType() and exit immediately for events that are not of interest.
     */
    void addEvent(Record record) {
        final JSONObject json = new JSONObject();
        json.put("eventId", createIdempotentEventId(record));
        json.put("data", new JSONObject(record.toJson()));
        json.put("eventType", "ZeebeEvent");
        queue.add(new ImmutablePair<>(record.getPosition(), json));
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

    void warnIfBackPressure() {
        final int eventQueueSize = queue.size();
        if (eventQueueSize > batchSize * 2) {
            batchingBackPressureIterationCounter++;
        } else {
            batchingBackPressureIterationCounter = 0;
        }
        final boolean batchingBackPressure = batchingBackPressureIterationCounter > backPressureWarningPeriodIterations;
        if (batchingBackPressure) {
            log.debug("The Event Store exporter has been experiencing back pressure for " + backPressureWarningSeconds + " seconds");
            log.debug("The event queue is currently: " + eventQueueSize);
            log.debug("If this continues to grow, decrease batchTimerMilli or increase batchSize.");
            batchingBackPressureIterationCounter = 0;
        }
    }
}
